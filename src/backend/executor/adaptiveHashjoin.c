#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "executor/executor.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/sharedtuplestore.h"

#include "executor/adaptiveHashjoin.h"


/*
 * Only the elected worker will be calling this
 */
void
combine_outer_match_statuses(BufFile *outer_match_statuses[], int length, size_t num_bytes, int batchno, BufFile **combined_bitmap_file)
{
	BufFile *first_file = outer_match_statuses[0];
	// make an output file for now
	BufFile *combined_file = *combined_bitmap_file;

	unsigned char current_byte_to_write = 0;
	unsigned char current_byte_to_read = 0;
	for(int64 cur = 0; cur < num_bytes; cur++) // make it while not EOF
	{
		BufFileRead(first_file, &current_byte_to_write, 1);
		BufFile *file1 = NULL;
		for (int k = 1; k < length; k++)
		{
			file1 = outer_match_statuses[k];
			elog(DEBUG3, "in combine_outer_match_statuses. batchno %i. pid %i.", batchno, MyProcPid);
			BufFileRead(file1, &current_byte_to_read, 1);
			current_byte_to_write = current_byte_to_write | current_byte_to_read;
		}
		BufFileWrite(combined_file, &current_byte_to_write, 1);
	}
	if (BufFileSeek(combined_file, 0, 0L, SEEK_SET))
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("could not rewind hash-join temporary file: %m")));

	for (int j = 0; j < length; j++)
		BufFileClose(outer_match_statuses[j]);
}

bool
ExecParallelHashJoinNewChunk(HashJoinState *hjstate, bool advance_from_probing)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int batchno = hashtable->curbatch;
	ParallelHashJoinBatch *phj_batch = hashtable->batches[batchno].shared;

	SharedTuplestoreAccessor *outer_tuples = hashtable->batches[batchno].outer_tuples;
	SharedTuplestoreAccessor *inner_tuples = hashtable->batches[batchno].inner_tuples;

	// This chunk_barrier is initialized in the ELECTING phase when the we
	// attached to the batch in ExecParallelHashJoinNewBatch()
	Barrier *chunk_barrier =
			&hashtable->batches[batchno].shared->fallback_chunk_barrier;

	if (batchno > 0 && advance_from_probing)
	{
		// the current chunk number can't be incremented if *any* worker isn't
		// done yet (otherwise they might access the wrong data structure!)
		// We must be in the PROBING phase
		// need to advance the batch

		if (BarrierArriveAndWait(chunk_barrier,
								 WAIT_EVENT_HASH_CHUNK_FINISHING_PROBING))
			phj_batch->current_chunk_num++;

		// Once the barrier is advanced we'll be in the DONE phase
		// unfortunately, we are still attached to chunk barrier so we don't want to reattach
		// and increase nparticipants or we will have deadlock
	}
	else
		BarrierAttach(chunk_barrier);

	// should only come here for batches after batch 0
	// either I need a new batch (was on the last chunk or coming from batch 0)
	// or I need the next chunk (exhausted outer)
	// should we do this if we already had a barrier arrive and wait above?
	switch (BarrierPhase(chunk_barrier))
	{
		case PHJ_CHUNK_ELECTING:
		phj_chunk_electing:
			/* One backend allocates the hash table or resets it. */
			elog(DEBUG3, "PHJ_CHUNK_ELECTING batch %i. pid %i.", batchno, MyProcPid);
			BarrierArriveAndWait(chunk_barrier,
									 WAIT_EVENT_HASH_CHUNK_ELECTING);
			/* Fall through. */

		case PHJ_CHUNK_ALLOCATING: // delete me
			/* Wait for allocation to complete. */
			elog(DEBUG3, "PHJ_CHUNK_ALLOCATING batch %i. pid %i", batchno, MyProcPid);
			BarrierArriveAndWait(chunk_barrier,
								 WAIT_EVENT_HASH_CHUNK_ALLOCATING);
			/* Fall through. */

		case PHJ_CHUNK_LOADING:
			/* Start (or join in) loading tuples. */
			elog(DEBUG3, "PHJ_CHUNK_LOADING batch %i. pid %i.", batchno, MyProcPid);
			// load the next chunk of inner tuples into the hashtable

			sts_begin_parallel_scan(inner_tuples);
			MinimalTuple tuple;
			TupleTableSlot *slot;

			tupleMetadata metadata;
			uint32		hashvalue;

			while ((tuple = sts_parallel_scan_next(inner_tuples,
												   &metadata, false)))
			{
				hashvalue = metadata.hashvalue;
				int chunk_num = metadata.tuplenum;
				ExecForceStoreMinimalTuple(tuple,
										   hjstate->hj_HashTupleSlot,
										   false);
				slot = hjstate->hj_HashTupleSlot;
				if (chunk_num == phj_batch->current_chunk_num)
					ExecParallelHashTableInsertCurrentBatch(hashtable, slot,
															hashvalue, chunk_num);
			}
			sts_end_parallel_scan(inner_tuples);
			BarrierArriveAndWait(chunk_barrier,
								 WAIT_EVENT_HASH_CHUNK_LOADING);
			/* Fall through. */

			// need to advance the phase at the top because we will return to here
			// when we come back for a new chunk and be in the probing phase
			// this is characterized by batch > 0 (and chunk > 0)

		case PHJ_CHUNK_PROBING:
			elog(DEBUG3, "PHJ_CHUNK_PROBING batch %i. pid %i.", batchno, MyProcPid);
			sts_begin_parallel_scan(outer_tuples);
			return true;

		case PHJ_CHUNK_DONE:
			elog(DEBUG3, "PHJ_CHUNK_DONE batch %i. pid %i.", batchno, MyProcPid);

			BarrierArriveAndWait(chunk_barrier, WAIT_EVENT_HASH_CHUNK_MORE_DONE);

			if (phj_batch->current_chunk_num > phj_batch->total_num_chunks)
			{
				Assert(BarrierPhase(chunk_barrier) == PHJ_CHUNK_FINAL);
				BarrierDetach(chunk_barrier);
				return false;
			}

			// otherwise it is time for the next chunk
			if (BarrierArriveExplicitAndWait(chunk_barrier, PHJ_CHUNK_ELECTING, WAIT_EVENT_CHUNK_FINAL))
			{
				// rewind/reset outer tuplestore and rewind outer match status files
				sts_reinitialize(outer_tuples);
				// TODO: make a version of sts_begin_parallel_scan() that doesn't null out the bitmap file
				// maybe I need to do sts_end_write() and or something manual to make it so I can rewind the outer tuples tuplestore and also keep the bitmap
				// or maybe just do sts_end_parallel_scan(outer_tuples);

				// reset inner's hashtable
				/* Recycle the existing bucket array. */
				dsa_pointer_atomic *buckets = (dsa_pointer_atomic *)
						dsa_get_address(hashtable->area, phj_batch->buckets);
				for (size_t i = 0; i < hashtable->nbuckets; ++i)
					dsa_pointer_atomic_write(&buckets[i], InvalidDsaPointer);

				// TODO: this will unfortunately rescan all inner tuples in the batch for each chunk
				sts_reinitialize(inner_tuples);
			}
			goto phj_chunk_electing;

		case PHJ_CHUNK_FINAL:
			Assert(BarrierPhase(chunk_barrier) == PHJ_CHUNK_FINAL);
			BarrierDetach(chunk_barrier);
			return false;

		default:
			elog(ERROR, "unexpected chunk phase %d. pid %i. batch %i.",
				 BarrierPhase(chunk_barrier), MyProcPid, batchno);
	}

	return false;
}


/*
 * Choose a batch to work on, and attach to it.  Returns true if successful,
 * false if there are no more batches.
 */
bool
ExecParallelHashJoinNewBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			start_batchno;
	int			batchno;

	/*
	 * If we started up so late that the batch tracking array has been freed
	 * already by ExecHashTableDetach(), then we are finished.  See also
	 * ExecParallelHashEnsureBatchAccessors().
	 */
	if (hashtable->batches == NULL)
		return false;

	/*
	 * For hashloop fallback only
	 * Only the elected worker who was chosen to combine the outer match status bitmaps
	 * should reach here. This worker must do some final cleanup and then detach from the batch
	 */
	if (hjstate->combined_bitmap != NULL)
	{
		BufFileClose(hjstate->combined_bitmap);
		hjstate->combined_bitmap = NULL;
		hashtable->batches[hashtable->curbatch].done = true;
		ExecHashTableDetachBatch(hashtable);
	}

	/*
	 * If we were already attached to a batch, remember not to bother checking
	 * it again, and detach from it (possibly freeing the hash table if we are
	 * last to detach).
	 * curbatch is set when the batch_barrier phase is either PHJ_BATCH_LOADING
	 * or PHJ_BATCH_CHUNKING (note that the PHJ_BATCH_LOADING case will fall through
	 * to the PHJ_BATCH_CHUNKING case). The PHJ_BATCH_CHUNKING case returns to the
	 * caller. So when this function is reentered with a curbatch >= 0 then we must
	 * be done probing.
	 */
	if (hashtable->curbatch >= 0)
	{

		int curbatch = hashtable->curbatch;
		ParallelHashJoinBatchAccessor *accessor = hashtable->batches + curbatch;
		ParallelHashJoinBatch *batch = accessor->shared;

		/*
		 * End the parallel scan on the outer tuples before we arrive at the next barrier
		 * so that the last worker to arrive at that barrier can reinitialize the SharedTuplestore
		 * for another parallel scan.
		 */

		// TODO:  do I still want to end the parallel scan even if parallel_hashloop_fallback is false?
		sts_end_parallel_scan(accessor->outer_tuples);

		if (batch->parallel_hashloop_fallback == true)
		{
			// everybody does this
			// might need to check if this is present
			BufFileClose(sts_get_my_STA_outerMatchStatuses(accessor->outer_tuples));

			/*
			 * If all workers (including this one) have finished probing the batch, one worker is elected to
			 * Combine all the outer match status files from the workers who were attached to this batch
			 * Loop through the outer match status files from all workers that were attached to this batch
			 * Combine them into one bitmap
			 * Use the bitmap, loop through the outer batch file again, and emit unmatched tuples
			 */

			// The batch files and outer match status files can be closed then

			if (BarrierArriveAndWait(&batch->batch_barrier,
									 WAIT_EVENT_HASH_BATCH_PROBING))
			{
				// on the elected combining worker should do this
				SharedTuplestoreAccessor *outer_acc = accessor->outer_tuples;

				//TODO: right? This doesn't work if I let the other workers move on -- the number of participants won't be right
				int length = BarrierParticipants(&batch->batch_barrier);
				//int length = sts_participants(outer_acc);
				BufFile **outer_match_statuses = palloc(sizeof(BufFile *) * length);
				char **outer_match_status_filenames = alloca(sizeof(char *) * length);

				populate_outer_match_statuses(outer_acc, outer_match_statuses, outer_match_status_filenames);

				BufFile *combined_bitmap_file = BufFileCreateTemp(false);
				// This also closes the files opened in populate_outer_match_statuses
				combine_outer_match_statuses(
						outer_match_statuses,
						length,
						BufFileBytesUsed(outer_match_statuses[0]),
						curbatch,
						&combined_bitmap_file);

				hjstate->combined_bitmap = combined_bitmap_file;
				hjstate->last_worker = true;
				hjstate->num_outer_read_files = length;
				pfree(outer_match_statuses);
				return true;
			}

			/*
			 * the elected combining worker should not reach here
			 */
			hashtable->batches[curbatch].done = true;
			ExecHashTableDetachBatch(hashtable);
		}
		else
		{
			// non fallback case
			BarrierArriveAndWait(&batch->batch_barrier,
								 WAIT_EVENT_HASH_BATCH_PROBING);
			hashtable->batches[curbatch].done = true;
			ExecHashTableDetachBatch(hashtable);
		}
	}

	/*
	 * Search for a batch that isn't done.  We use an atomic counter to start
	 * our search at a different batch in every participant when there are
	 * more batches than participants.
	 */
	batchno = start_batchno =
			pg_atomic_fetch_add_u32(&hashtable->parallel_state->distributor, 1) %
			hashtable->nbatch;

	do
	{
		if (!hashtable->batches[batchno].done)
		{
			Barrier    *batch_barrier =
					&hashtable->batches[batchno].shared->batch_barrier;

			switch (BarrierAttach(batch_barrier))
			{
				case PHJ_BATCH_ELECTING:
					/* One backend allocates the hash table. */
					elog(DEBUG3, "PHJ_BATCH_ELECTING batch %i. pid %i.",batchno, MyProcPid);
					hjstate->hj_InnerExhausted = false;
					if (BarrierArriveAndWait(batch_barrier,
											 WAIT_EVENT_HASH_BATCH_ELECTING))
					{
						ExecParallelHashTableAlloc(hashtable, batchno);
						Barrier *chunk_barrier =
								&hashtable->batches[batchno].shared->fallback_chunk_barrier;
						BarrierInit(chunk_barrier, 0);
						hashtable->batches[batchno].shared->current_chunk_num = 1;
					}
					/* Fall through. */

				case PHJ_BATCH_ALLOCATING:
					/* Wait for allocation to complete. */
					elog(DEBUG3, "PHJ_BATCH_ALLOCATING batch %i. pid %i", batchno, MyProcPid);
					hjstate->hj_InnerExhausted = false;
					BarrierArriveAndWait(batch_barrier,
										 WAIT_EVENT_HASH_BATCH_ALLOCATING);
					/* Fall through. */

				case PHJ_BATCH_LOADING: // delete me
					/* Start (or join in) loading tuples. */
					elog(DEBUG3, "PHJ_BATCH_LOADING batch %i. pid %i.", batchno, MyProcPid);
					ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
					hjstate->hj_InnerExhausted = false;
					BarrierArriveAndWait(batch_barrier,
										 WAIT_EVENT_HASH_BATCH_LOADING);
					/* Fall through. */

				case PHJ_BATCH_CHUNKING:
					/*
					 * This batch is ready to probe.  Return control to
					 * caller. We stay attached to batch_barrier so that the
					 * hash table stays alive until everyone's finished
					 * probing it, but no participant is allowed to wait at
					 * this barrier again (or else a deadlock could occur).
					 * All attached participants must eventually call
					 * BarrierArriveAndDetach() so that the final phase
					 * PHJ_BATCH_DONE can be reached.
					 */
					elog(DEBUG3, "PHJ_BATCH_CHUNKING batch %i. pid %i.", batchno, MyProcPid);
					ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
					// TODO: what if a worker joins here and hasn't set hjstate->hj_InnerExhausted = false;
					// it would be true from the previous batch

					// Create an outer match status file for this batch for this worker
					// This file must be accessible to the other workers
					// But *only* written to by this worker. Written to by this worker and readable by any worker
					if (hashtable->batches[batchno].shared->parallel_hashloop_fallback)
					{
						char outer_match_status_filename[MAXPGPATH];
						sts_make_STA_outerMatchStatuses(hashtable->batches[batchno].outer_tuples, batchno, outer_match_status_filename);
					}
					if (batchno == 0)
					{
						hashtable->batches[batchno].shared->current_chunk_num = 1;
						// is this right place to do this
						sts_begin_parallel_scan(hashtable->batches[batchno].outer_tuples);
					}
					return true;

				case PHJ_BATCH_OUTER_MATCH_STATUS_PROCESSING:
					elog(DEBUG3, "in PHJ_BATCH_OUTER_MATCH_STATUS_PROCESSING. done with batch %i. process %i.", batchno, MyProcPid);
					BarrierDetach(batch_barrier);
					// The batch isn't done but this worker can't contribute anything to it
					// so it might as well be done from this worker's perspective
					hashtable->batches[batchno].done = true;
					hashtable->curbatch = -1;
					break;

				case PHJ_BATCH_DONE:
					/*
					 * Already done.  Detach and go around again (if any
					 * remain).
					 */
					elog(DEBUG3, "in PHJ_BATCH_DONE. done with batch %i. process %i.", batchno, MyProcPid);
					BarrierDetach(batch_barrier);
					hashtable->batches[batchno].done = true;
					hashtable->curbatch = -1;
					break;

				default:
					elog(ERROR, "unexpected batch phase %d. pid %i. batchno %i.",
						 BarrierPhase(batch_barrier), MyProcPid, batchno);
			}
		}
		batchno = (batchno + 1) % hashtable->nbatch;
	} while (batchno != start_batchno);

	return false;
}
