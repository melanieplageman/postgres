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




bool
ExecParallelHashJoinNewChunk(HashJoinState *hjstate, bool advance_from_probing)
{
	HashJoinTable hashtable;
	int batchno;
	ParallelHashJoinBatch *phj_batch;
	SharedTuplestoreAccessor *outer_tuples;
	SharedTuplestoreAccessor *inner_tuples;
	Barrier *chunk_barrier;

	hashtable = hjstate->hj_HashTable;
	batchno = hashtable->curbatch;
	phj_batch = hashtable->batches[batchno].shared;
	outer_tuples = hashtable->batches[batchno].outer_tuples;
	inner_tuples = hashtable->batches[batchno].inner_tuples;

	/*
	 * This chunk_barrier is initialized in the ELECTING phase when this worker
	 * attached to the batch in ExecParallelHashJoinNewBatch()
	 */
	chunk_barrier = &hashtable->batches[batchno].shared->chunk_barrier;

	/*
	 * If this worker just came from probing (from HJ_SCAN_BUCKET) we need to
	 * advance the chunk number here. Otherwise this worker isn't attached yet
	 * to the chunk barrier.
	 */
	if (advance_from_probing)
	{
		/*
		 * The current chunk number can't be incremented if *any* worker isn't
		 * done yet (otherwise they might access the wrong data structure!)
		 */
		if (BarrierArriveAndWait(chunk_barrier,
		                         WAIT_EVENT_HASH_CHUNK_PROBING))
			phj_batch->current_chunk_num++;

		/* Once the barrier is advanced we'll be in the DONE phase */
	}
	else
		BarrierAttach(chunk_barrier);

	/*
	 * The outer side is exhausted and either
	 * 1) the current chunk of the inner side is exhausted and it is time to advance the chunk
	 * 2) the last chunk of the inner side is exhausted and it is time to advance the batch
	 */
	switch (BarrierPhase(chunk_barrier))
	{
		// TODO: remove this phase and coordinate access to hashtable above goto and after incrementing current_chunk_num
		case PHJ_CHUNK_ELECTING:
		phj_chunk_electing:
			BarrierArriveAndWait(chunk_barrier,
									 WAIT_EVENT_HASH_CHUNK_ELECTING);
			/* Fall through. */

		case PHJ_CHUNK_LOADING:
			/* Start (or join in) loading the next chunk of inner tuples. */
			sts_begin_parallel_scan(inner_tuples);

			MinimalTuple  tuple;
			tupleMetadata metadata;

			while ((tuple = sts_parallel_scan_next(inner_tuples, &metadata)))
			{
				if (metadata.tupleid != phj_batch->current_chunk_num)
					continue;

				ExecForceStoreMinimalTuple(tuple,
										   hjstate->hj_HashTupleSlot,
										   false);

				ExecParallelHashTableInsertCurrentBatch(
					hashtable,
					hjstate->hj_HashTupleSlot,
					metadata.hashvalue);
			}
			sts_end_parallel_scan(inner_tuples);
			BarrierArriveAndWait(chunk_barrier,
								 WAIT_EVENT_HASH_CHUNK_LOADING);
			/* Fall through. */

		case PHJ_CHUNK_PROBING:
			sts_begin_parallel_scan(outer_tuples);
			return true;

		case PHJ_CHUNK_DONE:

			BarrierArriveAndWait(chunk_barrier, WAIT_EVENT_HASH_CHUNK_DONE);

			if (phj_batch->current_chunk_num > phj_batch->total_num_chunks)
			{
				BarrierDetach(chunk_barrier);
				return false;
			}

			/*
			 * Otherwise it is time for the next chunk.
			 * One worker should reset the hashtable
			 */
			if (BarrierArriveExplicitAndWait(chunk_barrier, PHJ_CHUNK_ELECTING, WAIT_EVENT_HASH_ADVANCE_CHUNK))
			{
				/* rewind/reset outer tuplestore and rewind outer match status files */
				sts_reinitialize(outer_tuples);

				/* reset inner's hashtable and recycle the existing bucket array. */
				dsa_pointer_atomic *buckets = (dsa_pointer_atomic *)
						dsa_get_address(hashtable->area, phj_batch->buckets);
				for (size_t i = 0; i < hashtable->nbuckets; ++i)
					dsa_pointer_atomic_write(&buckets[i], InvalidDsaPointer);

				// TODO: this will unfortunately rescan all inner tuples in the batch for each chunk
				// should be able to save the block in the file which starts the next chunk instead
				sts_reinitialize(inner_tuples);
			}
			goto phj_chunk_electing;

		case PHJ_CHUNK_FINAL:
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
		ParallelHashJoinBatchAccessor *accessor = hashtable->batches + hashtable->curbatch;
		ParallelHashJoinBatch *batch = accessor->shared;

		/*
		 * End the parallel scan on the outer tuples before we arrive at the next barrier
		 * so that the last worker to arrive at that barrier can reinitialize the SharedTuplestore
		 * for another parallel scan.
		 */

		if (!batch->parallel_hashloop_fallback)
			BarrierArriveAndWait(&batch->batch_barrier,
			                     WAIT_EVENT_HASH_BATCH_PROBING);
		else
		{
			sts_close_outer_match_status_file(accessor->outer_tuples);

			/*
			 * If all workers (including this one) have finished probing the batch, one worker is elected to
			 * Combine all the outer match status files from the workers who were attached to this batch
			 * Loop through the outer match status files from all workers that were attached to this batch
			 * Combine them into one bitmap
			 * Use the bitmap, loop through the outer batch file again, and emit unmatched tuples
			 */

			if (BarrierArriveAndWait(&batch->batch_barrier,
									 WAIT_EVENT_HASH_BATCH_PROBING))
			{
				hjstate->combined_bitmap = sts_combine_outer_match_status_files(accessor->outer_tuples);
				hjstate->last_worker = true;
				return true;
			}
		}

		/* the elected combining worker should not reach here */
		hashtable->batches[hashtable->curbatch].done = true;
		ExecHashTableDetachBatch(hashtable);
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
			Barrier *batch_barrier =
					&hashtable->batches[batchno].shared->batch_barrier;

			switch (BarrierAttach(batch_barrier))
			{
				case PHJ_BATCH_ELECTING:
					/* One backend allocates the hash table. */
					if (BarrierArriveAndWait(batch_barrier,
											 WAIT_EVENT_HASH_BATCH_ELECTING))
					{
						ExecParallelHashTableAlloc(hashtable, batchno);
						Barrier *chunk_barrier =
								&hashtable->batches[batchno].shared->chunk_barrier;
						BarrierInit(chunk_barrier, 0);
						hashtable->batches[batchno].shared->current_chunk_num = 1;
					}
					/* Fall through. */

				case PHJ_BATCH_ALLOCATING:
					/* Wait for allocation to complete. */
					BarrierArriveAndWait(batch_barrier,
										 WAIT_EVENT_HASH_BATCH_ALLOCATING);
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
					ExecParallelHashTableSetCurrentBatch(hashtable, batchno);

					if (batchno == 0)
						sts_begin_parallel_scan(hashtable->batches[batchno].outer_tuples);
					/*
					 * Create an outer match status file for this batch for this worker
					 * This file must be accessible to the other workers
					 * But *only* written to by this worker.
					 * Written to by this worker and readable by any worker
					 */
					if (hashtable->batches[batchno].shared->parallel_hashloop_fallback)
						sts_make_outer_match_status_file(hashtable->batches[batchno].outer_tuples);

					return true;

				case PHJ_BATCH_OUTER_MATCH_STATUS_PROCESSING:
					/*
					 * The batch isn't done but this worker can't contribute
					 * anything to it so it might as well be done from this
					 * worker's perspective. (Only one worker can do work in
					 * this phase).
					 */

					/* Fall through. */

				case PHJ_BATCH_DONE:
					/*
					 * Already done. Detach and go around again (if any remain).
					 */
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
