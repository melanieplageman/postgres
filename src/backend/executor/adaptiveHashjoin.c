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
	int			batchno;
	ParallelHashJoinBatch *phj_batch;
	SharedTuplestoreAccessor *outer_tuples;
	SharedTuplestoreAccessor *inner_tuples;
	Barrier    *barriers;
	Barrier    *chunk_barrier;
	Barrier    *old_chunk_barrier;

	hashtable = hjstate->hj_HashTable;
	batchno = hashtable->curbatch;
	phj_batch = hashtable->batches[batchno].shared;
	outer_tuples = hashtable->batches[batchno].outer_tuples;
	inner_tuples = hashtable->batches[batchno].inner_tuples;

	/*
	 * These chunk_barriers are initialized in the ELECTING phase when this
	 * worker attached to the batch in ExecParallelHashJoinNewBatch()
	 */
	barriers = dsa_get_address(hashtable->area, hashtable->batches[batchno].shared->chunk_barriers);
	LWLockAcquire(&phj_batch->lock, LW_SHARED);
	old_chunk_barrier = &(barriers[phj_batch->current_chunk - 1]);
	LWLockRelease(&phj_batch->lock);

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
		if (BarrierArriveAndWait(old_chunk_barrier,
								 WAIT_EVENT_HASH_CHUNK_PROBING))
			phj_batch->current_chunk++;
		BarrierDetach(old_chunk_barrier);
		/* Once the barrier is advanced we'll be in the DONE phase */
	}
	/* TODO: definitely seems like a race condition around value of current_chunk */
	LWLockAcquire(&phj_batch->lock, LW_SHARED);
	if (phj_batch->current_chunk > phj_batch->total_chunks)
	{
		LWLockRelease(&phj_batch->lock);
		return false;
	}
	chunk_barrier = &(barriers[phj_batch->current_chunk - 1]);
	LWLockRelease(&phj_batch->lock);

	/*
	 * The outer side is exhausted and either 1) the current chunk of the
	 * inner side is exhausted and it is time to advance the chunk 2) the last
	 * chunk of the inner side is exhausted and it is time to advance the
	 * batch
	 */

	for (;;)
	{
		switch (BarrierAttach(chunk_barrier))
		{
			case PHJ_CHUNK_ELECTING:
				if (BarrierArriveAndWait(chunk_barrier,
										 WAIT_EVENT_HASH_CHUNK_ELECTING))
				{

					sts_reinitialize(outer_tuples);

					/* set the rewound flag back to false to prepare for the next stripe */
					sts_reset_rewound(inner_tuples);

					/*
					 * reset inner's hashtable and recycle the existing bucket
					 * array.
					 * TODO: this will unnecessarily reset the hashtable for the
					 * first stripe. fix this?
					 */
					dsa_pointer_atomic *buckets = (dsa_pointer_atomic *)
					dsa_get_address(hashtable->area, phj_batch->buckets);

					for (size_t i = 0; i < hashtable->nbuckets; ++i)
						dsa_pointer_atomic_write(&buckets[i], InvalidDsaPointer);
				}
				/* Fall through. */
			case PHJ_CHUNK_RESETTING:
				BarrierArriveAndWait(chunk_barrier, WAIT_EVENT_HASH_CHUNK_RESETTING);
			case PHJ_CHUNK_LOADING:
				/* Start (or join in) loading the next chunk of inner tuples. */
				sts_resume_parallel_scan(inner_tuples);

				MinimalTuple tuple;
				tupleMetadata metadata;

				while ((tuple = sts_parallel_scan_next(inner_tuples, &metadata)))
				{
					int current_stripe;
					LWLockAcquire(&phj_batch->lock, LW_SHARED);
					current_stripe = phj_batch->current_chunk;
					LWLockRelease(&phj_batch->lock);

					/* tuple from past. skip */
					if (metadata.chunk < current_stripe)
						continue;
					/* tuple from future. time to back out read_page. end of stripe */
					else if (metadata.chunk > current_stripe)
					{
						sts_backout_chunk(inner_tuples);
						if (sts_seen_all_participants(inner_tuples))
							break;

						sts_ready_for_next_stripe(inner_tuples);
						continue;
					}
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
				/*
				 * TODO: Is it a race condition where a worker enters here
				 * and starts probing before the hashtable is fully loaded?
				 */
				sts_begin_parallel_scan(outer_tuples);
				return true;

			case PHJ_CHUNK_DONE:
				LWLockAcquire(&phj_batch->lock, LW_SHARED);
				if (phj_batch->current_chunk > phj_batch->total_chunks)
				{
					LWLockRelease(&phj_batch->lock);
					return false;
				}
				LWLockRelease(&phj_batch->lock);
				/* TODO: exercise this somehow (ideally, in a test) */
				BarrierDetach(chunk_barrier);
				if (chunk_barrier < barriers + phj_batch->total_chunks)
				{
					++chunk_barrier;
					continue;
				}
				else
					return false;

			default:
				elog(ERROR, "unexpected chunk phase %d. pid %i. batch %i.",
					 BarrierPhase(chunk_barrier), MyProcPid, batchno);
		}
	}

	return false;
}


static void
ExecHashTableLoopDetachBatchForChosen(HashJoinTable hashtable)
{
	if (hashtable->parallel_state != NULL &&
		hashtable->curbatch >= 0)
	{
		int			curbatch = hashtable->curbatch;
		ParallelHashJoinBatch *batch = hashtable->batches[curbatch].shared;

		/* Make sure any temporary files are closed. */
		sts_end_parallel_scan(hashtable->batches[curbatch].inner_tuples);

		/* Detach from the batch we were last working on. */

		/*
		 * Technically we shouldn't access the barrier because we're no longer
		 * attached, but since there is no way it's moving after this point it
		 * seems safe to make the following assertion.
		 */
		Assert(BarrierPhase(&batch->batch_barrier) == PHJ_BATCH_DONE);

		/* Free shared chunks and buckets. */
		while (DsaPointerIsValid(batch->chunks))
		{
			HashMemoryChunk chunk =
			dsa_get_address(hashtable->area, batch->chunks);
			dsa_pointer next = chunk->next.shared;

			dsa_free(hashtable->area, batch->chunks);
			batch->chunks = next;
		}
		if (DsaPointerIsValid(batch->buckets))
		{
			dsa_free(hashtable->area, batch->buckets);
			batch->buckets = InvalidDsaPointer;
		}

		/*
		 * Free chunk barrier
		 */
		/* TODO: why is this NULL check needed? */
		if (DsaPointerIsValid(batch->chunk_barriers))
		{
			dsa_free(hashtable->area, batch->chunk_barriers);
			batch->chunk_barriers = InvalidDsaPointer;
		}

		/*
		 * Track the largest batch we've been attached to.  Though each
		 * backend might see a different subset of batches, explain.c will
		 * scan the results from all backends to find the largest value.
		 */
		hashtable->spacePeak =
			Max(hashtable->spacePeak,
				batch->size + sizeof(dsa_pointer_atomic) * hashtable->nbuckets);

	}
}

static void
ExecHashTableLoopDetachBatchForOthers(HashJoinTable hashtable)
{
	if (hashtable->parallel_state != NULL &&
		hashtable->curbatch >= 0)
	{
		int			curbatch = hashtable->curbatch;
		ParallelHashJoinBatch *batch = hashtable->batches[curbatch].shared;

		sts_end_parallel_scan(hashtable->batches[curbatch].inner_tuples);
		sts_end_parallel_scan(hashtable->batches[curbatch].outer_tuples);

		/*
		 * Track the largest batch we've been attached to.  Though each
		 * backend might see a different subset of batches, explain.c will
		 * scan the results from all backends to find the largest value.
		 */
		hashtable->spacePeak =
			Max(hashtable->spacePeak,
				batch->size + sizeof(dsa_pointer_atomic) * hashtable->nbuckets);

		/* Remember that we are not attached to a batch. */
		hashtable->curbatch = -1;
	}
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
	 * If we were already attached to a batch, remember not to bother checking
	 * it again, and detach from it (possibly freeing the hash table if we are
	 * last to detach). curbatch is set when the batch_barrier phase is either
	 * PHJ_BATCH_LOADING or PHJ_BATCH_CHUNKING (note that the
	 * PHJ_BATCH_LOADING case will fall through to the PHJ_BATCH_CHUNKING
	 * case). The PHJ_BATCH_CHUNKING case returns to the caller. So when this
	 * function is reentered with a curbatch >= 0 then we must be done
	 * probing.
	 */
	if (hashtable->curbatch >= 0)
	{
		ParallelHashJoinBatchAccessor *accessor = hashtable->batches + hashtable->curbatch;
		ParallelHashJoinBatch *batch = accessor->shared;

		if (!batch->parallel_hashloop_fallback)
		{
			hashtable->batches[hashtable->curbatch].done = true;
			ExecHashTableDetachBatch(hashtable);
		}

		else if (sb_combined_exists(accessor->sba))
		{
			sb_end_read(accessor->sba);
			accessor->done = true;

			/*
			 * though we have already de-commissioned the shared area of the
			 * hashtable the curbatch is backend-local and should still be
			 * valid
			 */
			sts_end_parallel_scan(hashtable->batches[hashtable->curbatch].outer_tuples);
			hashtable->curbatch = -1;
		}

		else
		{
			sb_end_write(accessor->sba);

			/*
			 * If all workers (including this one) have finished probing the
			 * batch, one worker is elected to Loop through the outer match
			 * status files from all workers that were attached to this batch
			 * Combine them into one bitmap Use the bitmap, loop through the
			 * outer batch file again, and emit unmatched tuples All workers
			 * will detach from the batch barrier and the last worker will
			 * clean up the hashtable. All workers except the last worker will
			 * end their scans of the outer and inner side The last worker
			 * will end its scan of the inner side
			 */
			if (BarrierArriveAndDetach(&batch->batch_barrier))
			{
				/*
				 * For hashloop fallback only Only the elected worker who was
				 * chosen to combine the outer match status bitmaps should
				 * reach here. This worker must do some final cleanup and then
				 * detach from the batch
				 */
				sb_combine(accessor->sba);
				ExecHashTableLoopDetachBatchForChosen(hashtable);
				hjstate->last_worker = true;
				return true;
			}
			/* the elected combining worker should not reach here */
			else
			{
				hashtable->batches[hashtable->curbatch].done = true;
				ExecHashTableLoopDetachBatchForOthers(hashtable);
			}
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
					if (BarrierArriveAndWait(batch_barrier,
											 WAIT_EVENT_HASH_BATCH_ELECTING))
					{
						ExecParallelHashTableAlloc(hashtable, batchno);
						ParallelHashJoinBatch *phj_batch = hashtable->batches[batchno].shared;

						phj_batch->chunk_barriers = dsa_allocate(hashtable->area, phj_batch->total_chunks * sizeof(Barrier));
						Barrier    *barriers = dsa_get_address(hashtable->area, phj_batch->chunk_barriers);

						for (int i = 0; i < phj_batch->total_chunks; i++)
						{
							BarrierInit(&(barriers[i]), 0);
						}
						phj_batch->current_chunk = 1;
						/* one worker needs to 0 out the read_pages of all the participants in the new batch */
						sts_reinitialize(hashtable->batches[batchno].inner_tuples);
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

					sts_begin_parallel_scan(hashtable->batches[batchno].inner_tuples);

					/*
					 * Create an outer match status file for this batch for
					 * this worker This file must be accessible to the other
					 * workers But *only* written to by this worker. Written
					 * to by this worker and readable by any worker
					 */
					if (hashtable->batches[batchno].shared->parallel_hashloop_fallback)
					{
						ParallelHashJoinBatchAccessor *accessor = hashtable->batches + hashtable->curbatch;

						sb_initialize_accessor(accessor->sba, sts_get_tuplenum(accessor->outer_tuples));
					}

					return true;

				case PHJ_BATCH_DONE:

					/*
					 * Already done. Detach and go around again (if any
					 * remain).
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
