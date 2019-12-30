/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeHashjoin.c
 *
 * PARALLELISM
 *
 * Hash joins can participate in parallel query execution in several ways.  A
 * parallel-oblivious hash join is one where the node is unaware that it is
 * part of a parallel plan.  In this case, a copy of the inner plan is used to
 * build a copy of the hash table in every backend, and the outer plan could
 * either be built from a partial or complete path, so that the results of the
 * hash join are correspondingly either partial or complete.  A parallel-aware
 * hash join is one that behaves differently, coordinating work between
 * backends, and appears as Parallel Hash Join in EXPLAIN output.  A Parallel
 * Hash Join always appears with a Parallel Hash node.
 *
 * Parallel-aware hash joins use the same per-backend state machine to track
 * progress through the hash join algorithm as parallel-oblivious hash joins.
 * In a parallel-aware hash join, there is also a shared state machine that
 * co-operating backends use to synchronize their local state machines and
 * program counters.  The shared state machine is managed with a Barrier IPC
 * primitive.  When all attached participants arrive at a barrier, the phase
 * advances and all waiting participants are released.
 *
 * When a participant begins working on a parallel hash join, it must first
 * figure out how much progress has already been made, because participants
 * don't wait for each other to begin.  For this reason there are switch
 * statements at key points in the code where we have to synchronize our local
 * state machine with the phase, and then jump to the correct part of the
 * algorithm so that we can get started.
 *
 * One barrier called build_barrier is used to coordinate the hashing phases.
 * The phase is represented by an integer which begins at zero and increments
 * one by one, but in the code it is referred to by symbolic names as follows:
 *
 *   PHJ_BUILD_ELECTING              -- initial state
 *   PHJ_BUILD_ALLOCATING            -- one sets up the batches and table 0
 *   PHJ_BUILD_HASHING_INNER         -- all hash the inner rel
 *   PHJ_BUILD_HASHING_OUTER         -- (multi-batch only) all hash the outer
 *   PHJ_BUILD_DONE                  -- building done, probing can begin
 *
 * While in the phase PHJ_BUILD_HASHING_INNER a separate pair of barriers may
 * be used repeatedly as required to coordinate expansions in the number of
 * batches or buckets.  Their phases are as follows:
 *
 *   PHJ_GROW_BATCHES_ELECTING       -- initial state
 *   PHJ_GROW_BATCHES_ALLOCATING     -- one allocates new batches
 *   PHJ_GROW_BATCHES_REPARTITIONING -- all repartition
 *   PHJ_GROW_BATCHES_FINISHING      -- one cleans up, detects skew
 *
 *   PHJ_GROW_BUCKETS_ELECTING       -- initial state
 *   PHJ_GROW_BUCKETS_ALLOCATING     -- one allocates new buckets
 *   PHJ_GROW_BUCKETS_REINSERTING    -- all insert tuples
 *
 * If the planner got the number of batches and buckets right, those won't be
 * necessary, but on the other hand we might finish up needing to expand the
 * buckets or batches multiple times while hashing the inner relation to stay
 * within our memory budget and load factor target.  For that reason it's a
 * separate pair of barriers using circular phases.
 *
 * The PHJ_BUILD_HASHING_OUTER phase is required only for multi-batch joins,
 * because we need to divide the outer relation into batches up front in order
 * to be able to process batches entirely independently.  In contrast, the
 * parallel-oblivious algorithm simply throws tuples 'forward' to 'later'
 * batches whenever it encounters them while scanning and probing, which it
 * can do because it processes batches in serial order.
 *
 * Once PHJ_BUILD_DONE is reached, backends then split up and process
 * different batches, or gang up and work together on probing batches if there
 * aren't enough to go around.  For each batch there is a separate barrier
 * with the following phases:
 *
 *  PHJ_BATCH_ELECTING       -- initial state
 *  PHJ_BATCH_ALLOCATING     -- one allocates buckets
 *  PHJ_BATCH_LOADING        -- all load the hash table from disk
 *  PHJ_BATCH_CHUNKING       -- all probe
 *  PHJ_BATCH_DONE           -- end
 *
 * Batch 0 is a special case, because it starts out in phase
 * PHJ_BATCH_CHUNKING; populating batch 0's hash table is done during
 * PHJ_BUILD_HASHING_INNER so we can skip loading.
 *
 * Initially we try to plan for a single-batch hash join using the combined
 * work_mem of all participants to create a large shared hash table.  If that
 * turns out either at planning or execution time to be impossible then we
 * fall back to regular work_mem sized hash tables.
 *
 * To avoid deadlocks, we never wait for any barrier unless it is known that
 * all other backends attached to it are actively executing the node or have
 * already arrived.  Practically, that means that we never return a tuple
 * while attached to a barrier, unless the barrier has reached its final
 * state.  In the slightly special case of the per-batch barrier, we return
 * tuples while in PHJ_BATCH_CHUNKING phase, but that's OK because we use
 * BarrierArriveAndDetach() to advance it to PHJ_BATCH_DONE without waiting.
 *
 *-------------------------------------------------------------------------
 */

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
 * States of the ExecHashJoin state machine
 */
#define HJ_BUILD_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_SCAN_BUCKET			3
#define HJ_FILL_INNER_TUPLES    4
#define HJ_NEED_NEW_BATCH		5
#define HJ_NEED_NEW_INNER_CHUNK 6
#define HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT 7
#define HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER 8

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)

static TupleTableSlot *ExecHashJoinOuterGetTuple(PlanState *outerNode,
												 HashJoinState *hjstate,
												 uint32 *hashvalue);
static TupleTableSlot *ExecParallelHashJoinOuterGetTuple(PlanState *outerNode,
														 HashJoinState *hjstate,
														 uint32 *hashvalue);
static TupleTableSlot *ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
												 BufFile *file,
												 uint32 *hashvalue,
												 TupleTableSlot *tupleSlot);

static bool ExecHashJoinAdvanceBatch(HashJoinState *hjstate);
static bool ExecHashJoinLoadInnerBatch(HashJoinState *hjstate);
static void ExecParallelHashJoinPartitionOuter(HashJoinState *node);

static TupleTableSlot *emitUnmatchedOuterTuple(ExprState *otherqual,
											   ExprContext *econtext,
											   HashJoinState *hjstate);


/* ----------------------------------------------------------------
 *		ExecHashJoinImpl
 *
 *		This function implements the Hybrid Hashjoin algorithm.  It is marked
 *		with an always-inline attribute so that ExecHashJoin() and
 *		ExecParallelHashJoin() can inline it.  Compilers that respect the
 *		attribute should create versions specialized for parallel == true and
 *		parallel == false with unnecessary branches removed.
 *
 *		Note: the relation we build hash table on is the "inner"
 *			  the other one is "outer".
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		Parallel-oblivious version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecHashJoin(PlanState *pstate)
{
	HashJoinState *node = castNode(HashJoinState, pstate);
	PlanState  *outerNode;
	HashState  *hashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	HashJoinTable hashtable;
	TupleTableSlot *outerTupleSlot;
	uint32		hashvalue;
	int			batchno;

	BufFile    *outerFileForAdaptiveRead;

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNode = (HashState *) innerPlanState(node);
	outerNode = outerPlanState(node);
	hashtable = node->hj_HashTable;
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * run the hash join state machine
	 */
	for (;;)
	{
		/*
		 * It's possible to iterate this loop many times before returning a
		 * tuple, in some pathological cases such as needing to move much of
		 * the current batch to a later batch.  So let's check for interrupts
		 * each time through.
		 */
		CHECK_FOR_INTERRUPTS();

		switch (node->hj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:

				/*
				 * First time through: build hash table for inner relation.
				 */
				Assert(hashtable == NULL);

				/*
				 * If the outer relation is completely empty, and it's not
				 * right/full join, we can quit without building the hash
				 * table.  However, for an inner join it is only a win to
				 * check this when the outer relation's startup cost is less
				 * than the projected cost of building the hash table.
				 * Otherwise it's best to build the hash table first and see
				 * if the inner relation is empty.  (When it's a left join, we
				 * should always make this check, since we aren't going to be
				 * able to skip the join on the strength of an empty inner
				 * relation anyway.)
				 *
				 * If we are rescanning the join, we make use of information
				 * gained on the previous scan: don't bother to try the
				 * prefetch if the previous scan found the outer relation
				 * nonempty. This is not 100% reliable since with new
				 * parameters the outer relation might yield different
				 * results, but it's a good heuristic.
				 *
				 * The only way to make the check is to try to fetch a tuple
				 * from the outer plan node.  If we succeed, we have to stash
				 * it away for later consumption by ExecHashJoinOuterGetTuple.
				 */
				if (HJ_FILL_INNER(node))
				{
					/* no chance to not build the hash table */
					node->hj_FirstOuterTupleSlot = NULL;
				}
				else if (HJ_FILL_OUTER(node) ||
						 (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
						  !node->hj_OuterNotEmpty))
				{
					node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
					if (TupIsNull(node->hj_FirstOuterTupleSlot))
					{
						node->hj_OuterNotEmpty = false;
						return NULL;
					}
					else
						node->hj_OuterNotEmpty = true;
				}
				else
					node->hj_FirstOuterTupleSlot = NULL;

				/* Create the hash table. */
				hashtable = ExecHashTableCreate(hashNode,
												node->hj_HashOperators,
												node->hj_Collations,
												HJ_FILL_INNER(node));
				node->hj_HashTable = hashtable;

				/* Execute the Hash node, to build the hash table. */
				hashNode->hashtable = hashtable;
				(void) MultiExecProcNode((PlanState *) hashNode);

				/*
				 * If the inner relation is completely empty, and we're not
				 * doing a left outer join, we can quit without scanning the
				 * outer relation.
				 */
				if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node))
					return NULL;

				/*
				 * need to remember whether nbatch has increased since we
				 * began scanning the outer relation
				 */
				hashtable->nbatch_outstart = hashtable->nbatch;

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */
				node->hj_OuterNotEmpty = false;

				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/* FALL THRU */

			case HJ_NEED_NEW_OUTER:

				/*
				 * We don't have an outer tuple, try to get the next one
				 */
				outerTupleSlot =
					ExecHashJoinOuterGetTuple(outerNode, node, &hashvalue);

				if (TupIsNull(outerTupleSlot))
				{
					/*
					 * end of batch, or maybe whole join. for hashloop
					 * fallback, all we know is outer batch is exhausted.
					 * inner could have more chunks
					 */
					if (HJ_FILL_INNER(node))
					{
						/* set up to scan for unmatched inner tuples */
						ExecPrepHashTableForUnmatched(node);
						node->hj_JoinState = HJ_FILL_INNER_TUPLES;
						break;
					}
					node->hj_JoinState = HJ_NEED_NEW_INNER_CHUNK;
					break;
				}

				econtext->ecxt_outertuple = outerTupleSlot;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->hj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch(hashtable, hashvalue,
										  &node->hj_CurBucketNo, &batchno);
				node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
																 hashvalue);
				node->hj_CurTuple = NULL;

				/*
				 * for the hashloop fallback case, only initialize
				 * hj_MatchedOuter to false during the first chunk. otherwise,
				 * we will be resetting hj_MatchedOuter to false for an outer
				 * tuple that has already matched an inner tuple. also,
				 * hj_MatchedOuter should be set to false for batch 0. there
				 * are no chunks for batch 0, and node->hj_InnerFirstChunk
				 * isn't set to true until HJ_NEED_NEW_BATCH, so need to
				 * handle batch 0 explicitly
				 */

				if (!node->hashloop_fallback || hashtable->curbatch == 0 || node->hj_InnerFirstChunk)
					node->hj_MatchedOuter = false;

				/*
				 * The tuple might not belong to the current batch (where
				 * "current batch" includes the skew buckets if any).
				 */
				if (batchno != hashtable->curbatch &&
					node->hj_CurSkewBucketNo == INVALID_SKEW_BUCKET_NO)
				{
					bool		shouldFree;
					MinimalTuple mintuple = ExecFetchSlotMinimalTuple(outerTupleSlot,
																	  &shouldFree);

					/*
					 * Need to postpone this outer tuple to a later batch.
					 * Save it in the corresponding outer-batch file.
					 */
					Assert(batchno > hashtable->curbatch);
					ExecHashJoinSaveTuple(mintuple, hashvalue,
										  &hashtable->outerBatchFile[batchno]);

					if (shouldFree)
						heap_free_minimal_tuple(mintuple);

					/* Loop around, staying in HJ_NEED_NEW_OUTER state */
					continue;
				}

				if (node->hashloop_fallback)
				{
					/* first tuple of new batch */
					if (node->hj_OuterMatchStatusesFile == NULL)
					{
						node->hj_OuterTupleCount = 0;
						node->hj_OuterMatchStatusesFile = BufFileCreateTemp(false);
					}

					/* for fallback case, always increment tuple count */
					node->hj_OuterTupleCount++;

					/* Use the next byte on every 8th tuple */
					if ((node->hj_OuterTupleCount - 1) % 8 == 0)
					{
						/*
						 * first chunk of new batch, so write and initialize
						 * enough bytes in the outer tuple match status file
						 * to capture all tuples' match statuses
						 */
						if (node->hj_InnerFirstChunk)
						{
							node->hj_OuterCurrentByte = 0;
							BufFileWrite(node->hj_OuterMatchStatusesFile, &node->hj_OuterCurrentByte, 1);
						}
						/* otherwise, just read the next byte */
						else
							BufFileRead(node->hj_OuterMatchStatusesFile, &node->hj_OuterCurrentByte, 1);
					}
				}

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

			case HJ_SCAN_BUCKET:

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (!ExecScanHashBucket(node, econtext))
				{
					/*
					 * The current outer tuple has run out of matches, so
					 * check whether to emit a dummy outer-join tuple.
					 * Whether we emit one or not, the next state is
					 * NEED_NEW_OUTER.
					 */
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					if (!node->hashloop_fallback || node->hj_HashTable->curbatch == 0)
					{
						TupleTableSlot *slot = emitUnmatchedOuterTuple(otherqual, econtext, node);

						if (slot != NULL)
							return slot;
					}
					continue;
				}

				if (joinqual != NULL && !ExecQual(joinqual, econtext))
				{
					InstrCountFiltered1(node, 1);
					break;
				}

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecScanHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */

				node->hj_MatchedOuter = true;

				/*
				 * This is really only needed if HJ_FILL_INNER(node),
				 * but we'll avoid the branch and just set it always.
				 */
				HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

				/* In an antijoin, we never return a matched tuple */
				if (node->js.jointype == JOIN_ANTI)
				{
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					continue;
				}

				/*
				 * If we only need to join to the first matching inner tuple,
				 * then consider returning this one, but after that, continue
				 * with next outer tuple.
				 */
				/* TODO: is semi-join correct for AHJ */
				if (node->js.single_match)
					node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/*
				 * Set the match bit for this outer tuple in the match status
				 * file
				 */
				if (node->hj_OuterMatchStatusesFile != NULL)
				{
					Assert(node->hashloop_fallback == true);
					int			byte_to_set = (node->hj_OuterTupleCount - 1) / 8;
					int			bit_to_set_in_byte = (node->hj_OuterTupleCount - 1) % 8;

					BufFileSeek(node->hj_OuterMatchStatusesFile, 0, byte_to_set, SEEK_SET);

					node->hj_OuterCurrentByte = node->hj_OuterCurrentByte | (1 << bit_to_set_in_byte);

					BufFileWrite(node->hj_OuterMatchStatusesFile, &node->hj_OuterCurrentByte, 1);
				}

				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				InstrCountFiltered2(node, 1);
				break;

			case HJ_FILL_INNER_TUPLES:

				/*
				 * We have finished a batch, but we are doing right/full join,
				 * so any unmatched inner tuples in the hashtable have to be
				 * emitted before we continue to the next batch.
				 */
				if (!ExecScanHashTableForUnmatched(node, econtext))
				{
					/* no more unmatched tuples */
					node->hj_JoinState = HJ_NEED_NEW_INNER_CHUNK;
					continue;
				}

				/*
				 * Generate a fake join tuple with nulls for the outer tuple,
				 * and return it if it passes the non-join quals.
				 */
				econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				InstrCountFiltered2(node, 1);
				break;

			case HJ_NEED_NEW_BATCH:

				/*
				 * Try to advance to next batch.  Done if there are no more.
				 * for batches after batch 0 for which hashloop_fallback is
				 * true, if inner is exhausted, need to consider emitting
				 * unmatched tuples we should never get here when
				 * hashloop_fallback is false but hj_InnerExhausted is true,
				 * however, it felt more clear to check for hashloop_fallback
				 * explicitly
				 */
				if (node->hashloop_fallback && HJ_FILL_OUTER(node) && node->hj_InnerExhausted)
				{
					/*
					 * For hashloop fallback, outer tuples are not emitted
					 * until directly before advancing the batch (after all
					 * inner chunks have been processed).
					 * node->hashloop_fallback should be true because it is
					 * not reset to false until advancing the batches
					 */
					node->hj_InnerExhausted = false;
					node->hj_JoinState = HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT;
					break;
				}

				if (!ExecHashJoinAdvanceBatch(node))
					return NULL;

				/*
				 * TODO: need to find a better way to distinguish if I should
				 * load inner batch again than checking for outer batch file
				 */
				/* I need to also do this even if it is NULL when it is a ROJ */

				/*
				 * need to load inner again if it is an inner or left outer
				 * join and there are outer tuples in the batch OR
				 */

				/*
				 * if it is a ROJ and there are inner tuples in the batch --
				 * should never have no tuples in either batch...
				 */
				if (BufFileRewindIfExists(node->hj_HashTable->outerBatchFile[node->hj_HashTable->curbatch]) != NULL ||
					(node->hj_HashTable->innerBatchFile[node->hj_HashTable->curbatch] != NULL && HJ_FILL_INNER(node)))
					ExecHashJoinLoadInnerBatch(node);	/* TODO: should I ever
														 * load inner when outer
														 * file is not present? */

				node->hj_JoinState = HJ_NEED_NEW_OUTER;
				break;

			case HJ_NEED_NEW_INNER_CHUNK:

				if (!node->hashloop_fallback)
				{
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
					break;
				}

				/*
				 * it is the hashloop fallback case and there are no more
				 * chunks inner is exhausted, so we must advance the batches
				 */
				if (node->hj_InnerPageOffset == 0L)
				{
					node->hj_InnerExhausted = true;
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
					break;
				}

				/*
				 * This is the hashloop fallback case and we have more chunks
				 * in inner. curbatch > 0. Rewind outer batch file (if
				 * present) so that we can start reading it. Rewind outer
				 * match statuses file if present so that we can set match
				 * bits as needed. Reset the tuple count and load the next
				 * chunk of inner. Then proceed to get a new outer tuple from
				 * our rewound outer batch file
				 */
				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/*
				 * TODO: need to find a better way to distinguish if I should
				 * load inner batch again than checking for outer batch file
				 */
				/* I need to also do this even if it is NULL when it is a ROJ */

				/*
				 * need to load inner again if it is an inner or left outer
				 * join and there are outer tuples in the batch OR
				 */

				/*
				 * if it is a ROJ and there are inner tuples in the batch --
				 * should never have no tuples in either batch...
				 */

				/*
				 * if outer is not null or if it is a ROJ and inner is not
				 * null, must rewind outer match status and load inner
				 */
				if (BufFileRewindIfExists(node->hj_HashTable->outerBatchFile[node->hj_HashTable->curbatch]) != NULL ||
					(node->hj_HashTable->innerBatchFile[node->hj_HashTable->curbatch] != NULL && HJ_FILL_INNER(node)))
				{
					BufFileRewindIfExists(node->hj_OuterMatchStatusesFile);
					node->hj_OuterTupleCount = 0;
					ExecHashJoinLoadInnerBatch(node);
				}
				break;

			case HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT:

				node->hj_OuterTupleCount = 0;
				BufFileRewindIfExists(node->hj_OuterMatchStatusesFile);

				/*
				 * TODO: is it okay to use the hashtable to get the outer
				 * batch file here?
				 */
				outerFileForAdaptiveRead = hashtable->outerBatchFile[hashtable->curbatch];
				if (outerFileForAdaptiveRead == NULL)	/* TODO: could this
														 * happen */
				{
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
					break;
				}
				BufFileRewindIfExists(outerFileForAdaptiveRead);

				node->hj_JoinState = HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER;
				/* fall through */

			case HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER:

				outerFileForAdaptiveRead = hashtable->outerBatchFile[hashtable->curbatch];

				while (true)
				{
					uint32		unmatchedOuterHashvalue;
					TupleTableSlot *slot = ExecHashJoinGetSavedTuple(node,
																	 outerFileForAdaptiveRead,
																	 &unmatchedOuterHashvalue,
																	 node->hj_OuterTupleSlot);

					node->hj_OuterTupleCount++;

					if (slot == NULL)
					{
						node->hj_JoinState = HJ_NEED_NEW_BATCH;
						break;
					}

					unsigned char bit = (node->hj_OuterTupleCount - 1) % 8;

					/* need to read the next byte */
					if (bit == 0)
						BufFileRead(node->hj_OuterMatchStatusesFile, &node->hj_OuterCurrentByte, 1);

					/* if the match bit is set for this tuple, continue */
					if ((node->hj_OuterCurrentByte >> bit) & 1)
						continue;

					/* if it is not a match then emit it NULL-extended */
					econtext->ecxt_outertuple = slot;
					econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;
					return ExecProject(node->js.ps.ps_ProjInfo);
				}
				/* came here from HJ_NEED_NEW_BATCH, so go back there */
				node->hj_JoinState = HJ_NEED_NEW_BATCH;
				break;

			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecParallelHashJoin
 *
 *		Parallel-aware version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecParallelHashJoin(PlanState *pstate)
{
	HashJoinState *node = castNode(HashJoinState, pstate);
	PlanState  *outerNode;
	HashState  *hashNode;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	HashJoinTable hashtable;
	TupleTableSlot *outerTupleSlot;
	uint32		hashvalue;
	int			batchno;
	ParallelHashJoinState *parallel_state;

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	hashNode = (HashState *) innerPlanState(node);
	outerNode = outerPlanState(node);
	hashtable = node->hj_HashTable;
	econtext = node->js.ps.ps_ExprContext;
	parallel_state = hashNode->parallel_state;

	bool		advance_from_probing = false;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * run the hash join state machine
	 */
	for (;;)
	{
		SharedTuplestoreAccessor *outer_acc;

		/*
		 * It's possible to iterate this loop many times before returning a
		 * tuple, in some pathological cases such as needing to move much of
		 * the current batch to a later batch.  So let's check for interrupts
		 * each time through.
		 */
		CHECK_FOR_INTERRUPTS();

		switch (node->hj_JoinState)
		{
			case HJ_BUILD_HASHTABLE:

				/*
				 * First time through: build hash table for inner relation.
				 */
				Assert(hashtable == NULL);
				/* volatile int mybp = 0; while (mybp == 0); */

				/*
				 * The empty-outer optimization is not implemented for shared
				 * hash tables, because no one participant can determine that
				 * there are no outer tuples, and it's not yet clear that it's
				 * worth the synchronization overhead of reaching consensus to
				 * figure that out.  So we have to build the hash table.
				 */
				node->hj_FirstOuterTupleSlot = NULL;

				/*
				 * Create the hash table.  If using Parallel Hash, then
				 * whoever gets here first will create the hash table and any
				 * later arrivals will merely attach to it.
				 */
				node->hj_HashTable = hashtable = ExecHashTableCreate(hashNode,
																	 node->hj_HashOperators,
																	 node->hj_Collations,
																	 HJ_FILL_INNER(node));

				/*
				 * Execute the Hash node, to build the hash table.  If using
				 * Parallel Hash, then we'll try to help hashing unless we
				 * arrived too late.
				 */
				hashNode->hashtable = hashtable;
				(void) MultiExecProcNode((PlanState *) hashNode);

				/*
				 * If the inner relation is completely empty, and we're not
				 * doing a left outer join, we can quit without scanning the
				 * outer relation.
				 */
				if (hashtable->totalTuples == 0 && !HJ_FILL_OUTER(node))
					return NULL;

				/*
				 * need to remember whether nbatch has increased since we
				 * began scanning the outer relation
				 */
				hashtable->nbatch_outstart = hashtable->nbatch;

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */
				node->hj_OuterNotEmpty = false;

				Barrier    *build_barrier;

				build_barrier = &parallel_state->build_barrier;
				Assert(BarrierPhase(build_barrier) == PHJ_BUILD_HASHING_OUTER ||
					   BarrierPhase(build_barrier) == PHJ_BUILD_DONE);
				if (BarrierPhase(build_barrier) == PHJ_BUILD_HASHING_OUTER)
				{
					/*
					 * If multi-batch, we need to hash the outer relation up
					 * front.
					 */
					if (hashtable->nbatch > 1)
						ExecParallelHashJoinPartitionOuter(node);
					BarrierArriveAndWait(build_barrier,
										 WAIT_EVENT_HASH_BUILD_HASHING_OUTER);
				}
				Assert(BarrierPhase(build_barrier) == PHJ_BUILD_DONE);

				/* Each backend should now select a batch to work on. */
				hashtable->curbatch = -1;
				node->hj_JoinState = HJ_NEED_NEW_BATCH;

				continue;

			case HJ_NEED_NEW_OUTER:

				/*
				 * We don't have an outer tuple, try to get the next one
				 */
				outerTupleSlot =
					ExecParallelHashJoinOuterGetTuple(outerNode, node,
													  &hashvalue);

				if (TupIsNull(outerTupleSlot))
				{
					/*
					 * end of batch, or maybe whole join. for hashloop
					 * fallback, all we know is outer batch is exhausted.
					 * inner could have more chunks
					 */
					if (HJ_FILL_INNER(node))
					{
						/* set up to scan for unmatched inner tuples */
						ExecPrepHashTableForUnmatched(node);
						node->hj_JoinState = HJ_FILL_INNER_TUPLES;
						break;
					}
					advance_from_probing = true;
					node->hj_JoinState = HJ_NEED_NEW_INNER_CHUNK;
					break;
				}

				econtext->ecxt_outertuple = outerTupleSlot;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->hj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch(hashtable, hashvalue,
										  &node->hj_CurBucketNo, &batchno);
				node->hj_CurSkewBucketNo = ExecHashGetSkewBucket(hashtable,
																 hashvalue);
				node->hj_CurTuple = NULL;

				/*
				 * for the hashloop fallback case, only initialize
				 * hj_MatchedOuter to false during the first chunk. otherwise,
				 * we will be resetting hj_MatchedOuter to false for an outer
				 * tuple that has already matched an inner tuple. also,
				 * hj_MatchedOuter should be set to false for batch 0. there
				 * are no chunks for batch 0
				 */

				ParallelHashJoinBatch *phj_batch = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared;

				if (!phj_batch->parallel_hashloop_fallback || phj_batch->current_chunk_num == 1)
					node->hj_MatchedOuter = false;
				node->hj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

			case HJ_SCAN_BUCKET:

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				phj_batch = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared;

				if (!ExecParallelScanHashBucket(node, econtext))
				{
					/*
					 * The current outer tuple has run out of matches, so
					 * check whether to emit a dummy outer-join tuple.
					 * Whether we emit one or not, the next state is
					 * NEED_NEW_OUTER.
					 */
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					if (!phj_batch->parallel_hashloop_fallback)
					{
						TupleTableSlot *slot = emitUnmatchedOuterTuple(otherqual, econtext, node);

						if (slot != NULL)
							return slot;
					}
					continue;
				}

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecScanHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual != NULL && !ExecQual(joinqual, econtext))
				{
					InstrCountFiltered1(node, 1);
					break;
				}

				node->hj_MatchedOuter = true;
				/*
				 * Full/right outer joins are currently not supported
				 * for parallel joins, so we don't need to set the
				 * match bit.  Experiments show that it's worth
				 * avoiding the shared memory traffic on large
				 * systems.
				 */
				Assert(!HJ_FILL_INNER(node));

				/*
				 * TODO: how does this interact with PAHJ -- do I need to set
				 * matchbit?
				 */
				/* In an antijoin, we never return a matched tuple */
				if (node->js.jointype == JOIN_ANTI)
				{
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					continue;
				}

				/*
				 * If we only need to join to the first matching inner tuple,
				 * then consider returning this one, but after that continue
				 * with next outer tuple.
				 */
				if (node->js.single_match)
					node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/*
				 * Set the match bit for this outer tuple in the match status
				 * file
				 */
				if (phj_batch->parallel_hashloop_fallback)
				{
					sts_set_outer_match_status(hashtable->batches[hashtable->curbatch].outer_tuples,
											   econtext->ecxt_outertuple->tuplenum);

				}
				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;

			case HJ_FILL_INNER_TUPLES:

				/*
				 * We have finished a batch, but we are doing right/full join,
				 * so any unmatched inner tuples in the hashtable have to be
				 * emitted before we continue to the next batch.
				 */
				if (!ExecScanHashTableForUnmatched(node, econtext))
				{
					/* no more unmatched tuples */
					advance_from_probing = true;
					node->hj_JoinState = HJ_NEED_NEW_INNER_CHUNK;
					continue;
				}

				/*
				 * Generate a fake join tuple with nulls for the outer tuple,
				 * and return it if it passes the non-join quals.
				 */
				econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

				if (otherqual == NULL || ExecQual(otherqual, econtext))
					return ExecProject(node->js.ps.ps_ProjInfo);
				else
					InstrCountFiltered2(node, 1);
				break;

			case HJ_NEED_NEW_BATCH:

				phj_batch = hashtable->batches[hashtable->curbatch].shared;

				/*
				 * Try to advance to next batch.  Done if there are no more.
				 */
				if (!ExecParallelHashJoinNewBatch(node))
					return NULL;	/* end of parallel-aware join */

				if (node->last_worker
					&& HJ_FILL_OUTER(node) && phj_batch->parallel_hashloop_fallback)
				{
					node->last_worker = false;
					node->hj_JoinState = HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT;
					break;
				}
				if (node->hj_HashTable->curbatch == 0)
				{
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					break;
				}
				advance_from_probing = false;
				node->hj_JoinState = HJ_NEED_NEW_INNER_CHUNK;
				/* FALL THRU */

			case HJ_NEED_NEW_INNER_CHUNK:

				if (hashtable->curbatch == -1 || hashtable->curbatch == 0)

					/*
					 * If we're not attached to a batch at all then we need to
					 * go to HJ_NEED_NEW_BATCH. Also batch 0 doesn't have more
					 * than 1 chunk.
					 */
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
				else if (!ExecParallelHashJoinNewChunk(node, advance_from_probing))
					/* If there's no next chunk then go to the next batch */
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
				else
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
				break;

			case HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT:

				outer_acc = hashtable->batches[hashtable->curbatch].outer_tuples;
				sts_reinitialize(outer_acc);
				sts_begin_parallel_scan(outer_acc);

				node->hj_JoinState = HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER;
				/* FALL THRU */

			case HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER:

				Assert(node->combined_bitmap != NULL);

				outer_acc = node->hj_HashTable->batches[node->hj_HashTable->curbatch].outer_tuples;

				MinimalTuple tuple;

				do
				{
					tupleMetadata metadata;

					if ((tuple = sts_parallel_scan_next(outer_acc, &metadata)) == NULL)
						break;

					int			bytenum = metadata.tupleid / 8;
					unsigned char bit = metadata.tupleid % 8;
					unsigned char byte_to_check = 0;

					/* seek to byte to check */
					if (BufFileSeek(node->combined_bitmap, 0, bytenum, SEEK_SET))
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not rewind shared outer temporary file: %m")));
					/* read byte containing ntuple bit */
					if (BufFileRead(node->combined_bitmap, &byte_to_check, 1) == 0)
						ereport(ERROR,
								(errcode_for_file_access(),
								 errmsg("could not read byte in outer match status bitmap: %m.")));
					/* if bit is set */
					bool		match = ((byte_to_check) >> bit) & 1;

					if (!match)
						break;
				} while (1);

				if (tuple == NULL)
				{
					sts_end_parallel_scan(outer_acc);
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
					break;
				}

				/* Emit the unmatched tuple */
				ExecForceStoreMinimalTuple(tuple,
										   econtext->ecxt_outertuple,
										   false);
				econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

				return ExecProject(node->js.ps.ps_ProjInfo);


			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecInitHashJoin
 *
 *		Init routine for HashJoin node.
 * ----------------------------------------------------------------
 */
HashJoinState *
ExecInitHashJoin(HashJoin *node, EState *estate, int eflags)
{
	HashJoinState *hjstate;
	Plan	   *outerNode;
	Hash	   *hashNode;
	TupleDesc	outerDesc,
				innerDesc;
	const TupleTableSlotOps *ops;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	hjstate = makeNode(HashJoinState);
	hjstate->js.ps.plan = (Plan *) node;
	hjstate->js.ps.state = estate;

	/*
	 * See ExecHashJoinInitializeDSM() and ExecHashJoinInitializeWorker()
	 * where this function may be replaced with a parallel version, if we
	 * managed to launch a parallel query.
	 */
	hjstate->js.ps.ExecProcNode = ExecHashJoin;
	hjstate->js.jointype = node->join.jointype;

	hjstate->hashloop_fallback = false;
	hjstate->hj_InnerPageOffset = 0L;
	hjstate->hj_InnerFirstChunk = false;
	hjstate->hj_OuterCurrentByte = 0;

	hjstate->hj_OuterMatchStatusesFile = NULL;
	hjstate->hj_OuterTupleCount = 0;
	hjstate->hj_InnerExhausted = false;

	hjstate->last_worker = false;
	hjstate->combined_bitmap = NULL;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &hjstate->js.ps);

	/*
	 * initialize child nodes
	 *
	 * Note: we could suppress the REWIND flag for the inner input, which
	 * would amount to betting that the hash will be a single batch.  Not
	 * clear if this would be a win or not.
	 */
	outerNode = outerPlan(node);
	hashNode = (Hash *) innerPlan(node);

	outerPlanState(hjstate) = ExecInitNode(outerNode, estate, eflags);
	outerDesc = ExecGetResultType(outerPlanState(hjstate));
	innerPlanState(hjstate) = ExecInitNode((Plan *) hashNode, estate, eflags);
	innerDesc = ExecGetResultType(innerPlanState(hjstate));

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(&hjstate->js.ps, &TTSOpsVirtual);
	ExecAssignProjectionInfo(&hjstate->js.ps, NULL);

	/*
	 * tuple table initialization
	 */
	ops = ExecGetResultSlotOps(outerPlanState(hjstate), NULL);
	hjstate->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate, outerDesc,
														ops);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	hjstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
			break;
		case JOIN_RIGHT:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
			break;
		case JOIN_FULL:
			hjstate->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate, outerDesc, &TTSOpsVirtual);
			hjstate->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate, innerDesc, &TTSOpsVirtual);
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.  -cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(hjstate);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		hjstate->hj_HashTupleSlot = slot;
	}

	/*
	 * initialize child expressions
	 */
	hjstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) hjstate);
	hjstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) hjstate);
	hjstate->hashclauses =
		ExecInitQual(node->hashclauses, (PlanState *) hjstate);

	/*
	 * initialize hash-specific info
	 */
	hjstate->hj_HashTable = NULL;
	hjstate->hj_FirstOuterTupleSlot = NULL;

	hjstate->hj_CurHashValue = 0;
	hjstate->hj_CurBucketNo = 0;
	hjstate->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	hjstate->hj_CurTuple = NULL;

	hjstate->hj_OuterHashKeys = ExecInitExprList(node->hashkeys,
												 (PlanState *) hjstate);
	hjstate->hj_HashOperators = node->hashoperators;
	hjstate->hj_Collations = node->hashcollations;

	hjstate->hj_JoinState = HJ_BUILD_HASHTABLE;
	hjstate->hj_MatchedOuter = false;
	hjstate->hj_OuterNotEmpty = false;

	return hjstate;
}

/* ----------------------------------------------------------------
 *		ExecEndHashJoin
 *
 *		clean up routine for HashJoin node
 * ----------------------------------------------------------------
 */
void
ExecEndHashJoin(HashJoinState *node)
{
	/*
	 * Free hash table
	 */
	if (node->hj_HashTable)
	{
		ExecHashTableDestroy(node->hj_HashTable);
		node->hj_HashTable = NULL;
	}

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->hj_OuterTupleSlot);
	ExecClearTuple(node->hj_HashTupleSlot);

	/*
	 * clean up subtrees
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}


static TupleTableSlot *
emitUnmatchedOuterTuple(ExprState *otherqual, ExprContext *econtext, HashJoinState *hjstate)
{
	if (hjstate->hj_MatchedOuter)
		return NULL;

	if (!HJ_FILL_OUTER(hjstate))
		return NULL;

	econtext->ecxt_innertuple = hjstate->hj_NullInnerTupleSlot;

	/*
	 * Generate a fake join tuple with nulls for the inner tuple, and return
	 * it if it passes the non-join quals.
	 */

	if (otherqual == NULL || ExecQual(otherqual, econtext))
		return ExecProject(hjstate->js.ps.ps_ProjInfo);

	InstrCountFiltered2(hjstate, 1);
	return NULL;
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for a parallel oblivious hashjoin: either by
 *		executing the outer plan node in the first pass, or from the temp
 *		files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode,
						  HashJoinState *hjstate,
						  uint32 *hashvalue)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;

	if (curbatch == 0)			/* if it is the first pass */
	{
		/*
		 * Check to see if first outer tuple was already fetched by
		 * ExecHashJoin() and not used yet.
		 */
		slot = hjstate->hj_FirstOuterTupleSlot;
		if (!TupIsNull(slot))
			hjstate->hj_FirstOuterTupleSlot = NULL;
		else
			slot = ExecProcNode(outerNode);

		while (!TupIsNull(slot))
		{
			/*
			 * We have to compute the tuple's hash value.
			 */
			ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

			econtext->ecxt_outertuple = slot;
			if (ExecHashGetHashValue(hashtable, econtext,
									 hjstate->hj_OuterHashKeys,
									 true,	/* outer tuple */
									 HJ_FILL_OUTER(hjstate),
									 hashvalue))
			{
				/* remember outer relation is not empty for possible rescan */
				hjstate->hj_OuterNotEmpty = true;

				return slot;
			}

			/*
			 * That tuple couldn't match because of a NULL, so discard it and
			 * continue with the next one.
			 */
			slot = ExecProcNode(outerNode);
		}
	}
	else if (curbatch < hashtable->nbatch)
	{
		BufFile    *file = hashtable->outerBatchFile[curbatch];

		/*
		 * In outer-join cases, we could get here even though the batch file
		 * is empty.
		 */
		if (file == NULL)
			return NULL;

		slot = ExecHashJoinGetSavedTuple(hjstate,
										 file,
										 hashvalue,
										 hjstate->hj_OuterTupleSlot);
		if (!TupIsNull(slot))
			return slot;
	}

	/* End of this batch */
	return NULL;
}

/*
 * ExecHashJoinOuterGetTuple variant for the parallel case.
 */
static TupleTableSlot *
ExecParallelHashJoinOuterGetTuple(PlanState *outerNode,
								  HashJoinState *hjstate,
								  uint32 *hashvalue)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	TupleTableSlot *slot;

	/*
	 * In the Parallel Hash case we only run the outer plan directly for
	 * single-batch hash joins.  Otherwise we have to go to batch files, even
	 * for batch 0.
	 */
	if (curbatch == 0 && hashtable->nbatch == 1)
	{
		slot = ExecProcNode(outerNode);

		while (!TupIsNull(slot))
		{
			ExprContext *econtext = hjstate->js.ps.ps_ExprContext;

			econtext->ecxt_outertuple = slot;
			if (ExecHashGetHashValue(hashtable, econtext,
									 hjstate->hj_OuterHashKeys,
									 true,	/* outer tuple */
									 HJ_FILL_OUTER(hjstate),
									 hashvalue))
				return slot;

			/*
			 * That tuple couldn't match because of a NULL, so discard it and
			 * continue with the next one.
			 */
			slot = ExecProcNode(outerNode);
		}
	}
	else if (curbatch < hashtable->nbatch)
	{
		MinimalTuple tuple;

		tupleMetadata metadata;
		int			tupleid;

		tuple = sts_parallel_scan_next(hashtable->batches[curbatch].outer_tuples,
									   &metadata);
		if (tuple != NULL)
		{
			/* where is this hashvalue being used? */
			*hashvalue = metadata.hashvalue;
			tupleid = metadata.tupleid;
			ExecForceStoreMinimalTuple(tuple,
									   hjstate->hj_OuterTupleSlot,
									   false);
			hjstate->hj_OuterTupleSlot->tuplenum = tupleid;
			slot = hjstate->hj_OuterTupleSlot;
			return slot;
		}
		else
			ExecClearTuple(hjstate->hj_OuterTupleSlot);
	}

	/* End of this batch */
	return NULL;
}

/*
 * ExecHashJoinAdvanceBatch
 *		switch to a new hashjoin batch
 *
 * Returns true if successful, false if there are no more batches.
 */
static bool
ExecHashJoinAdvanceBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			nbatch;
	int			curbatch;

	nbatch = hashtable->nbatch;
	curbatch = hashtable->curbatch;

	if (curbatch > 0)
	{
		/*
		 * We no longer need the previous outer batch file; close it right
		 * away to free disk space.
		 */
		if (hashtable->outerBatchFile[curbatch])
			BufFileClose(hashtable->outerBatchFile[curbatch]);
		hashtable->outerBatchFile[curbatch] = NULL;
	}
	else						/* we just finished the first batch */
	{
		/*
		 * Reset some of the skew optimization state variables, since we no
		 * longer need to consider skew tuples after the first batch. The
		 * memory context reset we are about to do will release the skew
		 * hashtable itself.
		 */
		hashtable->skewEnabled = false;
		hashtable->skewBucket = NULL;
		hashtable->skewBucketNums = NULL;
		hashtable->nSkewBuckets = 0;
		hashtable->spaceUsedSkew = 0;
	}

	/*
	 * We can always skip over any batches that are completely empty on both
	 * sides.  We can sometimes skip over batches that are empty on only one
	 * side, but there are exceptions:
	 *
	 * 1. In a left/full outer join, we have to process outer batches even if
	 * the inner batch is empty.  Similarly, in a right/full outer join, we
	 * have to process inner batches even if the outer batch is empty.
	 *
	 * 2. If we have increased nbatch since the initial estimate, we have to
	 * scan inner batches since they might contain tuples that need to be
	 * reassigned to later inner batches.
	 *
	 * 3. Similarly, if we have increased nbatch since starting the outer
	 * scan, we have to rescan outer batches in case they contain tuples that
	 * need to be reassigned.
	 */
	curbatch++;
	while (curbatch < nbatch &&
		   (hashtable->outerBatchFile[curbatch] == NULL ||
			hashtable->innerBatchFile[curbatch] == NULL))
	{
		if (hashtable->outerBatchFile[curbatch] &&
			HJ_FILL_OUTER(hjstate))
			break;				/* must process due to rule 1 */
		if (hashtable->innerBatchFile[curbatch] &&
			HJ_FILL_INNER(hjstate))
			break;				/* must process due to rule 1 */
		if (hashtable->innerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_original)
			break;				/* must process due to rule 2 */
		if (hashtable->outerBatchFile[curbatch] &&
			nbatch != hashtable->nbatch_outstart)
			break;				/* must process due to rule 3 */
		/* We can ignore this batch. */
		/* Release associated temp files right away. */
		if (hashtable->innerBatchFile[curbatch])
			BufFileClose(hashtable->innerBatchFile[curbatch]);
		hashtable->innerBatchFile[curbatch] = NULL;
		if (hashtable->outerBatchFile[curbatch])
			BufFileClose(hashtable->outerBatchFile[curbatch]);
		hashtable->outerBatchFile[curbatch] = NULL;
		curbatch++;
	}

	hjstate->hj_InnerPageOffset = 0L;
	hjstate->hj_InnerFirstChunk = true;
	hjstate->hashloop_fallback = false; /* new batch, so start it off false */
	if (hjstate->hj_OuterMatchStatusesFile != NULL)
		BufFileClose(hjstate->hj_OuterMatchStatusesFile);
	hjstate->hj_OuterMatchStatusesFile = NULL;
	if (curbatch >= nbatch)
		return false;			/* no more batches */

	hashtable->curbatch = curbatch;
	return true;
}

/*
 * Returns true if there are more chunks left, false otherwise
 */
static bool
ExecHashJoinLoadInnerBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int			curbatch = hashtable->curbatch;
	BufFile    *innerFile;
	TupleTableSlot *slot;
	uint32		hashvalue;

	off_t		tup_start_offset;
	off_t		chunk_start_offset;
	off_t		tup_end_offset;
	int64		current_saved_size;
	int			current_fileno;

	/*
	 * Reload the hash table with the new inner batch (which could be empty)
	 */
	ExecHashTableReset(hashtable);

	innerFile = hashtable->innerBatchFile[curbatch];

	/* Reset this even if the innerfile is not null */
	hjstate->hj_InnerFirstChunk = hjstate->hj_InnerPageOffset == 0L;

	if (innerFile != NULL)
	{
		/* TODO: should fileno always be 0? */
		if (BufFileSeek(innerFile, 0, hjstate->hj_InnerPageOffset, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not rewind hash-join temporary file: %m")));

		chunk_start_offset = hjstate->hj_InnerPageOffset;
		tup_end_offset = hjstate->hj_InnerPageOffset;
		while ((slot = ExecHashJoinGetSavedTuple(hjstate,
												 innerFile,
												 &hashvalue,
												 hjstate->hj_HashTupleSlot)))
		{
			/* next tuple's start is last tuple's end */
			tup_start_offset = tup_end_offset;
			/* after we got the tuple, figure out what the offset is */
			BufFileTell(innerFile, &current_fileno, &tup_end_offset);
			current_saved_size = tup_end_offset - chunk_start_offset;
			if (current_saved_size > work_mem)
			{
				hjstate->hj_InnerPageOffset = tup_start_offset;
				hjstate->hashloop_fallback = true;
				return true;
			}
			hjstate->hj_InnerPageOffset = tup_end_offset;

			/*
			 * NOTE: some tuples may be sent to future batches. With current
			 * hashloop patch, however, it is not possible for
			 * hashtable->nbatch to be increased here
			 */
			ExecHashTableInsert(hashtable, slot, hashvalue);
		}

		/* this is the end of the file */
		hjstate->hj_InnerPageOffset = 0L;

		/*
		 * after we processed all chunks, the inner batch file is no longer
		 * needed
		 */
		BufFileClose(innerFile);
		hashtable->innerBatchFile[curbatch] = NULL;
	}

	return false;
}


/*
 * ExecHashJoinSaveTuple
 *		save a tuple to a batch file.
 *
 * The data recorded in the file for each tuple is its hash value,
 * then the tuple in MinimalTuple format.
 *
 * Note: it is important always to call this in the regular executor
 * context, not in a shorter-lived context; else the temp file buffers
 * will get messed up.
 */
void
ExecHashJoinSaveTuple(MinimalTuple tuple, uint32 hashvalue,
					  BufFile **fileptr)
{
	BufFile    *file = *fileptr;
	size_t		written;

	if (file == NULL)
	{
		/* First write to this batch file, so open it. */
		file = BufFileCreateTemp(false);
		*fileptr = file;
	}

	written = BufFileWrite(file, (void *) &hashvalue, sizeof(uint32));
	if (written != sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));

	written = BufFileWrite(file, (void *) tuple, tuple->t_len);
	if (written != tuple->t_len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to hash-join temporary file: %m")));
}

/*
 * ExecHashJoinGetSavedTuple
 *		read the next tuple from a batch file.  Return NULL if no more.
 *
 * On success, *hashvalue is set to the tuple's hash value, and the tuple
 * itself is stored in the given slot.
 */
static TupleTableSlot *
ExecHashJoinGetSavedTuple(HashJoinState *hjstate,
						  BufFile *file,
						  uint32 *hashvalue,
						  TupleTableSlot *tupleSlot)
{
	uint32		header[2];
	size_t		nread;
	MinimalTuple tuple;

	/*
	 * We check for interrupts here because this is typically taken as an
	 * alternative code path to an ExecProcNode() call, which would include
	 * such a check.
	 */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Since both the hash value and the MinimalTuple length word are uint32,
	 * we can read them both in one BufFileRead() call without any type
	 * cheating.
	 */
	nread = BufFileRead(file, (void *) header, sizeof(header));
	if (nread == 0)				/* end of file */
	{
		ExecClearTuple(tupleSlot);
		return NULL;
	}
	if (nread != sizeof(header))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	*hashvalue = header[0];
	tuple = (MinimalTuple) palloc(header[1]);
	tuple->t_len = header[1];
	nread = BufFileRead(file,
						(void *) ((char *) tuple + sizeof(uint32)),
						header[1] - sizeof(uint32));
	if (nread != header[1] - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from hash-join temporary file: %m")));
	ExecForceStoreMinimalTuple(tuple, tupleSlot, true);
	return tupleSlot;
}


void
ExecReScanHashJoin(HashJoinState *node)
{
	/*
	 * In a multi-batch join, we currently have to do rescans the hard way,
	 * primarily because batch temp files may have already been released. But
	 * if it's a single-batch join, and there is no parameter change for the
	 * inner subnode, then we can just re-use the existing hash table without
	 * rebuilding it.
	 */
	if (node->hj_HashTable != NULL)
	{
		if (node->hj_HashTable->nbatch == 1 &&
			node->js.ps.righttree->chgParam == NULL)
		{
			/*
			 * Okay to reuse the hash table; needn't rescan inner, either.
			 *
			 * However, if it's a right/full join, we'd better reset the
			 * inner-tuple match flags contained in the table.
			 */
			if (HJ_FILL_INNER(node))
				ExecHashTableResetMatchFlags(node->hj_HashTable);

			/*
			 * Also, we need to reset our state about the emptiness of the
			 * outer relation, so that the new scan of the outer will update
			 * it correctly if it turns out to be empty this time. (There's no
			 * harm in clearing it now because ExecHashJoin won't need the
			 * info.  In the other cases, where the hash table doesn't exist
			 * or we are destroying it, we leave this state alone because
			 * ExecHashJoin will need it the first time through.)
			 */
			node->hj_OuterNotEmpty = false;

			/* ExecHashJoin can skip the BUILD_HASHTABLE step */
			node->hj_JoinState = HJ_NEED_NEW_OUTER;
		}
		else
		{
			/* must destroy and rebuild hash table */
			ExecHashTableDestroy(node->hj_HashTable);
			node->hj_HashTable = NULL;
			node->hj_JoinState = HJ_BUILD_HASHTABLE;

			/*
			 * if chgParam of subnode is not null then plan will be re-scanned
			 * by first ExecProcNode.
			 */
			if (node->js.ps.righttree->chgParam == NULL)
				ExecReScan(node->js.ps.righttree);
		}
	}

	/* Always reset intra-tuple state */
	node->hj_CurHashValue = 0;
	node->hj_CurBucketNo = 0;
	node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	node->hj_CurTuple = NULL;

	node->hj_MatchedOuter = false;
	node->hj_FirstOuterTupleSlot = NULL;

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (node->js.ps.lefttree->chgParam == NULL)
		ExecReScan(node->js.ps.lefttree);
}

void
ExecShutdownHashJoin(HashJoinState *node)
{
	if (node->hj_HashTable)
	{
		/*
		 * Detach from shared state before DSM memory goes away.  This makes
		 * sure that we don't have any pointers into DSM memory by the time
		 * ExecEndHashJoin runs.
		 */
		ExecHashTableDetachBatch(node->hj_HashTable);
		ExecHashTableDetach(node->hj_HashTable);
	}
}

static void
ExecParallelHashJoinPartitionOuter(HashJoinState *hjstate)
{
	PlanState  *outerState = outerPlanState(hjstate);
	ExprContext *econtext = hjstate->js.ps.ps_ExprContext;
	HashJoinTable hashtable = hjstate->hj_HashTable;
	TupleTableSlot *slot;
	uint32		hashvalue;
	int			i;

	Assert(hjstate->hj_FirstOuterTupleSlot == NULL);

	/* Execute outer plan, writing all tuples to shared tuplestores. */
	for (;;)
	{
		tupleMetadata metadata;

		slot = ExecProcNode(outerState);
		if (TupIsNull(slot))
			break;
		econtext->ecxt_outertuple = slot;
		if (ExecHashGetHashValue(hashtable, econtext,
								 hjstate->hj_OuterHashKeys,
								 true,	/* outer tuple */
								 HJ_FILL_OUTER(hjstate),
								 &hashvalue))
		{
			int			batchno;
			int			bucketno;
			bool		shouldFree;
			MinimalTuple mintup = ExecFetchSlotMinimalTuple(slot, &shouldFree);

			ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno,
									  &batchno);
			metadata.hashvalue = hashvalue;
			SharedTuplestoreAccessor *accessor = hashtable->batches[batchno].outer_tuples;

			metadata.tupleid = sts_increment_tuplenum(accessor);
			sts_puttuple(accessor, &metadata, mintup);

			if (shouldFree)
				heap_free_minimal_tuple(mintup);
		}
		CHECK_FOR_INTERRUPTS();
	}

	/* Make sure all outer partitions are readable by any backend. */
	for (i = 0; i < hashtable->nbatch; ++i)
		sts_end_write(hashtable->batches[i].outer_tuples);
}

void
ExecHashJoinEstimate(HashJoinState *state, ParallelContext *pcxt)
{
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelHashJoinState));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

void
ExecHashJoinInitializeDSM(HashJoinState *state, ParallelContext *pcxt)
{
	int			plan_node_id = state->js.ps.plan->plan_node_id;
	HashState  *hashNode;
	ParallelHashJoinState *pstate;

	/*
	 * Disable shared hash table mode if we failed to create a real DSM
	 * segment, because that means that we don't have a DSA area to work with.
	 */
	if (pcxt->seg == NULL)
		return;

	ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);

	/*
	 * Set up the state needed to coordinate access to the shared hash
	 * table(s), using the plan node ID as the toc key.
	 */
	pstate = shm_toc_allocate(pcxt->toc, sizeof(ParallelHashJoinState));
	shm_toc_insert(pcxt->toc, plan_node_id, pstate);

	/*
	 * Set up the shared hash join state with no batches initially.
	 * ExecHashTableCreate() will prepare at least one later and set nbatch
	 * and space_allowed.
	 */
	pstate->nbatch = 0;
	pstate->batch_increases = 0;
	pstate->space_allowed = 0;
	pstate->batches = InvalidDsaPointer;
	pstate->old_batches = InvalidDsaPointer;
	pstate->nbuckets = 0;
	pstate->growth = PHJ_GROWTH_OK;
	pstate->chunk_work_queue = InvalidDsaPointer;
	pg_atomic_init_u32(&pstate->distributor, 0);
	pstate->nparticipants = pcxt->nworkers + 1;
	pstate->total_tuples = 0;
	LWLockInitialize(&pstate->lock,
					 LWTRANCHE_PARALLEL_HASH_JOIN);
	BarrierInit(&pstate->build_barrier, 0);
	BarrierInit(&pstate->grow_batches_barrier, 0);
	BarrierInit(&pstate->grow_buckets_barrier, 0);

	/* Set up the space we'll use for shared temporary files. */
	SharedFileSetInit(&pstate->fileset, pcxt->seg);

	/* Initialize the shared state in the hash node. */
	hashNode = (HashState *) innerPlanState(state);
	hashNode->parallel_state = pstate;
}

/* ----------------------------------------------------------------
 *		ExecHashJoinReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecHashJoinReInitializeDSM(HashJoinState *state, ParallelContext *cxt)
{
	int			plan_node_id = state->js.ps.plan->plan_node_id;
	ParallelHashJoinState *pstate =
	shm_toc_lookup(cxt->toc, plan_node_id, false);

	/*
	 * It would be possible to reuse the shared hash table in single-batch
	 * cases by resetting and then fast-forwarding build_barrier to
	 * PHJ_BUILD_DONE and batch 0's batch_barrier to PHJ_BATCH_CHUNKING, but
	 * currently shared hash tables are already freed by now (by the last
	 * participant to detach from the batch).  We could consider keeping it
	 * around for single-batch joins.  We'd also need to adjust
	 * finalize_plan() so that it doesn't record a dummy dependency for
	 * Parallel Hash nodes, preventing the rescan optimization.  For now we
	 * don't try.
	 */

	/* Detach, freeing any remaining shared memory. */
	if (state->hj_HashTable != NULL)
	{
		ExecHashTableDetachBatch(state->hj_HashTable);
		ExecHashTableDetach(state->hj_HashTable);
	}

	/* Clear any shared batch files. */
	SharedFileSetDeleteAll(&pstate->fileset);

	/* Reset build_barrier to PHJ_BUILD_ELECTING so we can go around again. */
	BarrierInit(&pstate->build_barrier, 0);
}

void
ExecHashJoinInitializeWorker(HashJoinState *state,
							 ParallelWorkerContext *pwcxt)
{
	HashState  *hashNode;
	int			plan_node_id = state->js.ps.plan->plan_node_id;
	ParallelHashJoinState *pstate =
	shm_toc_lookup(pwcxt->toc, plan_node_id, false);

	/* Attach to the space for shared temporary files. */
	SharedFileSetAttach(&pstate->fileset, pwcxt->seg);

	/* Attach to the shared state in the hash node. */
	hashNode = (HashState *) innerPlanState(state);
	hashNode->parallel_state = pstate;

	ExecSetExecProcNode(&state->js.ps, ExecParallelHashJoin);
}
