/*-------------------------------------------------------------------------
 *
 * nodeHashjoin.c
 *	  Routines to handle hash join nodes
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
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
 *  PHJ_BATCH_PROBING        -- all probe
 *  PHJ_BATCH_DONE           -- end
 *
 * Batch 0 is a special case, because it starts out in phase
 * PHJ_BATCH_PROBING; populating batch 0's hash table is done during
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
 * tuples while in PHJ_BATCH_PROBING phase, but that's OK because we use
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
static bool ExecParallelCheckHashloopFallback(HashJoinState *hjstate, int batchno);
static bool ExecParallelHashJoinNewBatch(HashJoinState *hjstate);
static void ExecParallelHashJoinPartitionOuter(HashJoinState *node);

static TupleTableSlot *emitUnmatchedOuterTuple(ExprState *otherqual,
											   ExprContext *econtext,
											   HashJoinState *hjstate);
static void combine_outer_match_statuses(BufFile *outer_match_statuses[], int length, size_t num_bytes,
		int batchno, BufFile **combined_bitmap_file);

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
static pg_attribute_always_inline TupleTableSlot *
ExecHashJoinImpl(PlanState *pstate, bool parallel)
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
	parallel_state = hashNode->parallel_state;

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
		bool outerTupleMatchesExhausted = false;

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

				elog(DEBUG3, "HJ_BUILD_HASHTABLE");
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
				//volatile int mybp = 0; while (mybp == 0);
				if (HJ_FILL_INNER(node))
				{
					/* no chance to not build the hash table */
					node->hj_FirstOuterTupleSlot = NULL;
				}
				else if (parallel)
				{
					/*
					 * The empty-outer optimization is not implemented for
					 * shared hash tables, because no one participant can
					 * determine that there are no outer tuples, and it's not
					 * yet clear that it's worth the synchronization overhead
					 * of reaching consensus to figure that out.  So we have
					 * to build the hash table.
					 */
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

				/*
				 * Create the hash table.  If using Parallel Hash, then
				 * whoever gets here first will create the hash table and any
				 * later arrivals will merely attach to it.
				 */
				hashtable = ExecHashTableCreate(hashNode,
												node->hj_HashOperators,
												node->hj_Collations,
												HJ_FILL_INNER(node));
				node->hj_HashTable = hashtable;

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

				if (parallel)
				{
					Barrier    *build_barrier;

					build_barrier = &parallel_state->build_barrier;
					Assert(BarrierPhase(build_barrier) == PHJ_BUILD_HASHING_OUTER ||
						   BarrierPhase(build_barrier) == PHJ_BUILD_DONE);
					if (BarrierPhase(build_barrier) == PHJ_BUILD_HASHING_OUTER)
					{
						/*
						 * If multi-batch, we need to hash the outer relation
						 * up front.
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
				}
				else
					node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/* FALL THRU */

			case HJ_NEED_NEW_OUTER:

				elog(DEBUG3, "HJ_NEED_NEW_OUTER");
				/*
				 * We don't have an outer tuple, try to get the next one
				 */
				if (parallel)
					outerTupleSlot =
						ExecParallelHashJoinOuterGetTuple(outerNode, node,
														  &hashvalue);
				else
					outerTupleSlot =
						ExecHashJoinOuterGetTuple(outerNode, node, &hashvalue);

				if (TupIsNull(outerTupleSlot))
				{
					/*
					 * end of batch, or maybe whole join.
					 * for hashloop fallback, all we know is outer batch is
					 * exhausted. inner could have more chunks
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
				/*
				 * for the hashloop fallback case,
				 * only initialize hj_MatchedOuter to false during the first chunk.
				 * otherwise, we will be resetting hj_MatchedOuter to false for
				 * an outer tuple that has already matched an inner tuple.
				 * also, hj_MatchedOuter should be set to false for batch 0.
				 * there are no chunks for batch 0, and node->hj_InnerFirstChunk isn't
				 * set to true until HJ_NEED_NEW_BATCH,
				 * so need to handle batch 0 explicitly
				 */
				// TODO: handle parallel (once chunking is implemented)

				if (parallel)
				{
					ParallelHashJoinBatch *phj_batch = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared;
					if (phj_batch->parallel_hashloop_fallback == false || phj_batch->current_chunk_num == 1)
						node->hj_MatchedOuter = false;
				}
				else
				{
					if (node->hashloop_fallback == false || node->hj_InnerFirstChunk || hashtable->curbatch == 0)
						node->hj_MatchedOuter = false;
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

				if (batchno == 20)
					elog(DEBUG3, "HJ_NEED_NEW_OUTER. batch 20");
				node->hj_CurTuple = NULL;
				// TODO: this is doing it for every outer tuple -- make it only do it for new batches
				if (parallel)
					node->parallel_hashloop_fallback = ExecParallelCheckHashloopFallback(node, batchno);
				else
					node->parallel_hashloop_fallback = false;

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
					Assert(parallel_state == NULL);
					Assert(batchno > hashtable->curbatch);
					ExecHashJoinSaveTuple(mintuple, hashvalue,
										  &hashtable->outerBatchFile[batchno]);

					if (shouldFree)
						heap_free_minimal_tuple(mintuple);

					/* Loop around, staying in HJ_NEED_NEW_OUTER state */
					continue;
				}

				if (hashtable->outerBatchFile == NULL)
				{
					node->hj_JoinState = HJ_SCAN_BUCKET;
					break;
				}

				// TODO: why is both this and the thing above it here??
				BufFile *outerFile = hashtable->outerBatchFile[batchno];
				if (outerFile == NULL)
				{
					node->hj_JoinState = HJ_SCAN_BUCKET;
					break;
				}

				// TODO: make this clear this not for parallel fallback
				if (node->hashloop_fallback == true)
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
						 * enough bytes in the outer tuple match status file to
						 * capture all tuples' match statuses
						 */
						// TODO: wait, isn't this just writing one byte? this won't be enough for all outer tuples' match statuses
						if (node->hj_InnerFirstChunk)
						{
							node->hj_OuterCurrentByte = 0;
							BufFileWrite(node->hj_OuterMatchStatusesFile, &node->hj_OuterCurrentByte, 1);
						}
						/* otherwise, just read the next byte */
						else
							BufFileRead(node->hj_OuterMatchStatusesFile, &node->hj_OuterCurrentByte, 1);
					}

					elog(DEBUG3,
						 "in HJ_NEED_NEW_OUTER. batchno %i. val %i. read  byte %hhu. cur tup %li.",
						 batchno,
						 DatumGetInt32(outerTupleSlot->tts_values[0]),
						 node->hj_OuterCurrentByte,
						 node->hj_OuterTupleCount);
				}

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

			case HJ_SCAN_BUCKET:

				elog(DEBUG3, "HJ_SCAN_BUCKET");
				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (node->hj_HashTable->curbatch == 20)
					elog(DEBUG3, "begininng of HJ_SCAN_BUCKET for batch 20");
				if (parallel)
					outerTupleMatchesExhausted = !ExecParallelScanHashBucket(node, econtext);
				else
					outerTupleMatchesExhausted = !ExecScanHashBucket(node, econtext);

				if (outerTupleMatchesExhausted)
				{
					/*
					 * The current outer tuple has run out of matches, so check
					 * whether to emit a dummy outer-join tuple.  Whether we emit
					 * one or not, the next state is NEED_NEW_OUTER.
					 */
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					if (parallel)
					{
						//if (node->parallel_hashloop_fallback == false)
						// does it have to do with batch 0?
						//if (node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared->parallel_hashloop_fallback == false)
						if (node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared->parallel_hashloop_fallback == false || node->hj_HashTable->curbatch == 0)
						{
							// tells us for a batch that we intend to emit unmatched tuples as we go
							// valuable because it will still log even if there are no unmatched outer tuples -- just to make sure we are
							// doing the right thing for a given batch
							// PAHJ_debugging
							elog(DEBUG3, "in HJ_SCAN_BUCKET and parallel_hashloop_fallback is %i. batchno %i. emitting as we go.",
								 node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared->parallel_hashloop_fallback, hashtable->curbatch);
							// LOG_NOW
							TupleTableSlot *slot = emitUnmatchedOuterTuple(otherqual, econtext, node);
							if (slot != NULL)
								return slot;
						}

					}
					else if (node->hj_HashTable->curbatch == 0 || node->hashloop_fallback == false)
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
				if (joinqual == NULL || ExecQual(joinqual, econtext))
				{
					node->hj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

					// TODO: how does this interact with PAHJ -- do I need to set matchbit?
					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}

					/*
					 * If we only need to join to the first matching inner
					 * tuple, then consider returning this one, but after that
					 * continue with next outer tuple.
					 */
					if (node->js.single_match)
						node->hj_JoinState = HJ_NEED_NEW_OUTER;

					/*
					 * Set the match bit for this outer tuple in the match
					 * status file
					 */
					// TODO: for parallel, do I still need to do this for batch 0
					//if (parallel && batchno > 0)
					bool fallback = false;
					if (parallel)
						fallback = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared->parallel_hashloop_fallback;
					if (parallel && fallback == true)
					{
						// TODO: make this only when it is fallback case
						unsigned char current_outer_byte;
						// TODO: is this parallel-safe
						// TODO: should I instead get it from node->hj_OuterTupleSlot->tuplenum or add something else altogether?
						uint32 tupleid = econtext->ecxt_outertuple->tuplenum;
						SharedTuplestoreAccessor *outer_acc = hashtable->batches[hashtable->curbatch].outer_tuples;
						BufFile *parallel_outer_matchstatuses = sts_get_my_STA_outerMatchStatuses(outer_acc); // TODO: make sure I always want to get my own file here

						// TODO: make sure this logic is right for tuplenum -- ugh seems like no +1 ?
						if (BufFileSeek(parallel_outer_matchstatuses, 0, (tupleid / 8), SEEK_SET) != 0)
							elog(DEBUG1, "HJ_SCAN_BUCKET for batchno %i. at beginning of file. pid %i.", batchno, MyProcPid);
						BufFileRead(parallel_outer_matchstatuses, &current_outer_byte, 1);

						// I think I don't need to subtract 1 for parallel case because tuplenums are not off by one?
						int bit_to_set_in_byte = tupleid % 8;

						current_outer_byte = current_outer_byte | (1 << bit_to_set_in_byte);

						uint32 final_tuplenum = sts_gettuplenum(outer_acc);
						elog(DEBUG1,
							 "tupleval %i. HJ_SCAN_BUCKET. batchno o%iof. final_tuplenum %i. write_byteval %hhu. bytenum %i. tupleid %i. bitnum %i. pid %i.",
							 DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]),
							 hashtable->curbatch,
							 final_tuplenum,
							 current_outer_byte,
							 tupleid / 8,
							 tupleid,
							 bit_to_set_in_byte,
							 MyProcPid);

						elog(DEBUG1,
								"HJ_SCAN_BUCKET. batchno o%iof. final_tuplenum %i. tupleid %i. tupleval %i. match_status 1. bytenum %i. write_byteval %hhu. bitnum %i. pid %i.",
								hashtable->curbatch,
								final_tuplenum,
								tupleid,
								DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]),
								tupleid / 8,
								current_outer_byte,
								bit_to_set_in_byte,
								MyProcPid);

						if (BufFileSeek(parallel_outer_matchstatuses, 0, -1, SEEK_CUR) != 0)
							elog(ERROR, "there is a problem with outer match status file. pid %i.", MyProcPid);

						BufFileWrite(parallel_outer_matchstatuses, &current_outer_byte, 1);
					}

					// TODO: make it clear this is just for serial fallback case
					else if (node->hj_OuterMatchStatusesFile != NULL)
					{
						Assert(node->hashloop_fallback == true);
						int byte_to_set = (node->hj_OuterTupleCount - 1) / 8;
						int bit_to_set_in_byte = (node->hj_OuterTupleCount - 1) % 8;

						if (BufFileSeek(node->hj_OuterMatchStatusesFile, 0, byte_to_set, SEEK_SET) != 0)
							elog(DEBUG1, "at beginning of file");

						node->hj_OuterCurrentByte = node->hj_OuterCurrentByte | (1 << bit_to_set_in_byte);

						elog(DEBUG3,
								"in HJ_SCAN_BUCKET.    batchno %i. val %i. write byte %hhu. cur tup %li. bitnum %i. bytenum %i.",
								node->hj_HashTable->curbatch,
								DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]),
								node->hj_OuterCurrentByte,
								node->hj_OuterTupleCount,
								bit_to_set_in_byte,
								byte_to_set);

						BufFileWrite(node->hj_OuterMatchStatusesFile, &node->hj_OuterCurrentByte, 1);
					}
					if (otherqual == NULL || ExecQual(otherqual, econtext))
					{
						// LOG_NOW inner and outer tuples here
//							if (DatumGetInt32(econtext->ecxt_innertuple->tts_values[0]) != 2)
//								elog(NOTICE, "match o.%i.i.%i.",
//									DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]),
//									DatumGetInt32(econtext->ecxt_innertuple->tts_values[0]));
						if (DatumGetInt32(econtext->ecxt_innertuple->tts_values[0]) == 2 || DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]) == 2)
						{
							node->local_matched_tuple_count++;
						}
						return ExecProject(node->js.ps.ps_ProjInfo);
					}
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_FILL_INNER_TUPLES:

				elog(DEBUG3, "HJ_FILL_INNER_TUPLES");
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
				{
					// LOG_NOW inner and outer tuples here
					elog(NOTICE, "nomatch o.NULL.i.%i.",
						 DatumGetInt32(econtext->ecxt_innertuple->tts_values[0]));
					return ExecProject(node->js.ps.ps_ProjInfo);
				}
				else
					InstrCountFiltered2(node, 1);
				break;

			case HJ_NEED_NEW_BATCH:

				elog(DEBUG3, "HJ_NEED_NEW_BATCH");
				/*
				 * Try to advance to next batch.  Done if there are no more.
				 */
				if (parallel)
				{
					// VIP TODO: need to find a way to be able to check the parallel_hashloop_fallback status that is on the actual
					// batch when we try to advance to the next batch, we need to know if the batch we come out as having selected
					// should be processed fallback style -- not sure I can do it out here
					// the one in the node will not be correct here since workers can do batches as they please
					if (!ExecParallelHashJoinNewBatch(node))
					{
						elog(NOTICE, "%i.%li.", MyProcPid, node->local_matched_tuple_count);
						return NULL;	/* end of parallel-aware join */
					}
					bool fallback = false;
					if (parallel)
						fallback = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared->parallel_hashloop_fallback;
					elog(DEBUG3, "in HJ_NEED_NEW_BATCH. batchno %i. batch->parallel_hashloop_fallback is %i. node->last_worker is %i. pid %i.",
							hashtable->curbatch, fallback, node->last_worker, MyProcPid);
					// TODO: does this need to be parallel-safe?

					bool inner_exhausted = node->hj_InnerExhausted;
					if (node->last_worker == true && HJ_FILL_OUTER(node) && fallback == true && inner_exhausted == true)
					{
						node->last_worker = false;
						node->parallel_hashloop_fallback = false;
						node->hj_JoinState = HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT;
						break;
					}
				}
				else
				{
					/*
					 * for batches after batch 0 for which hashloop_fallback is
					 * true, if inner is exhausted, need to consider emitting
					 * unmatched tuples we should never get here when
					 * hashloop_fallback is false but hj_InnerExhausted is true,
					 * however, it felt more clear to check for
					 * hashloop_fallback explicitly
					 */
					if (node->hashloop_fallback == true && HJ_FILL_OUTER(node) && node->hj_InnerExhausted == true)
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
						return NULL;    /* end of parallel-oblivious join */

					// TODO: need to find a better way to distinguish if I should load inner batch again than checking for outer batch file
					// I need to also do this even if it is NULL when it is a ROJ
					// need to load inner again if it is an inner or left outer join and there are outer tuples in the batch OR
					// if it is a ROJ and there are inner tuples in the batch -- should never have no tuples in either batch...
					if (BufFileRewindIfExists(node->hj_HashTable->outerBatchFile[node->hj_HashTable->curbatch]) != NULL ||
							(node->hj_HashTable->innerBatchFile[node->hj_HashTable->curbatch] != NULL && HJ_FILL_INNER(node)))
					{
						ExecHashJoinLoadInnerBatch(node); /* TODO: should I ever load inner when outer file is not present? */
					}
					else
					{
						if (node->hj_HashTable->curbatch == 21)
							elog(WARNING, "HJ_NEED_NEW_BATCH. skipping loading batch 21. HJ_FILL_INNER is %i.", HJ_FILL_INNER(node));
					}

				}
				node->hj_JoinState = HJ_NEED_NEW_OUTER;
				break;

			case HJ_NEED_NEW_INNER_CHUNK:

				elog(DEBUG3, "HJ_NEED_NEW_INNER_CHUNK");

				// TODO: this probably isn't the right way to hack in parallel here, should probably change state machine
				if (parallel)
				{
					// TODO: this lock is way too aggressive -- need to find a better way
					LWLockAcquire(&hashtable->parallel_state->lock, LW_EXCLUSIVE);
					bool fallback = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared->parallel_hashloop_fallback;
					// FAVE_LOG
					if (fallback == true)
						elog(DEBUG3, "HJ_NEED_NEW_INNER_CHUNK. fallback is true. batchno %i.", node->hj_HashTable->curbatch);
					ParallelHashJoinBatch *phj_batch = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared;

					// tells us the number of chunks for each batch and if fallback is true
					// lets us see when we might have a batch with multiple chunks but fallback is false or vice versa
					elog(DEBUG3, "batch %i. current_chunk_num %i. total_num_chunks %i. fallback is %i.", node->hj_HashTable->curbatch, phj_batch->current_chunk_num, phj_batch->total_num_chunks, fallback);
					if (phj_batch->current_chunk_num == phj_batch->total_num_chunks)
					{
						// if we came to need new chunk and we are on the last chunk, advance the batches
						// TODO: do I want this backend local variable for this?
						node->hj_InnerExhausted = true;
						node->hj_JoinState = HJ_NEED_NEW_BATCH;
						LWLockRelease(&hashtable->parallel_state->lock);
						break;
					}
					// otherwise, we have more inner chunks
					else
					{
						elog(NOTICE, "batch %i. current_chunk_num %i. total_num_chunks %i. fallback is %i. pid %i.", node->hj_HashTable->curbatch, phj_batch->current_chunk_num, phj_batch->total_num_chunks, fallback, MyProcPid);
						node->hj_JoinState = HJ_NEED_NEW_OUTER;

						phj_batch->current_chunk_num++;

						// rewind/reset outer tuplestore and rewind outer match status files
						SharedTuplestoreAccessor *outer_tuples = node->hj_HashTable->batches[node->hj_HashTable->curbatch].outer_tuples;
						sts_reinitialize(outer_tuples);
						// maybe I need to do sts_end_write() and or something manual to make it so I can rewind the outer tuples tuplestore and also keep the bitmap
						// or maybe just do sts_end_parallel_scan(outer_tuples);
						// todo: make a version of sts_begin_parallel_scan() that doesn't null out the bitmap file
						sts_begin_parallel_scan(outer_tuples);

						// reset hashtable
						ExecHashTableReset(node->hj_HashTable);
						// load the next chunk of inner tuples into the hashtable
						// TODO: technically more than one worker should be able to do this
						LWLockRelease(&hashtable->parallel_state->lock);
						MinimalTuple tuple;
						TupleTableSlot *slot;
						SharedTuplestoreAccessor *inner_tuples = node->hj_HashTable->batches[node->hj_HashTable->curbatch].inner_tuples;
						sts_reinitialize(inner_tuples);
						sts_begin_parallel_scan(inner_tuples);
						tupleMetadata metadata;
						int chunk_num = 0;
						while ((tuple = sts_parallel_scan_next(inner_tuples,
															   &metadata, false)))
						{
							hashvalue = metadata.hashvalue;
							chunk_num = metadata.tuplenum;
							ExecForceStoreMinimalTuple(tuple,
													   node->hj_HashTupleSlot,
													   false);
							slot = node->hj_HashTupleSlot;

							if (chunk_num == phj_batch->current_chunk_num)
								ExecParallelHashTableInsertCurrentBatch(node->hj_HashTable, slot,
																		hashvalue, chunk_num);
						}
						sts_end_parallel_scan(inner_tuples);
						break;
					}
				}
// serial case (parallel case should never come here)
				/*
				 * there were never chunks because this is the normal case (not
				 * hashloop fallback) or this is batch 0. batch 0 cannot have
				 * chunks. hashloop_fallback should always be false when
				 * curbatch is 0 here. proceed to HJ_NEED_NEW_BATCH to either
				 * advance to the next batch or complete the join
				 */
				if (node->hj_HashTable->curbatch == 0) // don't do this check for parallel
				{
					Assert(node->hashloop_fallback == false);
					if(node->hj_InnerPageOffset != 0L)
						elog(DEBUG1, "hj_InnerPageOffset is not reset to 0 on batch 0");
				}

				if (node->hashloop_fallback == false)
				{
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
					break;
				}

				/*
				 * it is the hashloop fallback case and there are no more chunks
				 * inner is exhausted, so we must advance the batches
				 */
				if (node->hj_InnerPageOffset == 0L)
				{
					node->hj_InnerExhausted = true;
					node->hj_JoinState = HJ_NEED_NEW_BATCH;
					break;
				}

				/*
				 * This is the hashloop fallback case and we have more chunks in
				 * inner. curbatch > 0. Rewind outer batch file (if present) so
				 * that we can start reading it. Rewind outer match statuses
				 * file if present so that we can set match bits as needed Reset
				 * the tuple count and load the next chunk of inner. Then
				 * proceed to get a new outer tuple from our rewound outer batch
				 * file
				 */
				node->hj_JoinState = HJ_NEED_NEW_OUTER;


				// TODO: need to find a better way to distinguish if I should load inner batch again than checking for outer batch file
				// I need to also do this even if it is NULL when it is a ROJ
				// need to load inner again if it is an inner or left outer join and there are outer tuples in the batch OR
				// if it is a ROJ and there are inner tuples in the batch -- should never have no tuples in either batch...
				// TODO: is this right?
				// if outer is not null or if it is a ROJ and inner is not null, must rewind outer match status and load inner
				if (BufFileRewindIfExists(node->hj_HashTable->outerBatchFile[node->hj_HashTable->curbatch]) != NULL ||
					(node->hj_HashTable->innerBatchFile[node->hj_HashTable->curbatch] != NULL && HJ_FILL_INNER(node)))
				{
					BufFileRewindIfExists(node->hj_OuterMatchStatusesFile);
					node->hj_OuterTupleCount = 0;
					ExecHashJoinLoadInnerBatch(node);
				}
				else
				{
					if (node->hj_HashTable->curbatch == 21)
						elog(WARNING, "HJ_NEED_NEW_INNER_CHUNK. skipping loading batch 21");
				}
				break;

			case HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT:

				elog(DEBUG3, "HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT");

				if (parallel)
				{
					ParallelHashJoinBatch *phj_batch = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared;
					// PAHJ_debugging
					// tells us that a batch is falling back, how many chunks it has, and that it is correctly going to see if
					// it has any unmatched tuples to emit
					// VIP_PAHJ_debugging -- reenable to make sure we have multiple batches with differing numbers of chunks for testing
					elog(DEBUG3, "HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT: parallel. batch %i falls back with %i chunks. pid %i.",
							node->hj_HashTable->curbatch,
							phj_batch->total_num_chunks,
							MyProcPid);
					bool fallback = false;
					if (parallel)
						fallback = node->hj_HashTable->batches[node->hj_HashTable->curbatch].shared->parallel_hashloop_fallback;
					if (fallback == false)
					{
						node->hj_JoinState = HJ_NEED_NEW_BATCH;
						break;
					}
					SharedTuplestoreAccessor *outer_acc = hashtable->batches[hashtable->curbatch].outer_tuples;
					sts_reinitialize(outer_acc);
					sts_begin_parallel_scan(outer_acc);
				}
				else
				{
					// AHJ debugging
					elog(DEBUG3, "HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER_INIT: serial. batch %i falls back with %i chunks.", hashtable->curbatch, node->serial_chunk_count);
					node->hj_OuterTupleCount = 0;
					BufFileRewindIfExists(node->hj_OuterMatchStatusesFile);

					/* TODO: is it okay to use the hashtable to get the outer batch file here? */
					outerFileForAdaptiveRead = hashtable->outerBatchFile[hashtable->curbatch];
					if (outerFileForAdaptiveRead == NULL) /* TODO: could this happen */
					{
						node->hj_JoinState = HJ_NEED_NEW_BATCH;
						break;
					}
					BufFileRewindIfExists(outerFileForAdaptiveRead);
				}

				node->hj_JoinState = HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER;
				/* fall through */

			case HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER:

				elog(DEBUG3, "HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER");
				if (parallel)
				{
					SharedTuplestoreAccessor *outer_acc = node->hj_HashTable->batches[node->hj_HashTable->curbatch].outer_tuples;
					MinimalTuple tuple;
					// tells us that a fallback batch will actually scan for unmatched tuples -- in case the one in init didn't end up sending us here
					// and outerFileForAdaptiveRead was NULL
					elog(DEBUG3, "in HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER. batchno %i. pid %i. emitting at end of batch.", node->hj_HashTable->curbatch, MyProcPid);


					if (node->combined_bitmap == NULL)
						// TODO: make this an error for now, since all outer match status files have to exist bc we always make them
						elog(ERROR, "ExecParallelHashJoinNewBatch. outermatchstatus file is NULL. pid %i.", MyProcPid);

					do
					{
						tupleMetadata metadata;
						tuple = sts_parallel_scan_next(outer_acc, &metadata, true);
						if (tuple == NULL)
							break;

						int bytenum = metadata.tuplenum / 8;
						unsigned char bit = metadata.tuplenum % 8;
						unsigned char byte_to_check = 0;

						// seek to byte to check
						if (BufFileSeek(node->combined_bitmap, 0, bytenum, SEEK_SET))
							ereport(ERROR,
									(errcode_for_file_access(),
											errmsg("could not rewind shared outer temporary file: %m")));
						// read byte containing ntuple bit
						if (BufFileRead(node->combined_bitmap, &byte_to_check, 1) == 0)
							ereport(ERROR,
									(errcode_for_file_access(),
											errmsg("could not read byte in outer match status bitmap: %m.")));
						// if bit is set
						bool match = false;
						if (((byte_to_check) >> bit) & 1)
							match = true;

						elog(DEBUG1, "ExecParallelHashJoinNewBatch. tupleid: %i. match_status %i. read_byteval %hhu. bytenum %i. bitnum %hhu. pid %i.",
							 metadata.tuplenum, match, byte_to_check, bytenum, bit, MyProcPid);
						elog(DEBUG1,
							 "ProbeEnd tupleid %i. tupleval ?. match_status %i. bytenum %i. read_byteval %hhu. bitnum %i. pid %i.",
							 metadata.tuplenum,
							 match,
							 bytenum,
							 byte_to_check,
							 bit,
							 MyProcPid);
						// re-enable me
						elog(DEBUG3, "HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER. parallel. batch %i matched outer tuple value %i. pid %i",
							 hashtable->curbatch,
							 DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]),
							 MyProcPid);

						if (!match)
							break;
					} while (1);

					if (tuple == NULL)
					{
						// TODO: do I need to do any of this for the parallel non-fallback case?
						sts_end_parallel_scan(outer_acc);
						node->hj_JoinState = HJ_NEED_NEW_BATCH;
						break;
					}

					ExecForceStoreMinimalTuple(tuple,
											   econtext->ecxt_outertuple,
											   false);
					econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;
					// LOG_NOW inner and outer tuples here
					elog(NOTICE, "nomatch o.%i.i.NULL.",
						 DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]));

					// PAHJ_debugging
					// VIP_PAHJ_debugging tells us that an unmatched tuple gets emitted for this batch at the end
					// important that at least one test case exercises this
					// re-enable me to complete PAHJ test suite
					elog(DEBUG2, "HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER. parallel. emitting batch %i unmatched outer tuple value %i. pid %i",
							hashtable->curbatch,
							DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]),
							MyProcPid);
					return ExecProject(node->js.ps.ps_ProjInfo);

				}
				else
				{
					outerFileForAdaptiveRead = hashtable->outerBatchFile[hashtable->curbatch];

					while (true)
					{
						uint32 unmatchedOuterHashvalue;
						TupleTableSlot *temp = ExecHashJoinGetSavedTuple(node, outerFileForAdaptiveRead, &unmatchedOuterHashvalue, node->hj_OuterTupleSlot);
						node->hj_OuterTupleCount++;

						if (temp == NULL)
						{
							node->hj_JoinState = HJ_NEED_NEW_BATCH;
							break;
						}

						unsigned char bit = (node->hj_OuterTupleCount - 1) % 8;

						/* need to read the next byte */
						if (bit == 0)
							BufFileRead(node->hj_OuterMatchStatusesFile, &node->hj_OuterCurrentByte, 1);

						elog(DEBUG1, "in HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER. batchno %i. val %i. num %li. bitnum %hhu. current byte %hhu.",
							 node->hj_HashTable->curbatch,
							 DatumGetInt32(temp->tts_values[0]),
							 node->hj_OuterTupleCount,
							 bit,
							 node->hj_OuterCurrentByte);

						/* if the match bit is set for this tuple, continue */
						if ((node->hj_OuterCurrentByte >> bit) & 1)
							continue;
						/*
						 * if it is not a match
						 * emit it NULL-extended
						 */
						// AHJ debugging
						elog(DEBUG3, "HJ_ADAPTIVE_EMIT_UNMATCHED_OUTER. serial. emitting batch %i unmatched outer tuple value %i.",
								node->hj_HashTable->curbatch,
								DatumGetInt32(temp->tts_values[0])
								);
						econtext->ecxt_outertuple = temp;
						econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;
						return ExecProject(node->js.ps.ps_ProjInfo);
					}
				}
				/* came here from HJ_NEED_NEW_BATCH, so go back there */
				if (parallel)
				{
					sts_end_parallel_scan(hashtable->batches[hashtable->curbatch].outer_tuples);
					sts_end_parallel_scan(hashtable->batches[hashtable->curbatch].inner_tuples);
				}
				node->hj_JoinState = HJ_NEED_NEW_BATCH;
				break;

			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecHashJoin
 *
 *		Parallel-oblivious version.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *			/* return: a tuple or NULL */
ExecHashJoin(PlanState *pstate)
{
	/*
	 * On sufficiently smart compilers this should be inlined with the
	 * parallel-aware branches removed.
	 */
	return ExecHashJoinImpl(pstate, false);
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
	/*
	 * On sufficiently smart compilers this should be inlined with the
	 * parallel-oblivious branches removed.
	 */
	return ExecHashJoinImpl(pstate, true);
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
	hjstate->hj_OuterTupleCount  = 0;
	hjstate->local_matched_tuple_count = 0;
	hjstate->hj_InnerExhausted = false;

	hjstate->last_worker = false;
	hjstate->combined_bitmap = NULL;
	hjstate->current_outer_read_file = 0;
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
	 * Generate a fake join tuple with nulls for the inner
	 * tuple, and return it if it passes the non-join quals.
	 */
	// PAHJ_VIP_debugging
	// AHJ_VIP_debugging
	// re-enable for testing -- tells us for both serial and parallel which unmatched outer tuples are emitted during execution
	// as opposed to waiting to emit until the end of the batch in fallback case
	// will only log when there is an unmatched tuple
	elog(DEBUG3, "emitting outer tuple. fall back is false. value %i. batch %i. pid %i..", DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]), hjstate->hj_HashTable->curbatch, MyProcPid);
	if (otherqual == NULL || ExecQual(otherqual, econtext))
	{
		// logging inner and outer tuples here
		elog(NOTICE, "nomatch o.%i.i.NULL.",
			 DatumGetInt32(econtext->ecxt_outertuple->tts_values[0]));
		return ExecProject(hjstate->js.ps.ps_ProjInfo);
	}

	InstrCountFiltered2(hjstate, 1);
	return NULL;
}

/*
 * Only the elected worker will be calling this
 */
static void
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
	// tells us if batch 20 even tries to get an outer tuple
	if (curbatch == 20)
	{
		elog(DEBUG3, "ExecParallelHashJoinOuterGetTuple batch %i.", curbatch);
		//volatile int mybp = 0; while (mybp == 0);
	}
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
		int tupleid;
		tuple = sts_parallel_scan_next(hashtable->batches[curbatch].outer_tuples,
									   &metadata, true);
		if (tuple != NULL)
		{
			// where is this hashvalue being used?
			*hashvalue = metadata.hashvalue;
			tupleid = metadata.tuplenum; // change tuplenums to tupleid or similar to make less confusing that it is used for inner and outer
			ExecForceStoreMinimalTuple(tuple,
									   hjstate->hj_OuterTupleSlot,
									   false);
			// TODO: are either of these parallel-safe
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
	hjstate->serial_chunk_count = 1;
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
static bool ExecHashJoinLoadInnerBatch(HashJoinState *hjstate)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int curbatch = hashtable->curbatch;
	BufFile    *innerFile;
	TupleTableSlot *slot;
	uint32		hashvalue;

	off_t tup_start_offset;
	off_t chunk_start_offset;
	off_t tup_end_offset;
	int64 current_saved_size;
	int current_fileno;

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
				hjstate->serial_chunk_count++;
				return true;
			}
			hjstate->hj_InnerPageOffset = tup_end_offset;
			/*
			 * NOTE: some tuples may be sent to future batches.
			 * With current hashloop patch, however, it is not possible
			 * for hashtable->nbatch to be increased here
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

static bool
ExecParallelCheckHashloopFallback(HashJoinState *hjstate, int batchno)
{
	HashJoinTable hashtable = hjstate->hj_HashTable;
	int curbatch = hashtable->curbatch;
	if (curbatch != batchno)
		elog(ERROR, "curbatch != batchno in ExecParallelCheckHashloopFallback");
	ParallelHashJoinBatchAccessor *accessor = hashtable->batches + curbatch;
	ParallelHashJoinBatch *batch = accessor->shared;
	return batch->parallel_hashloop_fallback;
}

/*
 * Choose a batch to work on, and attach to it.  Returns true if successful,
 * false if there are no more batches.
 */
static bool
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
	 * or PHJ_BATCH_PROBING (note that the PHJ_BATCH_LOADING case will fall through
	 * to the PHJ_BATCH_PROBING case). The PHJ_BATCH_PROBING case returns to the
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
				hjstate->parallel_hashloop_fallback = true;
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
		uint32		hashvalue;
		MinimalTuple tuple;
		TupleTableSlot *slot;

		if (!hashtable->batches[batchno].done)
		{
			SharedTuplestoreAccessor *inner_tuples;
			Barrier    *batch_barrier =
			&hashtable->batches[batchno].shared->batch_barrier;

			switch (BarrierAttach(batch_barrier))
			{
				case PHJ_BATCH_ELECTING:
					/* One backend allocates the hash table. */
					elog(DEBUG3, "PHJ_BATCH_ELECTING batch %i. pid %i.",batchno, MyProcPid);
					if (BarrierArriveAndWait(batch_barrier,
											 WAIT_EVENT_HASH_BATCH_ELECTING))
						ExecParallelHashTableAlloc(hashtable, batchno);
					/* Fall through. */

				case PHJ_BATCH_ALLOCATING:
					/* Wait for allocation to complete. */
					elog(DEBUG3, "PHJ_BATCH_ALLOCATING batch %i. pid %i", batchno, MyProcPid);
					BarrierArriveAndWait(batch_barrier,
										 WAIT_EVENT_HASH_BATCH_ALLOCATING);
					/* Fall through. */

				case PHJ_BATCH_LOADING:
					/* Start (or join in) loading tuples. */
					elog(DEBUG3, "PHJ_BATCH_LOADING batch %i. pid %i.", batchno, MyProcPid);
					ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
					ParallelHashJoinBatch *phj_batch = hashtable->batches[batchno].shared;
					// FAVE_LOG show which workers are participating in loading tuples into a batch
					if (phj_batch->parallel_hashloop_fallback == true)
					{
						// PAHJ_debugging
						// VIP_PAHJ_debugging
						// re-enable for testing because it tells us if, for a given batch, more than one pid is participating
						// in loading inner tuples into hashtable which is an important test case -- also important that we have this
						// for different numbers of chunk in batch
						elog(DEBUG3, "PHJ_BATCH_LOADING. batch %i falls back with %i chunks. pid %i.",
								batchno, phj_batch->total_num_chunks, MyProcPid);
					}
					else
					{
						// same as above but it tells us for the non-fallback case who is participating in loading
						// also helps us see during loading phase if we have batches with multiple chunks that don't fall back
						elog(DEBUG3, "PHJ_BATCH_LOADING. batch %i does not fall back with %i chunks. pid %i.",
							 batchno, phj_batch->total_num_chunks, MyProcPid);
					}
					inner_tuples = hashtable->batches[batchno].inner_tuples;
					sts_begin_parallel_scan(inner_tuples);
					tupleMetadata metadata;
					int chunk_num = 0;
					while ((tuple = sts_parallel_scan_next(inner_tuples,
														   &metadata, false)))
					{
						hashvalue = metadata.hashvalue;
						chunk_num = metadata.tuplenum;
						ExecForceStoreMinimalTuple(tuple,
												   hjstate->hj_HashTupleSlot,
												   false);
						slot = hjstate->hj_HashTupleSlot;
						if (batchno == 7)
							elog(DEBUG3, "ExecParallelHashJoinNewBatch PHJ_BATCH_LOADING.  pid %i. batchno %i. chunk_num %i . total_chunk_num is %i .", MyProcPid, batchno, chunk_num, phj_batch->total_num_chunks);
						// TODO: make parallel safe
						// TODO: what should I do if they are not the same? should probably wait
						// TODO: should I do this inside ExecParallelHashTableInsertCurrentBatch
						// TODO: need to insert chunk_num also, I think
						if (chunk_num == phj_batch->current_chunk_num)
							ExecParallelHashTableInsertCurrentBatch(hashtable, slot,
																hashvalue, chunk_num);
					}
					sts_end_parallel_scan(inner_tuples);
					BarrierArriveAndWait(batch_barrier,
										 WAIT_EVENT_HASH_BATCH_LOADING);
					/* Fall through. */

				case PHJ_BATCH_PROBING:
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
					elog(DEBUG3, "PHJ_BATCH_PROBING batch %i. pid %i.", batchno, MyProcPid);

					ExecParallelHashTableSetCurrentBatch(hashtable, batchno);
					phj_batch = hashtable->batches[batchno].shared;
					// FAVE_LOG shows workers participating in probing a specific batch
					// PAHJ_debugging VIP_PAHJ_debugging
					// need test case with multiple workers probing the same batch with and without multiple chunks
					// same for below
					if (phj_batch->parallel_hashloop_fallback == true)
						elog(DEBUG3, "PHJ_BATCH_PROBING. batch %i falls back with %i chunks. pid %i.",
						 batchno, phj_batch->total_num_chunks, MyProcPid);
					else
						elog(DEBUG3, "PHJ_BATCH_PROBING. batch %i does not fall back with %i chunks. pid %i.",
							 batchno, phj_batch->total_num_chunks, MyProcPid);
					sts_begin_parallel_scan(hashtable->batches[batchno].outer_tuples);

					// print estimated size for batch 20
					if (batchno == 20)
					{
						size_t size = phj_batch->estimated_size;
						// FAVE_LOG
						// PAHJ_debugging
						// not needed for testing but hleps us see if the estimated size of the outer batch file exceeds our work_mem
						// and if we have started chunking it and if we have set fallback to true
						// will be useful later if we want to calibrate the logic for falling back
						elog(DEBUG3, "In ExecParallelHashJoinNewBatch PHJ_BATCH_PROBING. For batchno 20, the estimated size of the outer file is %zu. fallback is %i. total_num_chunks is %i . pid %i.",
								size, phj_batch->parallel_hashloop_fallback, phj_batch->total_num_chunks, MyProcPid);
					}

					// Create an outer match status file for this batch for this worker
					// This file must be accessible to the other workers
					// But *only* written to by this worker. Written to by this worker and readable by any worker
					if (hashtable->batches[batchno].shared->parallel_hashloop_fallback == true)
					{
						char outer_match_status_filename[MAXPGPATH];
						sts_make_STA_outerMatchStatuses(hashtable->batches[batchno].outer_tuples, batchno, outer_match_status_filename);
						hjstate->parallel_hashloop_fallback = true;
					}
					return true;

				case PHJ_BATCH_OUTER_MATCH_STATUS_PROCESSING:
					elog(DEBUG3, "in PHJ_BATCH_OUTER_MATCH_STATUS_PROCESSING. done with batchno %i. process %i.", batchno, MyProcPid);
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
					elog(DEBUG3, "in PHJ_BATCH_DONE. done with batchno %i. process %i.", batchno, MyProcPid);
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
			sts_puttuple(hashtable->batches[batchno].outer_tuples,
						 &metadata, mintup, true, 0);

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
	pstate->num_batch_increases = 0;
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
	 * PHJ_BUILD_DONE and batch 0's batch_barrier to PHJ_BATCH_PROBING, but
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
