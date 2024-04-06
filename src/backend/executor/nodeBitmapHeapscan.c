/*-------------------------------------------------------------------------
 *
 * nodeBitmapHeapscan.c
 *	  Routines to support bitmapped scans of relations
 *
 * NOTE: it is critical that this plan type only be used with MVCC-compliant
 * snapshots (ie, regular snapshots, not SnapshotAny or one of the other
 * special snapshots).  The reason is that since index and heap scans are
 * decoupled, there can be no assurance that the index tuple prompting a
 * visit to a particular heap TID still exists when the visit is made.
 * Therefore the tuple might not exist anymore either (which is OK because
 * heap_fetch will cope) --- but worse, the tuple slot could have been
 * re-used for a newer tuple.  With an MVCC snapshot the newer tuple is
 * certain to fail the time qual and so it will not be mistakenly returned,
 * but with anything else we might return a tuple that doesn't meet the
 * required index qual conditions.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeBitmapHeapscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecBitmapHeapScan			scans a relation using bitmap info
 *		ExecBitmapHeapNext			workhorse for above
 *		ExecInitBitmapHeapScan		creates and initializes state info.
 *		ExecReScanBitmapHeapScan	prepares to rescan the plan.
 *		ExecEndBitmapHeapScan		releases all storage.
 */
#include "postgres.h"

#include <math.h>

#include "access/relscan.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/heapam.h"
#include "executor/executor.h"
#include "executor/nodeBitmapHeapscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/spccache.h"

static TupleTableSlot *BitmapHeapNext(BitmapHeapScanState *node);
static inline void BitmapDoneInitializingSharedState(ParallelBitmapHeapState *pstate);
static bool BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate);


/* ----------------------------------------------------------------
 *		BitmapHeapNext
 *
 *		Retrieve next tuple from the BitmapHeapScan node's currentRelation
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
BitmapHeapNext(BitmapHeapScanState *node)
{
	ExprContext *econtext;
	TableScanDesc scan;
	TIDBitmap  *tbm;
	TupleTableSlot *slot;
	ParallelBitmapHeapState *pstate = node->pstate;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;
	scan = node->ss.ss_currentScanDesc;
	tbm = node->tbm;

	/*
	 * If we haven't yet performed the underlying index scan, do it, and begin
	 * the iteration over the bitmap.
	 *
	 * For prefetching, we use *two* iterators, one for the pages we are
	 * actually scanning and another that runs ahead of the first for
	 * prefetching.  node->prefetch_pages tracks exactly how many pages ahead
	 * the prefetch iterator is.  Also, node->prefetch_target tracks the
	 * desired prefetch distance, which starts small and increases up to the
	 * node->prefetch_maximum.  This is to avoid doing a lot of prefetching in
	 * a scan that stops after a few tuples because of a LIMIT.
	 */
	if (!node->initialized)
	{
		bool		need_tuples = false;

		/*
		 * The leader will immediately come out of the function, but others
		 * will be blocked until leader populates the TBM and wakes them up.
		 * Serial bitmap table scan will simply build the bitmap.
		 */
		bool		init_shared_state = node->pstate ?
			BitmapShouldInitializeSharedState(node->pstate) : false;

		/*
		 * Only serial bitmap table scans and the parallel leader in a
		 * parallel bitmap table scan should build the bitmap.
		 */
		if (!pstate || init_shared_state)
		{
			tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));

			if (!tbm || !IsA(tbm, TIDBitmap))
				elog(ERROR, "unrecognized result from subplan");
			node->tbm = tbm;

			if (init_shared_state)
			{
				/*
				 * Prepare to iterate over the TBM. This will return the
				 * dsa_pointer of the iterator state which will be used by
				 * multiple processes to iterate jointly.
				 */
				pstate->tbmiterator = tbm_prepare_shared_iterate(tbm);
#ifdef USE_PREFETCH
				if (node->prefetch_maximum > 0)
				{
					pstate->prefetch_iterator =
						tbm_prepare_shared_iterate(tbm);
				}
#endif
				/* We have initialized the shared state so wake up others. */
				BitmapDoneInitializingSharedState(pstate);
			}
		}


		/*
		 * We can potentially skip fetching heap pages if we do not need any
		 * columns of the table, either for checking non-indexable quals or
		 * for returning data.  This test is a bit simplistic, as it checks
		 * the stronger condition that there's no qual or return tlist at all.
		 * But in most cases it's probably not worth working harder than that.
		 */
		need_tuples = (node->ss.ps.plan->qual != NIL ||
					   node->ss.ps.plan->targetlist != NIL);

		/*
		 * If this is the first scan of the underlying table, create the table
		 * scan descriptor and begin the scan.
		 */
		scan = table_beginscan_bm(node->ss.ss_currentScanDesc,
								  node->ss.ss_currentRelation,
								  node->ss.ps.state->es_snapshot,
								  need_tuples,
								  node->tbm,
								  node->pstate,
								  node->ss.ps.state->es_query_dsa);

		node->ss.ss_currentScanDesc = scan;

#ifdef USE_PREFETCH
		if (node->prefetch_maximum > 0)
		{
			unified_tbm_begin_iterate(&node->prefetch_iterator, tbm, dsa,
									  pstate ?
									  pstate->prefetch_iterator :
									  InvalidDsaPointer);
		}
#endif							/* USE_PREFETCH */

		node->initialized = true;

		goto new_page;
	}

	for (;;)
	{
		while (table_scan_bitmap_next_tuple(scan, slot))
		{
			/*
			 * Continuing in previously obtained page.
			 */

			CHECK_FOR_INTERRUPTS();

#ifdef USE_PREFETCH

			/*
			 * Try to prefetch at least a few pages even before we get to the
			 * second page if we don't stop reading after the first tuple.
			 */
			if (!pstate)
			{
				if (node->prefetch_target < node->prefetch_maximum)
					node->prefetch_target++;
			}
			else if (pstate->prefetch_target < node->prefetch_maximum)
			{
				/* take spinlock while updating shared state */
				SpinLockAcquire(&pstate->mutex);
				if (pstate->prefetch_target < node->prefetch_maximum)
					pstate->prefetch_target++;
				SpinLockRelease(&pstate->mutex);
			}
#endif							/* USE_PREFETCH */

			/*
			 * We issue prefetch requests *after* fetching the current page to
			 * try to avoid having prefetching interfere with the main I/O.
			 * Also, this should happen only when we have determined there is
			 * still something to do on the current page, else we may
			 * uselessly prefetch the same page we are just about to request
			 * for real.
			 */
			BitmapPrefetch(node, scan);

			/*
			 * If we are using lossy info, we have to recheck the qual
			 * conditions at every tuple.
			 */
			if (node->recheck)
			{
				econtext->ecxt_scantuple = slot;
				if (!ExecQualAndReset(node->bitmapqualorig, econtext))
				{
					/* Fails recheck, so drop it and loop back for another */
					InstrCountFiltered2(node, 1);
					ExecClearTuple(slot);
					continue;
				}
			}

			/* OK to return this tuple */
			return slot;
		}

new_page:

		BitmapAdjustPrefetchIterator(node);

		if (!table_scan_bitmap_next_block(scan, &node->blockno, &node->recheck,
										  &node->lossy_pages, &node->exact_pages))
			break;

		/*
		 * If serial, we can error out if the the prefetch block doesn't stay
		 * ahead of the current block.
		 */
		if (node->pstate == NULL &&
			!node->prefetch_iterator.exhausted &&
			node->pfblockno < node->blockno)
			elog(ERROR, "prefetch and main iterators are out of sync");

		/* Adjust the prefetch target */
		BitmapAdjustPrefetchTarget(node);
	}

	/*
	 * if we get here it means we are at the end of the scan..
	 */
	return ExecClearTuple(slot);
}

/*
 *	BitmapDoneInitializingSharedState - Shared state is initialized
 *
 *	By this time the leader has already populated the TBM and initialized the
 *	shared state so wake up other processes.
 */
static inline void
BitmapDoneInitializingSharedState(ParallelBitmapHeapState *pstate)
{
	SpinLockAcquire(&pstate->mutex);
	pstate->state = BM_FINISHED;
	SpinLockRelease(&pstate->mutex);
	ConditionVariableBroadcast(&pstate->cv);
}

/*
 * BitmapHeapRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
BitmapHeapRecheck(BitmapHeapScanState *node, TupleTableSlot *slot)
{
	ExprContext *econtext;

	/*
	 * extract necessary information from index scan node
	 */
	econtext = node->ss.ps.ps_ExprContext;

	/* Does the tuple meet the original qual conditions? */
	econtext->ecxt_scantuple = slot;
	return ExecQualAndReset(node->bitmapqualorig, econtext);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecBitmapHeapScan(PlanState *pstate)
{
	BitmapHeapScanState *node = castNode(BitmapHeapScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) BitmapHeapNext,
					(ExecScanRecheckMtd) BitmapHeapRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanBitmapHeapScan(node)
 * ----------------------------------------------------------------
 */
void
ExecReScanBitmapHeapScan(BitmapHeapScanState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	/* release bitmaps and buffers if any */
	if (node->prefetch_iterator.exhausted)
		unified_tbm_end_iterate(&node->prefetch_iterator);
	if (node->tbm)
		tbm_free(node->tbm);
	if (node->pvmbuffer != InvalidBuffer)
		ReleaseBuffer(node->pvmbuffer);
	node->tbm = NULL;
	node->initialized = false;
	node->pvmbuffer = InvalidBuffer;
	node->recheck = true;
	node->blockno = InvalidBlockNumber;
	node->pfblockno = InvalidBlockNumber;
	/* Only used for serial BHS */
	node->prefetch_pages = 0;
	node->prefetch_target = -1;

	ExecScanReScan(&node->ss);

	/*
	 * if chgParam of subnode is not null then plan will be re-scanned by
	 * first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
}

/* ----------------------------------------------------------------
 *		ExecEndBitmapHeapScan
 * ----------------------------------------------------------------
 */
void
ExecEndBitmapHeapScan(BitmapHeapScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * extract information from the node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));


	/*
	 * close bitmap heap scan
	 */
	if (scanDesc)
		table_endscan_bm(scanDesc);

	/*
	 * release bitmaps and buffers if any
	 */
	if (!node->prefetch_iterator.exhausted)
		unified_tbm_end_iterate(&node->prefetch_iterator);
	if (node->tbm)
		tbm_free(node->tbm);
	if (node->pvmbuffer != InvalidBuffer)
		ReleaseBuffer(node->pvmbuffer);
}

/* ----------------------------------------------------------------
 *		ExecInitBitmapHeapScan
 *
 *		Initializes the scan's state information.
 * ----------------------------------------------------------------
 */
BitmapHeapScanState *
ExecInitBitmapHeapScan(BitmapHeapScan *node, EState *estate, int eflags)
{
	BitmapHeapScanState *scanstate;
	Relation	currentRelation;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * Assert caller didn't ask for an unsafe snapshot --- see comments at
	 * head of file.
	 */
	Assert(IsMVCCSnapshot(estate->es_snapshot));

	/*
	 * create state structure
	 */
	scanstate = makeNode(BitmapHeapScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecBitmapHeapScan;

	scanstate->tbm = NULL;
	scanstate->pvmbuffer = InvalidBuffer;
	scanstate->exact_pages = 0;
	scanstate->lossy_pages = 0;
	scanstate->prefetch_pages = 0;
	scanstate->prefetch_target = -1;
	scanstate->initialized = false;
	scanstate->pstate = NULL;
	scanstate->recheck = true;
	scanstate->blockno = InvalidBlockNumber;
	scanstate->pfblockno = InvalidBlockNumber;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

	/*
	 * initialize child nodes
	 */
	outerPlanState(scanstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * get the scan type from the relation descriptor.
	 */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(currentRelation),
						  table_slot_callbacks(currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);
	scanstate->bitmapqualorig =
		ExecInitQual(node->bitmapqualorig, (PlanState *) scanstate);

	/*
	 * Maximum number of prefetches for the tablespace if configured,
	 * otherwise the current value of the effective_io_concurrency GUC.
	 */
	scanstate->prefetch_maximum =
		get_tablespace_io_concurrency(currentRelation->rd_rel->reltablespace);

	scanstate->ss.ss_currentRelation = currentRelation;

	/*
	 * all done.
	 */
	return scanstate;
}

/*----------------
 *		BitmapShouldInitializeSharedState
 *
 *		The first process to come here and see the state to the BM_INITIAL
 *		will become the leader for the parallel bitmap scan and will be
 *		responsible for populating the TIDBitmap.  The other processes will
 *		be blocked by the condition variable until the leader wakes them up.
 * ---------------
 */
static bool
BitmapShouldInitializeSharedState(ParallelBitmapHeapState *pstate)
{
	SharedBitmapState state;

	while (1)
	{
		SpinLockAcquire(&pstate->mutex);
		state = pstate->state;
		if (pstate->state == BM_INITIAL)
			pstate->state = BM_INPROGRESS;
		SpinLockRelease(&pstate->mutex);

		/* Exit if bitmap is done, or if we're the leader. */
		if (state != BM_INPROGRESS)
			break;

		/* Wait for the leader to wake us up. */
		ConditionVariableSleep(&pstate->cv, WAIT_EVENT_PARALLEL_BITMAP_SCAN);
	}

	ConditionVariableCancelSleep();

	return (state == BM_INITIAL);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapEstimate(BitmapHeapScanState *node,
					   ParallelContext *pcxt)
{
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(ParallelBitmapHeapState));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapInitializeDSM
 *
 *		Set up a parallel bitmap heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapInitializeDSM(BitmapHeapScanState *node,
							ParallelContext *pcxt)
{
	ParallelBitmapHeapState *pstate;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;

	/* If there's no DSA, there are no workers; initialize nothing. */
	if (dsa == NULL)
		return;

	pstate = shm_toc_allocate(pcxt->toc, sizeof(ParallelBitmapHeapState));

	pstate->tbmiterator = 0;
	pstate->prefetch_iterator = 0;

	/* Initialize the mutex */
	SpinLockInit(&pstate->mutex);
	pstate->prefetch_pages = 0;
	pstate->prefetch_target = -1;
	pstate->state = BM_INITIAL;

	ConditionVariableInit(&pstate->cv);

	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pstate);
	node->pstate = pstate;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapReInitializeDSM(BitmapHeapScanState *node,
							  ParallelContext *pcxt)
{
	ParallelBitmapHeapState *pstate = node->pstate;
	dsa_area   *dsa = node->ss.ps.state->es_query_dsa;

	/* If there's no DSA, there are no workers; do nothing. */
	if (dsa == NULL)
		return;

	pstate->state = BM_INITIAL;
	pstate->prefetch_pages = 0;
	pstate->prefetch_target = -1;

	if (DsaPointerIsValid(pstate->tbmiterator))
		tbm_free_shared_area(dsa, pstate->tbmiterator);

	if (DsaPointerIsValid(pstate->prefetch_iterator))
		tbm_free_shared_area(dsa, pstate->prefetch_iterator);

	pstate->tbmiterator = InvalidDsaPointer;
	pstate->prefetch_iterator = InvalidDsaPointer;
}

/* ----------------------------------------------------------------
 *		ExecBitmapHeapInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecBitmapHeapInitializeWorker(BitmapHeapScanState *node,
							   ParallelWorkerContext *pwcxt)
{
	ParallelBitmapHeapState *pstate;

	Assert(node->ss.ps.state->es_query_dsa != NULL);

	pstate = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	node->pstate = pstate;
}
