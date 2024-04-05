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

/*  MTODO: add function comments */
/*  MTODO: is the dsa_area supposed to be void when not parallel? */
/*
 * tbm will be NULL for parallel workers
 */
void
bhs_begin_iterate(BitmapHeapIterator *iterator, TIDBitmap *tbm,
				  dsa_area *dsa, dsa_pointer dsp)
{
	Assert(iterator);

	iterator->serial = NULL;
	iterator->parallel = NULL;
	iterator->exhausted = false;

	/* Allocate a private iterator and attach the shared state to it */
	if (DsaPointerIsValid(dsp))
		iterator->parallel = tbm_attach_shared_iterate(dsa, dsp);
	else
		iterator->serial = tbm_begin_iterate(tbm);
}

TBMIterateResult *
bhs_iterate(BitmapHeapIterator *iterator)
{
	Assert(iterator);
	Assert(!iterator->exhausted);

	if (iterator->serial)
		return tbm_iterate(iterator->serial);
	else
		return tbm_shared_iterate(iterator->parallel);
}

void
bhs_end_iterate(BitmapHeapIterator *iterator)
{
	Assert(iterator);

	if (iterator->serial)
		tbm_end_iterate(iterator->serial);
	else if (iterator->parallel)
		tbm_end_shared_iterate(iterator->parallel);

	iterator->serial = NULL;
	iterator->parallel = NULL;
	iterator->exhausted = true;
}



 /*
  * If we haven't yet performed the underlying index scan, do it, and begin
  * the iteration over the bitmap.
  *
  * For prefetching, we use *two* iterators, one for the pages we are actually
  * scanning and another that runs ahead of the first for prefetching.
  * node->prefetch_pages tracks exactly how many pages ahead the prefetch
  * iterator is.  Also, node->prefetch_target tracks the desired prefetch
  * distance, which starts small and increases up to the
  * scan->prefetch_maximum.  This is to avoid doing a lot of prefetching in a
  * scan that stops after a few tuples because of a LIMIT.
  */
static void
BitmapHeapInitialize(BitmapHeapScanState *node)
{
	Relation	rel = node->ss.ss_currentRelation;
	bool		pf_maximum = 0;
	bool		init_shared_state = false;
	uint32		extra_flags = 0;

	Assert(!node->initialized);

	/*
	 * The leader will immediately come out of the function, but others will
	 * be blocked until leader populates the TBM and wakes them up.
	 */
	init_shared_state = node->pstate ?
		BitmapShouldInitializeSharedState(node->pstate) : false;

	/*
	 * Maximum number of prefetches for the tablespace if configured,
	 * otherwise the current value of the effective_io_concurrency GUC.
	 */
#ifdef USE_PREFETCH
	pf_maximum = get_tablespace_io_concurrency(rel->rd_rel->reltablespace);
#endif

	if (!node->pstate || init_shared_state)
	{
		node->tbm = (TIDBitmap *) MultiExecProcNode(outerPlanState(node));

		if (!node->tbm || !IsA(node->tbm, TIDBitmap))
			elog(ERROR, "unrecognized result from subplan");

		if (init_shared_state)
		{
			/*
			 * Prepare to iterate over the TBM. This will return the
			 * dsa_pointer of the iterator state which will be used by
			 * multiple processes to iterate jointly.
			 */
			node->pstate->tbmiterator = tbm_prepare_shared_iterate(node->tbm);

#ifdef USE_PREFETCH
			if (pf_maximum > 0)
			{
				node->pstate->prefetch_iterator =
					tbm_prepare_shared_iterate(node->tbm);
			}
#endif
			/* We have initialized the shared state so wake up others. */
			BitmapDoneInitializingSharedState(node->pstate);
		}
	}

	/*
	 * We can potentially skip fetching heap pages if we do not need any
	 * columns of the table, either for checking non-indexable quals or for
	 * returning data.  This test is a bit simplistic, as it checks the
	 * stronger condition that there's no qual or return tlist at all. But in
	 * most cases it's probably not worth working harder than that.
	 */
	if (node->ss.ps.plan->qual != NIL || node->ss.ps.plan->targetlist != NIL)
		extra_flags |= SO_NEED_TUPLE;

	node->ss.ss_currentScanDesc = table_beginscan_bm(node->ss.ss_currentScanDesc,
													 rel,
													 node->ss.ps.state->es_snapshot,
													 extra_flags,
													 pf_maximum,
													 node->tbm,
													 node->pstate,
													 node->ss.ps.state->es_query_dsa);

	node->initialized = true;
}


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
	TupleTableSlot *slot;
	TableScanDesc scan;

	/*
	 * If we haven't yet performed the underlying index scan, do it, and begin
	 * the iteration over the bitmap. This happens on rescan as well.
	 */
	if (!node->initialized)
		BitmapHeapInitialize(node);

	/*
	 * BitmapHeapInitialize() may make the scan descriptor so don't get it
	 * from the node until after calling it.
	 */
	econtext = node->ss.ps.ps_ExprContext;
	slot = node->ss.ss_ScanTupleSlot;
	scan = node->ss.ss_currentScanDesc;

	while (table_scan_bitmap_next_tuple(scan, slot, &node->recheck,
										&node->lossy_pages, &node->exact_pages))
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * If we are using lossy info, we have to recheck the qual conditions
		 * at every tuple.
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

	/* rescan to release any page pin */
	if (node->ss.ss_currentScanDesc)
		table_rescan(node->ss.ss_currentScanDesc, NULL);

	/* release bitmaps if any */
	if (node->tbm)
		tbm_free(node->tbm);
	node->tbm = NULL;
	node->initialized = false;
	node->recheck = true;

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
	 * close heap scan
	 */
	if (scanDesc)
		table_endscan(scanDesc);

	/*
	 * release bitmaps if any
	 */
	if (node->tbm)
		tbm_free(node->tbm);
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
	scanstate->exact_pages = 0;
	scanstate->lossy_pages = 0;
	scanstate->initialized = false;
	scanstate->pstate = NULL;
	scanstate->recheck = true;

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
