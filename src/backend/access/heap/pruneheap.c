/*-------------------------------------------------------------------------
 *
 * pruneheap.c
 *	  heap page pruning and HOT-chain management code
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/pruneheap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"


/* Local functions */
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid);
static void heap_prune_record_redirect(PruneState *prstate, Page page, HeapPageFreeze *pagefrz,
						   OffsetNumber offnum, OffsetNumber rdoffnum, bool opportunistic);
static void heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum, VacDeadItems *dead_items);
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum);
static void page_verify_redirects(Page page);
static void
catalog_dead_item_for_vacuum(VacDeadItems *dead_items, BlockNumber blkno, OffsetNumber offnum);

/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * This is an opportunistic function.  It will perform housekeeping
 * only if the page heuristically looks like a candidate for pruning and we
 * can acquire buffer cleanup lock without blocking.
 *
 * Note: this is called quite often.  It's important that it fall out quickly
 * if there's not any use in pruning.
 *
 * Caller must have pin on the buffer, and must *not* have a lock on it.
 */
void
heap_page_prune_opt(Relation relation, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prune_xid;
	GlobalVisState *vistest;
	Size		minfree;
	BlockNumber blkno = BufferGetBlockNumber(buffer);
	HeapPageFreeze pagefrz;
	PruneResult prune_result;
	VacuumCutoffs cutoffs;
	OffsetNumber offnum = 0;

	pagefrz.freeze_required = false;
	pagefrz.FreezePageRelfrozenXid = InvalidTransactionId;
	pagefrz.FreezePageRelminMxid = InvalidTransactionId;
	pagefrz.NoFreezePageRelfrozenXid = InvalidTransactionId;
	pagefrz.NoFreezePageRelminMxid = InvalidTransactionId;
	pagefrz.cutoffs = &cutoffs;
	pagefrz.opportunistic = true;

	memset(&prune_result, 0, sizeof(PruneResult));
	/*
	 * We can't write WAL in recovery mode, so there's no point trying to
	 * clean the page. The primary will likely issue a cleaning WAL record
	 * soon anyway, so this is no particular loss.
	 */
	if (RecoveryInProgress())
		return;

	/*
	 * XXX: Magic to keep old_snapshot_threshold tests appear "working". They
	 * currently are broken, and discussion of what to do about them is
	 * ongoing. See
	 * https://www.postgresql.org/message-id/20200403001235.e6jfdll3gh2ygbuc%40alap3.anarazel.de
	 */
	if (old_snapshot_threshold == 0)
		SnapshotTooOldMagicForTest();

	/*
	 * First check whether there's any chance there's something to prune,
	 * determining the appropriate horizon is a waste if there's no prune_xid
	 * (i.e. no updates/deletes left potentially dead tuples around).
	 */
	prune_xid = ((PageHeader) page)->pd_prune_xid;
	if (!TransactionIdIsValid(prune_xid))
		return;

	/*
	 * Check whether prune_xid indicates that there may be dead rows that can
	 * be cleaned up.
	 */
	vistest = GlobalVisTestFor(relation);

	if (!GlobalVisTestIsRemovableXid(vistest, prune_xid))
		return;

	cutoffs.OldestXmin = InvalidTransactionId;
	cutoffs.OldestMxact = InvalidMultiXactId;
	cutoffs.FreezeLimit = InvalidTransactionId;
	cutoffs.MultiXactCutoff = InvalidMultiXactId;
	cutoffs.relfrozenxid = relation->rd_rel->relfrozenxid;
	/* We should not use relminmxid */
	cutoffs.relminmxid = relation->rd_rel->relminmxid;
	/*
	 * We prune when a previous UPDATE failed to find enough space on the page
	 * for a new tuple version, or when free space falls below the relation's
	 * fill-factor target (but not less than 10%).
	 *
	 * Checking free space here is questionable since we aren't holding any
	 * lock on the buffer; in the worst case we could get a bogus answer. It's
	 * unlikely to be *seriously* wrong, though, since reading either pd_lower
	 * or pd_upper is probably atomic.  Avoiding taking a lock seems more
	 * important than sometimes getting a wrong answer in what is after all
	 * just a heuristic estimate.
	 */
	minfree = RelationGetTargetPageFreeSpace(relation,
											 HEAP_DEFAULT_FILLFACTOR);
	minfree = Max(minfree, BLCKSZ / 10);

	if (!PageIsFull(page) && !(PageGetHeapFreeSpace(page) < minfree))
		return;

	/* OK, try to get exclusive buffer lock */
	if (!ConditionalLockBufferForCleanup(buffer))
		return;

	/*
	 * Now that we have buffer lock, get accurate information about the
	 * page's free space, and recheck the heuristic about whether to
	 * prune.
	 */
	if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
		heap_page_prune(relation, buffer, blkno, page,
				InvalidBuffer, 0, vistest,
				&pagefrz, NULL,
				false, true,
				&prune_result, &offnum);

	/* And release buffer lock */
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	pgstat_update_heap_dead_tuples(relation,
									prune_result.nunused);

	/*
	 * We avoid reuse of any free space created on the page by unrelated
	 * UPDATEs/INSERTs by opting to not update the FSM at this point.  The
	 * free space should be reused by UPDATEs to *this* page.
	 */
}

static void
catalog_dead_item_for_vacuum(VacDeadItems *dead_items, BlockNumber blkno, OffsetNumber offnum)
{
	ItemPointerData tmp;
	if (!dead_items)
		return;
	ItemPointerSetBlockNumber(&tmp, blkno);
	ItemPointerSetOffsetNumber(&tmp, offnum);
	dead_items->items[dead_items->num_items++] = tmp;
}

bool
heap_page_prune(Relation rel, Buffer buf, BlockNumber blkno, Page page,
		Buffer vmbuffer, uint8 vmbits, GlobalVisState *vistest,
		HeapPageFreeze *pagefrz,
		VacDeadItems *dead_items,
		bool pronto_vac,
		bool opportunistic,
		PruneResult *result,
		OffsetNumber *off_loc)
{
	OffsetNumber maxoff, offnum;
	PruneState	prstate;
	bool do_freeze = false, do_prune = false;

	bool vm_modified = false;
	uint8 visiflags = 0;
	bool all_visible_according_to_vm = vmbits & VISIBILITYMAP_ALL_VISIBLE;
	bool all_frozen_according_to_vm = vmbits & VISIBILITYMAP_ALL_FROZEN;

	TransactionId frz_conflict_horizon = InvalidTransactionId;

	bool new_prune_xid_found = false;

	Oid tableoid = RelationGetRelid(rel);

	bool page_all_visible = PageIsAllVisible(page);

	Assert(BufferGetBlockNumber(buf) == blkno);
	Assert(off_loc);

	prstate.youngest_visible_xmin = InvalidTransactionId;
	prstate.youngest_xmax_reaped = InvalidTransactionId;
	prstate.blkno = blkno;
	prstate.hastup = false;
	prstate.nfrozen = 0;
	prstate.new_prune_xid = InvalidTransactionId;
	prstate.vistest = vistest;
	prstate.all_visible = true;
	prstate.all_frozen = true;
	prstate.nredirected = prstate.ndead = prstate.nunused = 0;
	prstate.result = result;
	memset(prstate.marked, 0, sizeof(prstate.marked));
	memset(prstate.frz_attempted, 0, sizeof(prstate.frz_attempted));

	maxoff = PageGetMaxOffsetNumber(page);

	for (offnum = maxoff;
		 offnum >= FirstOffsetNumber;
		 offnum = OffsetNumberPrev(offnum))
	{
		HeapTupleData tup;
		ItemId itemid = PageGetItemId(page, offnum);

		if (!ItemIdIsNormal(itemid))
		{
			prstate.htsv[offnum] = -1;
			continue;
		}

		tup.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tup.t_tableOid = tableoid;
		tup.t_len = ItemIdGetLength(itemid);
		ItemPointerSet(&tup.t_self, blkno, offnum);

		*off_loc = offnum;

		prstate.htsv[offnum] =
			heap_prune_satisfies_vacuum(&prstate, &tup, buf, offnum);
	}

	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		HeapTupleHeader htup;
		bool tuple_frozen;
		ItemId itemid = PageGetItemId(page, offnum);

		if (!ItemIdIsUsed(itemid))
			continue;

		if (ItemIdIsDead(itemid))
		{
			if (pronto_vac)
			{
				ItemIdSetUnused(itemid);
				prstate.nowunused[prstate.nunused++] = offnum;
				continue;
			}
			prstate.all_visible = false;
			catalog_dead_item_for_vacuum(dead_items, blkno, offnum);
			continue;
		}

		*off_loc = offnum;

		heap_prune_chain(buf, offnum, &prstate, pagefrz, pronto_vac,
				dead_items, opportunistic);

		if (!ItemIdIsNormal(itemid) || prstate.marked[offnum] ||
				prstate.frz_attempted[offnum] ||
				prstate.htsv[offnum] == HEAPTUPLE_DEAD ||
				prstate.htsv[offnum] == -1)
			continue;

		htup = (HeapTupleHeader) PageGetItem(page, itemid);
		if ((heap_prepare_freeze_tuple(htup, pagefrz,
									&prstate.frozen[prstate.nfrozen], &tuple_frozen,
									prstate.vistest, opportunistic)))
		{
			prstate.frozen[prstate.nfrozen++].offset = offnum;
		}

		prstate.frz_attempted[offnum] = true;
		prstate.all_frozen = prstate.all_frozen && tuple_frozen;
	}

	*off_loc = InvalidOffsetNumber;

	if (!prstate.all_visible)
		prstate.all_frozen = false;

	do_prune = prstate.nredirected > 0 || prstate.ndead > 0 || prstate.nunused > 0;

	do_freeze = (pagefrz->freeze_required ||
		(prstate.all_visible && prstate.all_frozen && prstate.nfrozen > 0) ||
		(prstate.nfrozen > 0 && !XLogCheckBufferNeedsBackup(buf)));

	if (opportunistic && !do_prune)
		do_freeze = false;

	if (do_freeze)
	{
		for (int i = 0; i < prstate.nfrozen; i++)
		{
			HeapTupleFreeze *frz = prstate.frozen + i;
			ItemId		itemid = PageGetItemId(page, frz->offset);
			HeapTupleHeader htup = (HeapTupleHeader) PageGetItem(page, itemid);

			if (frz->checkflags & HEAP_FREEZE_CHECK_XMIN_COMMITTED)
			{
				TransactionId xmin = HeapTupleHeaderGetRawXmin(htup);

				Assert(!HeapTupleHeaderXminFrozen(htup));
				if (unlikely(!TransactionIdDidCommit(xmin)))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							errmsg_internal("uncommitted xmin %u needs to be frozen",
											xmin)));
			}

			if (frz->checkflags & HEAP_FREEZE_CHECK_XMAX_ABORTED)
			{
				TransactionId xmax = HeapTupleHeaderGetRawXmax(htup);

				Assert(TransactionIdIsNormal(xmax));
				if (unlikely(TransactionIdDidCommit(xmax)))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							errmsg_internal("cannot freeze committed xmax %u",
											xmax)));
			}
		}
	}
	else
	{
		prstate.nfrozen = 0;
		prstate.all_frozen = false;
	}

	result->nunused = prstate.nunused;
	result->nfrozen = prstate.nfrozen;

	if (opportunistic && !do_prune)
		return prstate.hastup;

	START_CRIT_SECTION();

	if (do_prune)
	{
		heap_page_prune_execute(buf,
								prstate.redirected, prstate.nredirected,
								prstate.nowdead, prstate.ndead,
								prstate.nowunused, prstate.nunused);

		if (((PageHeader) page)->pd_prune_xid != prstate.new_prune_xid)
			new_prune_xid_found = true;

		((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
	}

	do_prune = prstate.nunused > 0 || do_prune;

	if (prstate.all_visible && !page_all_visible)
		PageSetAllVisible(page);
	else if (!prstate.all_visible && page_all_visible)
		PageClearAllVisible(page);

	if (do_prune || new_prune_xid_found || PageIsFull(page))
		PageClearFull(page);

	if (do_prune || do_freeze ||
			(prstate.all_visible && !page_all_visible) ||
			(!prstate.all_visible && page_all_visible))
		MarkBufferDirty(buf);
	else if (new_prune_xid_found || PageIsFull(page))
		MarkBufferDirtyHint(buf, true);

	// TODO: gross. should we try and get the vmbuffer for opp pruning case?
	if (BufferIsValid(vmbuffer))
	{
		if (prstate.all_frozen)
		{
			visiflags |= VISIBILITYMAP_ALL_FROZEN;
			result->page_frozen = true;
		}

		if (prstate.all_visible)
		{
			visiflags |= VISIBILITYMAP_ALL_VISIBLE;
			result->page_all_visible = true;
		}

		if (!prstate.all_visible && all_visible_according_to_vm)
		{
			vmbits = visibilitymap_get_status(rel, blkno, &vmbuffer);
			all_visible_according_to_vm = vmbits & VISIBILITYMAP_ALL_VISIBLE;
			all_frozen_according_to_vm = vmbits & VISIBILITYMAP_ALL_FROZEN;
		}

		if ((prstate.all_visible && !all_visible_according_to_vm) ||
			(prstate.all_visible && prstate.all_frozen && !all_frozen_according_to_vm))
		{
			vm_modified = visibilitymap_set_and_lock(rel, blkno, buf,
					vmbuffer, visiflags);
			if (!vm_modified)
				LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);
		}
		else if ((!prstate.all_visible && all_visible_according_to_vm) ||
				(!prstate.all_frozen && all_frozen_according_to_vm))
		{
			uint8 clear_flags = VISIBILITYMAP_VALID_BITS;
			if (prstate.all_visible)
				clear_flags = VISIBILITYMAP_ALL_FROZEN;
			vm_modified = visibilitymap_clear_and_lock(rel, blkno,
					vmbuffer, clear_flags);
			if (!vm_modified)
				LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);
		}
	}
	else
		vm_modified = false;

	if (do_freeze)
	{
		Assert(prstate.nfrozen > 0);
		heap_freeze_execute_prepared(rel, buf, prstate.frozen, prstate.nfrozen);

		if (prstate.all_visible && prstate.all_frozen)
			frz_conflict_horizon = prstate.youngest_visible_xmin;
		else
		{
			frz_conflict_horizon = GlobalVisTestNonRemovableHorizon(vistest);
			if (frz_conflict_horizon > 0)
				TransactionIdRetreat(frz_conflict_horizon);
		}
	}

	if (RelationNeedsWAL(rel) &&
			(do_prune || vm_modified || do_freeze))
	{
		xl_heap_prune xlrec;
		XLogRecPtr	recptr;
		xl_heap_freeze_plan plans[MaxHeapTuplesPerPage];
		OffsetNumber frz_offsets[MaxHeapTuplesPerPage];

		xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(rel);
		xlrec.flags = visiflags;
		if (vm_modified && xlrec.isCatalogRel)
			xlrec.flags |= VISIBILITYMAP_XLOG_CATALOG_REL;

		if (do_prune && (vm_modified || do_freeze))
			xlrec.snapshotConflictHorizon = Max(prstate.youngest_xmax_reaped,
					frz_conflict_horizon);
		else if (do_prune)
			xlrec.snapshotConflictHorizon = prstate.youngest_xmax_reaped;
		else if (do_freeze)
			xlrec.snapshotConflictHorizon = frz_conflict_horizon;
		else
			xlrec.snapshotConflictHorizon = prstate.youngest_visible_xmin;

		xlrec.nplans = 0;
		if (do_freeze)
			xlrec.nplans =
				heap_log_freeze_plan(prstate.frozen, prstate.nfrozen, plans, frz_offsets);
		xlrec.nredirected = prstate.nredirected;
		xlrec.ndead = prstate.ndead;

		xlrec.nunused = prstate.nunused;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapPrune);
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

		if (vm_modified)
			XLogRegisterBuffer(1, vmbuffer, 0);

		if (xlrec.nplans > 0)
			XLogRegisterBufData(0, (char *) plans,
								xlrec.nplans * sizeof(xl_heap_freeze_plan));

		if (prstate.nredirected > 0)
			XLogRegisterBufData(0, (char *) prstate.redirected,
								prstate.nredirected *
								sizeof(OffsetNumber) * 2);

		if (prstate.ndead > 0)
			XLogRegisterBufData(0, (char *) prstate.nowdead,
								prstate.ndead * sizeof(OffsetNumber));

		if (prstate.nunused > 0)
			XLogRegisterBufData(0, (char *) prstate.nowunused,
								prstate.nunused * sizeof(OffsetNumber));

		if (xlrec.nplans > 0)
			XLogRegisterBufData(0, (char *) frz_offsets,
								prstate.nfrozen * sizeof(OffsetNumber));

		recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_PRUNE);

		PageSetLSN(page, recptr);

		if (vm_modified)
			PageSetLSN(BufferGetPage(vmbuffer), recptr);
	}

	if (vm_modified)
		LockBuffer(vmbuffer, BUFFER_LOCK_UNLOCK);

	END_CRIT_SECTION();

	return prstate.hastup;
}


HTSV_Result
heap_prune_satisfies_vacuum(PruneState *prstate, HeapTuple tup, Buffer buffer,
		OffsetNumber offnum)
{
	HTSV_Result res;
	TransactionId dead_after;
	TransactionId xmin;

	res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &dead_after);
	xmin = HeapTupleHeaderGetXmin(tup->t_data);

	if (res == HEAPTUPLE_RECENTLY_DEAD &&
			GlobalVisTestIsRemovableXid(prstate->vistest, dead_after))
	{
		res = HEAPTUPLE_DEAD;
	}

	if (res == HEAPTUPLE_LIVE &&
		HeapTupleHeaderXminCommitted(tup->t_data) &&
			GlobalVisTestIsRemovableXid(prstate->vistest, xmin))
	{
		res = HEAPTUPLE_LIVE_AND_VISIBLE;
	}

	switch (res)
	{
		case HEAPTUPLE_DEAD:
			break;
		case HEAPTUPLE_LIVE_AND_VISIBLE:
			prstate->result->live_tuples++;
			if (TransactionIdIsNormal(xmin) &&
				TransactionIdFollows(xmin, prstate->youngest_visible_xmin))
			{
				prstate->youngest_visible_xmin = xmin;
			}
			break;
		case HEAPTUPLE_LIVE:
			prstate->result->live_tuples++;
			prstate->all_visible = false;
			break;
		case HEAPTUPLE_RECENTLY_DEAD:
			prstate->result->recently_dead_tuples++;
			prstate->all_visible = false;
			break;
		case HEAPTUPLE_INSERT_IN_PROGRESS:
			prstate->all_visible = false;
			break;
		case HEAPTUPLE_DELETE_IN_PROGRESS:
			prstate->result->live_tuples++;
			prstate->all_visible = false;
			break;
		default:
			elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
	}

	return res;
}


/*
 * Prune specified line pointer or a HOT chain originating at line pointer.
 *
 * If the item is an index-referenced tuple (i.e. not a heap-only tuple),
 * the HOT chain is pruned by removing all DEAD tuples at the start of the HOT
 * chain.  We also prune any RECENTLY_DEAD tuples preceding a DEAD tuple.
 * This is OK because a RECENTLY_DEAD tuple preceding a DEAD tuple is really
 * DEAD, our visibility test is just too coarse to detect it.
 *
 * In general, pruning must never leave behind a DEAD tuple that still has
 * tuple storage.  VACUUM isn't prepared to deal with that case.  That's why
 * VACUUM prunes the same heap page a second time (without dropping its lock
 * in the interim) when it sees a newly DEAD tuple that we initially saw as
 * in-progress.  Retrying pruning like this can only happen when an inserting
 * transaction concurrently aborts.
 *
 * The root line pointer is redirected to the tuple immediately after the
 * latest DEAD tuple.  If all tuples in the chain are DEAD, the root line
 * pointer is marked LP_DEAD.  (This includes the case of a DEAD simple
 * tuple, which we treat as a chain of length 1.)
 *
 * We don't actually change the page here. We just add entries to the arrays in
 * prstate showing the changes to be made.  Items to be redirected are added
 * to the redirected[] array (two entries per redirection); items to be set to
 * LP_DEAD state are added to nowdead[]; and items to be set to LP_UNUSED
 * state are added to nowunused[].
 *
 * Returns the number of tuples (to be) deleted from the page.
 */
void
heap_prune_chain(Buffer buffer, OffsetNumber rootoffnum, PruneState *prstate,
		HeapPageFreeze *pagefrz, bool pronto_vac,
		VacDeadItems *dead_items, bool opportunistic)
{
	Page		dp = (Page) BufferGetPage(buffer);
	TransactionId priorXmax = InvalidTransactionId;
	ItemId		rootlp;
	HeapTupleHeader htup;
	OffsetNumber latestdead = InvalidOffsetNumber,
				maxoff = PageGetMaxOffsetNumber(dp),
				offnum;
	OffsetNumber chainitems[MaxHeapTuplesPerPage];
	int			nchain = 0,
				i;

	rootlp = PageGetItemId(dp, rootoffnum);

	/*
	 * If it's a heap-only tuple, then it is not the start of a HOT chain.
	 */

	if (ItemIdIsNormal(rootlp))
	{
		Assert(prstate->htsv[rootoffnum] != -1);
		htup = (HeapTupleHeader) PageGetItem(dp, rootlp);

		if (HeapTupleHeaderIsHeapOnly(htup))
		{
			/*
			 * If the tuple is DEAD and doesn't chain to anything else, mark
			 * it unused immediately.  (If it does chain, we can only remove
			 * it as part of pruning its chain.)
			 *
			 * We need this primarily to handle aborted HOT updates, that is,
			 * XMIN_INVALID heap-only tuples.  Those might not be linked to by
			 * any chain, since the parent tuple might be re-updated before
			 * any pruning occurs.  So we have to be able to reap them
			 * separately from chain-pruning.  (Note that
			 * HeapTupleHeaderIsHotUpdated will never return true for an
			 * XMIN_INVALID tuple, so this code will work even when there were
			 * sequential updates within the aborted transaction.)
			 *
			 * Note that we might first arrive at a dead heap-only tuple
			 * either here or while following a chain below.  Whichever path
			 * gets there first will mark the tuple unused.
			 */
			if (prstate->htsv[rootoffnum] == HEAPTUPLE_DEAD &&
				!HeapTupleHeaderIsHotUpdated(htup) && !prstate->marked[rootoffnum])
			{
				heap_prune_record_unused(prstate, rootoffnum);
				HeapTupleHeaderAdvanceConflictHorizon(htup,
													  &prstate->youngest_xmax_reaped);
			}

			return;
		}
	}

	if (prstate->marked[rootoffnum])
		return;

	/* Start from the root tuple */
	offnum = rootoffnum;

	/* while not end of the chain */
	for (;;)
	{
		ItemId		lp;
		bool		tupdead,
					recent_dead;

		/* Sanity check (pure paranoia) */
		if (offnum < FirstOffsetNumber)
			break;

		/*
		 * An offset past the end of page's line pointer array is possible
		 * when the array was truncated (original item must have been unused)
		 */
		if (offnum > maxoff)
			break;

		/* If item is already processed, stop --- it must not be same chain */
		if (prstate->marked[offnum])
			break;

		lp = PageGetItemId(dp, offnum);

		/* Unused item obviously isn't part of the chain */
		if (!ItemIdIsUsed(lp))
			break;

		/*
		 * If we are looking at the redirected root line pointer, jump to the
		 * first normal tuple in the chain.  If we find a redirect somewhere
		 * else, stop --- it must not be same chain.
		 */
		if (ItemIdIsRedirected(lp))
		{
			if (nchain > 0)
				break;			/* not at start of chain */
			chainitems[nchain++] = offnum;
			offnum = ItemIdGetRedirect(rootlp);
			continue;
		}

		/*
		 * Likewise, a dead line pointer can't be part of the chain. (We
		 * already eliminated the case of dead root tuple outside this
		 * function.)
		 */
		if (ItemIdIsDead(lp))
			break;

		Assert(ItemIdIsNormal(lp));
		Assert(prstate->htsv[offnum] != -1);
		htup = (HeapTupleHeader) PageGetItem(dp, lp);

		/*
		 * Check the tuple XMIN against prior XMAX, if any
		 */
		if (TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
			break;

		/*
		 * OK, this tuple is indeed a member of the chain.
		 */
		chainitems[nchain++] = offnum;

		/*
		 * Check tuple's visibility status.
		 */
		tupdead = recent_dead = false;

		switch ((HTSV_Result) prstate->htsv[offnum])
		{
			case HEAPTUPLE_DEAD:
				tupdead = true;
				break;

			case HEAPTUPLE_RECENTLY_DEAD:
				recent_dead = true;

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				break;

			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				break;

			case HEAPTUPLE_LIVE:
			case HEAPTUPLE_LIVE_AND_VISIBLE:
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * If we wanted to optimize for aborts, we might consider
				 * marking the page prunable when we see INSERT_IN_PROGRESS.
				 * But we don't.  See related decisions about when to mark the
				 * page prunable in heapam.c.
				 */
				break;

			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}

		/*
		 * Remember the last DEAD tuple seen.  We will advance past
		 * RECENTLY_DEAD tuples just in case there's a DEAD one after them;
		 * but we can't advance past anything else.  We have to make sure that
		 * we don't miss any DEAD tuples, since DEAD tuples that still have
		 * tuple storage after pruning will confuse VACUUM.
		 */
		if (tupdead)
		{
			latestdead = offnum;
			HeapTupleHeaderAdvanceConflictHorizon(htup,
												  &prstate->youngest_xmax_reaped);
		}
		else if (!recent_dead)
			break;

		/*
		 * If the tuple is not HOT-updated, then we are at the end of this
		 * HOT-update chain.
		 */
		if (!HeapTupleHeaderIsHotUpdated(htup))
			break;

		/* HOT implies it can't have moved to different partition */
		Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

		/*
		 * Advance to next chain member.
		 */
		Assert(ItemPointerGetBlockNumber(&htup->t_ctid) ==
			   BufferGetBlockNumber(buffer));
		offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
		priorXmax = HeapTupleHeaderGetUpdateXid(htup);
	}

	/*
	 * If we found a DEAD tuple in the chain, adjust the HOT chain so that all
	 * the DEAD tuples at the start of the chain are removed and the root line
	 * pointer is appropriately redirected.
	 */
	if (OffsetNumberIsValid(latestdead))
	{
		/*
		 * Mark as unused each intermediate item that we are able to remove
		 * from the chain.
		 *
		 * When the previous item is the last dead tuple seen, we are at the
		 * right candidate for redirection.
		 */
		for (i = 1; (i < nchain) && (chainitems[i - 1] != latestdead); i++)
		{
			heap_prune_record_unused(prstate, chainitems[i]);
		}

		/*
		 * If the DEAD tuple is at the end of the chain, the entire chain is
		 * dead and the root line pointer can be marked dead.  Otherwise just
		 * redirect the root to the correct chain member.
		 */
		if (i >= nchain)
		{
			if (pronto_vac)
				heap_prune_record_unused(prstate, rootoffnum);
			else
				heap_prune_record_dead(prstate, rootoffnum, dead_items);
		}
		else
			heap_prune_record_redirect(prstate, dp, pagefrz, rootoffnum,
					chainitems[i], opportunistic);
	}
	else if (nchain < 2 && ItemIdIsRedirected(rootlp))
	{
		/*
		 * We found a redirect item that doesn't point to a valid follow-on
		 * item.  This can happen if the loop in heap_page_prune caused us to
		 * visit the dead successor of a redirect item before visiting the
		 * redirect item.  We can clean up by setting the redirect item to
		 * DEAD state.
		 */
		if (pronto_vac)
			heap_prune_record_unused(prstate, rootoffnum);
		else
			heap_prune_record_dead(prstate, rootoffnum, dead_items);
	}

	// TODO: I think this misses HOT tuples which are not redirected to and not dead
	if (!prstate->marked[rootoffnum])
		prstate->hastup = true;
}

/* Record lowest soon-prunable XID */
static void
heap_prune_record_prunable(PruneState *prstate, TransactionId xid)
{
	/*
	 * This should exactly match the PageSetPrunable macro.  We can't store
	 * directly into the page header yet, so we update working state.
	 */
	Assert(TransactionIdIsNormal(xid));
	if (!TransactionIdIsValid(prstate->new_prune_xid) ||
		TransactionIdPrecedes(xid, prstate->new_prune_xid))
		prstate->new_prune_xid = xid;
}

/* Record line pointer to be redirected */
static void
heap_prune_record_redirect(PruneState *prstate, Page page, HeapPageFreeze *pagefrz,
						   OffsetNumber offnum, OffsetNumber rdoffnum, bool opportunistic)
{
	HeapTupleHeader htup;
	ItemId itemid;
	bool tuple_frozen;

	Assert(prstate->nredirected < MaxHeapTuplesPerPage);
	prstate->redirected[prstate->nredirected * 2] = offnum;
	prstate->redirected[prstate->nredirected * 2 + 1] = rdoffnum;
	prstate->nredirected++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
	Assert(!prstate->marked[rdoffnum]);
	prstate->marked[rdoffnum] = true;
	prstate->hastup = true;
	if (prstate->frz_attempted[rdoffnum] ||
		prstate->htsv[rdoffnum] == HEAPTUPLE_DEAD ||
		prstate->htsv[rdoffnum] == -1)
		return;

	itemid = PageGetItemId(page, rdoffnum);
	htup = (HeapTupleHeader) PageGetItem(page, itemid);
	if ((heap_prepare_freeze_tuple(htup, pagefrz,
								&prstate->frozen[prstate->nfrozen], &tuple_frozen,
								prstate->vistest, opportunistic)))
	{
		prstate->frozen[prstate->nfrozen++].offset = rdoffnum;
	}

	prstate->frz_attempted[rdoffnum] = true;
	prstate->all_frozen = prstate->all_frozen && tuple_frozen;
}

/* Record line pointer to be marked dead */
static void
heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum, VacDeadItems *dead_items)
{
	Assert(prstate->ndead < MaxHeapTuplesPerPage);
	prstate->nowdead[prstate->ndead] = offnum;
	prstate->ndead++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
	prstate->all_visible = false;
	catalog_dead_item_for_vacuum(dead_items, prstate->blkno, offnum);
}

/* Record line pointer to be marked unused */
static void
heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->nunused < MaxHeapTuplesPerPage);
	prstate->nowunused[prstate->nunused] = offnum;
	prstate->nunused++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
}


/*
 * Perform the actual page changes needed by heap_page_prune.
 * It is expected that the caller has a full cleanup lock on the
 * buffer.
 */
void
heap_page_prune_execute(Buffer buffer,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	Page		page = (Page) BufferGetPage(buffer);
	OffsetNumber *offnum;
	HeapTupleHeader htup PG_USED_FOR_ASSERTS_ONLY;

	/* Shouldn't be called unless there's something to do */
	Assert(nredirected > 0 || ndead > 0 || nunused > 0);

	/* Update all redirected line pointers */
	offnum = redirected;
	for (int i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);
		ItemId		tolp PG_USED_FOR_ASSERTS_ONLY;

#ifdef USE_ASSERT_CHECKING

		/*
		 * Any existing item that we set as an LP_REDIRECT (any 'from' item)
		 * must be the first item from a HOT chain.  If the item has tuple
		 * storage then it can't be a heap-only tuple.  Otherwise we are just
		 * maintaining an existing LP_REDIRECT from an existing HOT chain that
		 * has been pruned at least once before now.
		 */
		if (!ItemIdIsRedirected(fromlp))
		{
			Assert(ItemIdHasStorage(fromlp) && ItemIdIsNormal(fromlp));

			htup = (HeapTupleHeader) PageGetItem(page, fromlp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* We shouldn't need to redundantly set the redirect */
			Assert(ItemIdGetRedirect(fromlp) != tooff);
		}

		/*
		 * The item that we're about to set as an LP_REDIRECT (the 'from'
		 * item) will point to an existing item (the 'to' item) that is
		 * already a heap-only tuple.  There can be at most one LP_REDIRECT
		 * item per HOT chain.
		 *
		 * We need to keep around an LP_REDIRECT item (after original
		 * non-heap-only root tuple gets pruned away) so that it's always
		 * possible for VACUUM to easily figure out what TID to delete from
		 * indexes when an entire HOT chain becomes dead.  A heap-only tuple
		 * can never become LP_DEAD; an LP_REDIRECT item or a regular heap
		 * tuple can.
		 *
		 * This check may miss problems, e.g. the target of a redirect could
		 * be marked as unused subsequently. The page_verify_redirects() check
		 * below will catch such problems.
		 */
		tolp = PageGetItemId(page, tooff);
		Assert(ItemIdHasStorage(tolp) && ItemIdIsNormal(tolp));
		htup = (HeapTupleHeader) PageGetItem(page, tolp);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
#endif

		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (int i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		/*
		 * An LP_DEAD line pointer must be left behind when the original item
		 * (which is dead to everybody) could still be referenced by a TID in
		 * an index.  This should never be necessary with any individual
		 * heap-only tuple item, though. (It's not clear how much of a problem
		 * that would be, but there is no reason to allow it.)
		 */
		if (ItemIdHasStorage(lp))
		{
			Assert(ItemIdIsNormal(lp));
			htup = (HeapTupleHeader) PageGetItem(page, lp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* Whole HOT chain becomes dead */
			Assert(ItemIdIsRedirected(lp));
		}
#endif

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (int i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);
		ItemIdSetUnused(lp);
	}

	/*
	 * Finally, repair any fragmentation, and update the page's hint bit about
	 * whether it has free pointers.
	 */
	PageRepairFragmentation(page);

	/*
	 * Now that the page has been modified, assert that redirect items still
	 * point to valid targets.
	 */
	page_verify_redirects(page);
}


/*
 * If built with assertions, verify that all LP_REDIRECT items point to a
 * valid item.
 *
 * One way that bugs related to HOT pruning show is redirect items pointing to
 * removed tuples. It's not trivial to reliably check that marking an item
 * unused will not orphan a redirect item during heap_prune_chain() /
 * heap_page_prune_execute(), so we additionally check the whole page after
 * pruning. Without this check such bugs would typically only cause asserts
 * later, potentially well after the corruption has been introduced.
 *
 * Also check comments in heap_page_prune_execute()'s redirection loop.
 */
static void
page_verify_redirects(Page page)
{
#ifdef USE_ASSERT_CHECKING
	OffsetNumber offnum;
	OffsetNumber maxoff;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		OffsetNumber targoff;
		ItemId		targitem;
		HeapTupleHeader htup;

		if (!ItemIdIsRedirected(itemid))
			continue;

		targoff = ItemIdGetRedirect(itemid);
		targitem = PageGetItemId(page, targoff);

		Assert(ItemIdIsUsed(targitem));
		Assert(ItemIdIsNormal(targitem));
		Assert(ItemIdHasStorage(targitem));
		htup = (HeapTupleHeader) PageGetItem(page, targitem);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
	}
#endif
}


/*
 * For all items in this page, find their respective root line pointers.
 * If item k is part of a HOT-chain with root at item j, then we set
 * root_offsets[k - 1] = j.
 *
 * The passed-in root_offsets array must have MaxHeapTuplesPerPage entries.
 * Unused entries are filled with InvalidOffsetNumber (zero).
 *
 * The function must be called with at least share lock on the buffer, to
 * prevent concurrent prune operations.
 *
 * Note: The information collected here is valid only as long as the caller
 * holds a pin on the buffer. Once pin is released, a tuple might be pruned
 * and reused by a completely unrelated tuple.
 */
void
heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	OffsetNumber offnum,
				maxoff;

	MemSet(root_offsets, InvalidOffsetNumber,
		   MaxHeapTuplesPerPage * sizeof(OffsetNumber));

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
	{
		ItemId		lp = PageGetItemId(page, offnum);
		HeapTupleHeader htup;
		OffsetNumber nextoffnum;
		TransactionId priorXmax;

		/* skip unused and dead items */
		if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
			continue;

		if (ItemIdIsNormal(lp))
		{
			htup = (HeapTupleHeader) PageGetItem(page, lp);

			/*
			 * Check if this tuple is part of a HOT-chain rooted at some other
			 * tuple. If so, skip it for now; we'll process it when we find
			 * its root.
			 */
			if (HeapTupleHeaderIsHeapOnly(htup))
				continue;

			/*
			 * This is either a plain tuple or the root of a HOT-chain.
			 * Remember it in the mapping.
			 */
			root_offsets[offnum - 1] = offnum;

			/* If it's not the start of a HOT-chain, we're done with it */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				continue;

			/* Set up to scan the HOT-chain */
			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
		else
		{
			/* Must be a redirect item. We do not set its root_offsets entry */
			Assert(ItemIdIsRedirected(lp));
			/* Set up to scan the HOT-chain */
			nextoffnum = ItemIdGetRedirect(lp);
			priorXmax = InvalidTransactionId;
		}

		/*
		 * Now follow the HOT-chain and collect other tuples in the chain.
		 *
		 * Note: Even though this is a nested loop, the complexity of the
		 * function is O(N) because a tuple in the page should be visited not
		 * more than twice, once in the outer loop and once in HOT-chain
		 * chases.
		 */
		for (;;)
		{
			/* Sanity check (pure paranoia) */
			if (offnum < FirstOffsetNumber)
				break;

			/*
			 * An offset past the end of page's line pointer array is possible
			 * when the array was truncated
			 */
			if (offnum > maxoff)
				break;

			lp = PageGetItemId(page, nextoffnum);

			/* Check for broken chains */
			if (!ItemIdIsNormal(lp))
				break;

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			if (TransactionIdIsValid(priorXmax) &&
				!TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(htup)))
				break;

			/* Remember the root line pointer for this item */
			root_offsets[nextoffnum - 1] = offnum;

			/* Advance to next chain member, if any */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				break;

			/* HOT implies it can't have moved to different partition */
			Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
	}
}
