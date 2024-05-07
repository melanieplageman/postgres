/*-------------------------------------------------------------------------
 *
 * procarray.h
 *	  POSTGRES process array definitions.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/procarray.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCARRAY_H
#define PROCARRAY_H

#include "storage/lock.h"
#include "storage/standby.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"


/*
 * State for the GlobalVisTest* family of functions. Those functions can
 * e.g. be used to decide if a deleted row can be removed without violating
 * MVCC semantics: If the deleted row's xmax is not considered to be running
 * by anyone, the row can be removed.
 *
 * To avoid slowing down GetSnapshotData(), we don't calculate a precise
 * cutoff XID while building a snapshot (looking at the frequently changing
 * xmins scales badly). Instead we compute two boundaries while building the
 * snapshot:
 *
 * 1) definitely_needed, indicating that rows deleted by XIDs >=
 *    definitely_needed are definitely still visible.
 *
 * 2) maybe_needed, indicating that rows deleted by XIDs < maybe_needed can
 *    definitely be removed
 *
 * When testing an XID that falls in between the two (i.e. XID >= maybe_needed
 * && XID < definitely_needed), the boundaries can be recomputed (using
 * ComputeXidHorizons()) to get a more accurate answer. This is cheaper than
 * maintaining an accurate value all the time.
 *
 * As it is not cheap to compute accurate boundaries, we limit the number of
 * times that happens in short succession. See GlobalVisTestShouldUpdate().
 *
 *
 * There are three backend lifetime instances of this struct, optimized for
 * different types of relations. As e.g. a normal user defined table in one
 * database is inaccessible to backends connected to another database, a test
 * specific to a relation can be more aggressive than a test for a shared
 * relation.  Currently we track four different states:
 *
 * 1) GlobalVisSharedRels, which only considers an XID's
 *    effects visible-to-everyone if neither snapshots in any database, nor a
 *    replication slot's xmin, nor a replication slot's catalog_xmin might
 *    still consider XID as running.
 *
 * 2) GlobalVisCatalogRels, which only considers an XID's
 *    effects visible-to-everyone if neither snapshots in the current
 *    database, nor a replication slot's xmin, nor a replication slot's
 *    catalog_xmin might still consider XID as running.
 *
 *    I.e. the difference to GlobalVisSharedRels is that
 *    snapshot in other databases are ignored.
 *
 * 3) GlobalVisDataRels, which only considers an XID's
 *    effects visible-to-everyone if neither snapshots in the current
 *    database, nor a replication slot's xmin consider XID as running.
 *
 *    I.e. the difference to GlobalVisCatalogRels is that
 *    replication slot's catalog_xmin is not taken into account.
 *
 * 4) GlobalVisTempRels, which only considers the current session, as temp
 *    tables are not visible to other sessions.
 *
 * GlobalVisTestFor(relation) returns the appropriate state
 * for the relation.
 *
 * The boundaries are FullTransactionIds instead of TransactionIds to avoid
 * wraparound dangers. There e.g. would otherwise exist no procarray state to
 * prevent maybe_needed to become old enough after the GetSnapshotData()
 * call.
 *
 * The typedef is in the header.
 */
typedef struct GlobalVisState
{
	/* XIDs >= are considered running by some backend */
	FullTransactionId definitely_needed;

	/* XIDs < are not considered to be running by any backend */
	FullTransactionId maybe_needed;
} GlobalVisState;

extern Size ProcArrayShmemSize(void);
extern void CreateSharedProcArray(void);
extern void ProcArrayAdd(PGPROC *proc);
extern void ProcArrayRemove(PGPROC *proc, TransactionId latestXid);

extern void ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid);
extern void ProcArrayClearTransaction(PGPROC *proc);

extern void ProcArrayInitRecovery(TransactionId initializedUptoXID);
extern void ProcArrayApplyRecoveryInfo(RunningTransactions running);
extern void ProcArrayApplyXidAssignment(TransactionId topxid,
										int nsubxids, TransactionId *subxids);

extern void RecordKnownAssignedTransactionIds(TransactionId xid);
extern void ExpireTreeKnownAssignedTransactionIds(TransactionId xid,
												  int nsubxids, TransactionId *subxids,
												  TransactionId max_xid);
extern void ExpireAllKnownAssignedTransactionIds(void);
extern void ExpireOldKnownAssignedTransactionIds(TransactionId xid);
extern void KnownAssignedTransactionIdsIdleMaintenance(void);

extern int	GetMaxSnapshotXidCount(void);
extern int	GetMaxSnapshotSubxidCount(void);

extern Snapshot GetSnapshotData(Snapshot snapshot);

extern bool ProcArrayInstallImportedXmin(TransactionId xmin,
										 VirtualTransactionId *sourcevxid);
extern bool ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc);

extern RunningTransactions GetRunningTransactionData(void);

extern bool TransactionIdIsInProgress(TransactionId xid);
extern bool TransactionIdIsActive(TransactionId xid);
extern TransactionId GetOldestNonRemovableTransactionId(Relation rel);

extern void GetThisGlobalVis(GlobalVisState *src, GlobalVisState *result);
extern void GetSharedGlobalVis(GlobalVisState *vis);
extern TransactionId GetOldestNonRemovableTransactionIdAll(void);
extern TransactionId GetOldestTransactionIdConsideredRunning(void);
extern TransactionId GetOldestActiveTransactionId(void);
extern TransactionId GetOldestSafeDecodingTransactionId(bool catalogOnly);
extern void GetReplicationHorizons(TransactionId *xmin, TransactionId *catalog_xmin);

extern VirtualTransactionId *GetVirtualXIDsDelayingChkpt(int *nvxids, int type);
extern bool HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids,
										 int nvxids, int type);

extern PGPROC *ProcNumberGetProc(int procNumber);
extern void ProcNumberGetTransactionIds(int procNumber, TransactionId *xid,
										TransactionId *xmin, int *nsubxid,
										bool *overflowed);
extern PGPROC *BackendPidGetProc(int pid);
extern PGPROC *BackendPidGetProcWithLock(int pid);
extern int	BackendXidGetPid(TransactionId xid);
extern bool IsBackendPid(int pid);

extern VirtualTransactionId *GetCurrentVirtualXIDs(TransactionId limitXmin,
												   bool excludeXmin0, bool allDbs, int excludeVacuum,
												   int *nvxids);
extern VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid);
extern pid_t CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode);
extern pid_t SignalVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode,
									  bool conflictPending);

extern bool MinimumActiveBackends(int min);
extern int	CountDBBackends(Oid databaseid);
extern int	CountDBConnections(Oid databaseid);
extern void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending);
extern int	CountUserBackends(Oid roleid);
extern bool CountOtherDBBackends(Oid databaseId,
								 int *nbackends, int *nprepared);
extern void TerminateOtherDBBackends(Oid databaseId);

extern void XidCacheRemoveRunningXids(TransactionId xid,
									  int nxids, const TransactionId *xids,
									  TransactionId latestXid);

extern void ProcArraySetReplicationSlotXmin(TransactionId xmin,
											TransactionId catalog_xmin, bool already_locked);

extern void ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
											TransactionId *catalog_xmin);

#endif							/* PROCARRAY_H */
