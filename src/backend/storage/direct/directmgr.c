/*-------------------------------------------------------------------------
 *
 * directmgr.c
 *	  routines for managing unbuffered IO
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/direct/directmgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include "access/xlog.h"
#include "storage/directmgr.h"
#include "utils/rel.h"

void unbuffered_prep(UnBufferedWriteState *wstate)
{
	wstate->redo = GetRedoRecPtr();
}

/*
 * When writing data outside shared buffers, a concurrent CHECKPOINT can move
 * the redo pointer past our WAL entries and won't flush our data to disk. If
 * the database crashes before the data makes it to disk, our WAL won't be
 * replayed and the data will be lost.
 * Thus, if a CHECKPOINT begins between unbuffered_prep() and
 * unbuffered_finish(), the backend must fsync the data itself.
 */
void unbuffered_finish(UnBufferedWriteState *wstate, ForkNumber forknum)
{
	if (RedoRecPtrChanged(wstate->redo))
		smgrimmedsync(wstate->smgr_rel, forknum);
}

void
unbuffered_write(UnBufferedWriteState *wstate, ForkNumber forknum, BlockNumber blocknum, Page page)
{
	PageSetChecksumInplace(page, blocknum);
	smgrwrite(wstate->smgr_rel, forknum, blocknum, (char *) page, false);
}

void
unbuffered_extend(UnBufferedWriteState *wstate, ForkNumber forknum, BlockNumber blocknum, Page page, bool empty)
{
	if (!empty)
		PageSetChecksumInplace(page, blocknum);
	smgrextend(wstate->smgr_rel, forknum, blocknum, (char *) page, false);
}

