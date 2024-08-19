/*-------------------------------------------------------------------------
 *
 * lsntimedesc.c
 *	  rmgr descriptor routines for LSNTimeStream in pgstat_wal.c
 *
 * Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/lsntimedesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogdefs.h"
#include "access/xact.h"
#include "pgstat.h"
#include "utils/timestamp.h"


/*
 * String representation of an LSNTime record.
 */
void
lsntime_desc(StringInfo buf, XLogReaderState *record)
{
	uint8		info PG_USED_FOR_ASSERTS_ONLY;
	xl_lsntime *lsnt_rec;

	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	Assert(info == XLOG_LSNTIME);

	lsnt_rec = (xl_lsntime *) XLogRecGetData(record);

	appendStringInfo(buf, "%X/%X, %s",
			LSN_FORMAT_ARGS(lsnt_rec->lsn),
			timestamptz_to_str(lsnt_rec->time));
}

const char *
lsntime_identify(uint8 info)
{
	Assert(info == XLOG_LSNTIME);
	return "LSNTIME";
}

