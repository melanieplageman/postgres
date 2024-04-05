/*-------------------------------------------------------------------------
 *
 * nodeBitmapHeapscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeBitmapHeapscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEBITMAPHEAPSCAN_H
#define NODEBITMAPHEAPSCAN_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern BitmapHeapScanState *ExecInitBitmapHeapScan(BitmapHeapScan *node, EState *estate, int eflags);
extern void ExecEndBitmapHeapScan(BitmapHeapScanState *node);
extern void ExecReScanBitmapHeapScan(BitmapHeapScanState *node);
extern void ExecBitmapHeapEstimate(BitmapHeapScanState *node,
								   ParallelContext *pcxt);
extern void ExecBitmapHeapInitializeDSM(BitmapHeapScanState *node,
										ParallelContext *pcxt);
extern void ExecBitmapHeapReInitializeDSM(BitmapHeapScanState *node,
										  ParallelContext *pcxt);
extern void ExecBitmapHeapInitializeWorker(BitmapHeapScanState *node,
										   ParallelWorkerContext *pwcxt);
typedef struct BitmapHeapIterator
{
	struct TBMIterator *serial;
	struct TBMSharedIterator *parallel;
	bool		exhausted;
} BitmapHeapIterator;

extern void bhs_iterate(BitmapHeapIterator *iterator, TBMIterateResult *result);
extern void bhs_begin_iterate(BitmapHeapIterator *iterator, TIDBitmap *tbm,
							  dsa_area *dsa, dsa_pointer dsp);
extern void bhs_end_iterate(BitmapHeapIterator *iterator);


#endif							/* NODEBITMAPHEAPSCAN_H */
