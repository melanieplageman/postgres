#ifndef ADAPTIVE_HASHJOIN_H
#define ADAPTIVE_HASHJOIN_H


extern bool ExecParallelHashJoinNewChunk(HashJoinState *hjstate, bool advance_from_probing);
extern bool ExecParallelHashJoinNewBatch(HashJoinState *hjstate);

#endif /* ADAPTIVE_HASHJOIN_H */
