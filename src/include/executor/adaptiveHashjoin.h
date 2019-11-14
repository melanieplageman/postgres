#ifndef ADAPTIVEHASHJOIN_H 
#define ADAPTIVEHASHJOIN_H 


extern bool ExecParallelHashJoinNewChunk(HashJoinState *hjstate, bool advance_from_probing);
extern bool ExecParallelHashJoinNewBatch(HashJoinState *hjstate);
extern void combine_outer_match_statuses(BufFile *outer_match_statuses[], int length, size_t num_bytes,
												int batchno, BufFile **combined_bitmap_file);

#endif							/* ADAPTIVEHASHJOIN_H */
