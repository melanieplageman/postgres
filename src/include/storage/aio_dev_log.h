#include "executor/instrument.h"

/* This will not be used once I am done debugging */
typedef struct PgStreamingReadCompletionLogItem
{
	instr_time submission_time;
	instr_time completion_time;
	/* prefetch distance at time IO was submitted */
	int prefetch_distance;
	/* CNC at time IO was completed */
	uint32     cnc;
	/* # inflight at time IO was completed */
	int32 inflight;
} PgStreamingReadCompletionLogItem;

typedef struct PgStreamingReadWaitLogItem
{
	instr_time wait_start;
	instr_time wait_end;
} PgStreamingReadWaitLogItem;

typedef struct PgStreamingReadCompletionLog
{
	int length;
	int max_length;
	PgStreamingReadCompletionLogItem data[FLEXIBLE_ARRAY_MEMBER];
} PgStreamingReadCompletionLog;

typedef struct PgStreamingReadWaitLog
{
	int length;
	int max_length;
	PgStreamingReadWaitLogItem data[FLEXIBLE_ARRAY_MEMBER];
} PgStreamingReadWaitLog;

typedef struct PgStreamingReadConsumptionLog
{
	int length;
	int max_length;
	instr_time data[FLEXIBLE_ARRAY_MEMBER];
} PgStreamingReadConsumptionLog;

typedef struct PgStreamingReadDevLog
{
	PgStreamingReadWaitLog        *wait_log;
	PgStreamingReadCompletionLog    *completion_log;
	PgStreamingReadConsumptionLog  *consumption_log;
} PgStreamingReadDevLog;

extern void
aio_dev_log_wait(PgStreamingReadWaitLog *log, instr_time wait_start, instr_time wait_end);

extern void
aio_dev_log_consumption(PgStreamingReadConsumptionLog *log, instr_time consumption_time);

extern void
aio_dev_log_completion(PgStreamingReadCompletionLog *log, instr_time
		submission_time, instr_time completion_time, int prefetch_distance, int cnc, int inflight);

extern PgStreamingReadWaitLog *
aio_dev_make_wait_log(int max_length);

extern PgStreamingReadConsumptionLog *
aio_dev_make_consumption_log(int max_length);

extern PgStreamingReadCompletionLog *
aio_dev_make_completion_log(int max_length);

extern bool
aio_dev_allocate_logfile(const char* logfile, FILE **fpout);

extern void 
aio_dev_cleanup_logfile(const char* logfile, FILE *fpout);

extern void
aio_dev_write_wait_log(PgStreamingReadWaitLog *log, FILE *logfile);

extern void
aio_dev_write_completion_log(PgStreamingReadCompletionLog *log, FILE *logfile, int max_inflight, int max_pfd);

extern void
aio_dev_write_consumption_log(PgStreamingReadConsumptionLog *log, FILE *logfile);

extern void
aio_dev_write_log(PgStreamingReadDevLog *dev_log, int max_inflight, int max_pfd);
