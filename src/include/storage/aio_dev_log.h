#include "executor/instrument.h"

typedef enum PgsrIOEvent
{
	PGSR_IO_EVENT_SUBMISSION,
	PGSR_IO_EVENT_WAIT,
	PGSR_IO_EVENT_COMPLETION,
	PGSR_IO_EVENT_CONSUMPTION,
	PGSR_IO_EVENT_NUM_TYPES
} PgsrIOEvent;

typedef enum PgsrMetric
{
	PGSR_METRIC_AVG_TPUT,
	PGSR_METRIC_LATENCY,
	PGSR_METRIC_PFD,
	PGSR_METRIC_TOTAL_SUBMITTED,
	PGSR_METRIC_INFLIGHT,
	PGSR_METRIC_CNC,
	PGSR_METRIC_TOTAL_COMPLETED,
	PGSR_METRIC_TOTAL_CONSUMED,
	PGSR_METRIC_TOTAL_REAL_CONSUMED,
	PGSR_METRIC_WAIT,
} PgsrMetric;

#define PGSR_METRIC_NUM_TYPES (PGSR_METRIC_WAIT + 1)

typedef struct PgsrLogItem
{
	PgsrIOEvent event;
	instr_time time;
	int16 io_id;
	PgsrMetric metric;
	float value;
} PgsrLogItem;

typedef struct PgsrLog
{
	int length;
	int max_length;
	PgsrLogItem data[FLEXIBLE_ARRAY_MEMBER];
} PgsrLog;

extern void
aio_dev_log_metric(PgsrLog *log, instr_time time, PgsrIOEvent event, int16 io_id, PgsrMetric metric, float value);

extern PgsrLog *
aio_dev_make_metric_log(int max_length);

extern void
aio_dev_write_log(PgsrLog *log, int max_prefetch_distance);
