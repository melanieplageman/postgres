#include "postgres.h"

#include <unistd.h>

#include "executor/instrument.h"
#include "storage/aio_dev_log.h"
#include "storage/fd.h"
#include "utils/guc.h"

PgsrLog *
aio_dev_make_metric_log(int max_length)
{
	PgsrLog *log =
		palloc0((offsetof(PgsrLog, data) +
				sizeof(PgsrLogItem) * max_length));

	log->length = 0;
	log->max_length = max_length;
	return log;
}

void
aio_dev_log_metric(PgsrLog *log, instr_time time, PgsrIOEvent event, int16 io_id, PgsrMetric metric, float value)
{
	PgsrLogItem *entry;

	Assert(log);
	entry = &log->data[log->length];
	entry->event = event;
	entry->io_id = io_id;
	entry->time = time;
	entry->metric = metric;
	entry->value = value;
	log->length++;
	Assert(log->length <= log->max_length);
}


static bool
aio_dev_allocate_logfile(const char* logfile, FILE **fpout)
{
	*fpout = AllocateFile(logfile, "w");

	if ((*fpout) == NULL)
	{
		ereport(LOG,
					(errcode_for_file_access(),
					errmsg("Could not allocate PGSR logfile \"%s\": %m",
									logfile)));
		return false;
	}

	return true;
}

static void
aio_dev_cleanup_logfile(const char* logfile, FILE *fpout)
{
	if (ferror(fpout))
	{
		ereport(LOG,
					(errcode_for_file_access(),
					errmsg("Could not access PGSR logfile \"%s\": %m",
									logfile)));
		FreeFile(fpout);
		unlink(logfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
						(errcode_for_file_access(),
						errmsg("Could not free PGSR logfile \"%s\": %m",
										logfile)));
		unlink(logfile);
	}
}

static float
aio_dev_format_value(float value, PgsrMetric metric)
{
	switch (metric)
	{
		case PGSR_METRIC_LATENCY:
		case PGSR_METRIC_WAIT:
			return INSTR_TIME_GET_MICROSEC(value);
		case PGSR_METRIC_AVG_TPUT:
		case PGSR_METRIC_PFD:
		case PGSR_METRIC_TOTAL_SUBMITTED:
		case PGSR_METRIC_INFLIGHT:
		case PGSR_METRIC_CNC:
		case PGSR_METRIC_TOTAL_COMPLETED:
		case PGSR_METRIC_TOTAL_CONSUMED:
		case PGSR_METRIC_TOTAL_REAL_CONSUMED:
			return value;
	}

	elog(ERROR, "unknown metric type: %d", metric);
}

void
aio_dev_write_log(PgsrLog *log, int max_prefetch_distance)
{
	FILE * logfile;
	char *log_dir = "/tmp/pgsr_pfd";
	char filename[MAXPGPATH];

	snprintf(filename, MAXPGPATH, "%s/%s_%s", log_dir, "metric_log", application_name);

	if (log->length <= 0 ||
			!aio_dev_allocate_logfile(filename, &logfile))
		return;

	fprintf(logfile, "event,metric,io_id,time,value\n");

	for (int i = 0; i < log->length; i++)
	{
		PgsrLogItem *entry = &log->data[i];

		fprintf(logfile,
				"%d,%d,%d,%lf,%lf\n",
				entry->event,
				entry->metric,
				entry->io_id,
				INSTR_TIME_GET_MICROSEC(entry->time),
				aio_dev_format_value(entry->value, entry->metric)
			);
	}

	aio_dev_cleanup_logfile(filename, logfile);
}
