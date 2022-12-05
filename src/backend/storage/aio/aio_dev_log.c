#include "postgres.h"

#include <unistd.h>

#include "executor/instrument.h"
#include "storage/aio_dev_log.h"
#include "storage/fd.h"

#define PGSR_PFD_COMPLETION_LOGFILE "/tmp/pgsr_pfd_completion_log"
#define PGSR_PFD_WAIT_LOGFILE "/tmp/pgsr_pfd_wait_log"
#define PGSR_PFD_CONSUMPTION_LOGFILE "/tmp/pgsr_pfd_consumption_log"

PgStreamingReadWaitLog *
aio_dev_make_wait_log(int max_length)
{
	PgStreamingReadWaitLog *log = palloc0(offsetof(PgStreamingReadWaitLog, data) +
			sizeof(PgStreamingReadWaitLogItem) * max_length);

	log->length = 0;
	log->max_length = max_length;

	return log;
}

PgStreamingReadConsumptionLog *
aio_dev_make_consumption_log(int max_length)
{
	PgStreamingReadConsumptionLog *log =
		palloc0(offsetof(PgStreamingReadConsumptionLog, data) +
				sizeof(instr_time) * max_length);

		log->length     = 0;
		log->max_length = max_length;
		return log;
}

PgStreamingReadCompletionLog *
aio_dev_make_completion_log(int max_length)
{
	PgStreamingReadCompletionLog *log =
		palloc0(offsetof(PgStreamingReadCompletionLog, data) +
				sizeof(PgStreamingReadCompletionLogItem) * max_length);

	log->length     = 0;
	log->max_length = max_length;

	return log;
}

void
aio_dev_log_wait(PgStreamingReadWaitLog *log, instr_time wait_start, instr_time wait_end)
{
	PgStreamingReadWaitLogItem *entry;

	Assert(log);
	entry = &log->data[log->length];
	entry->wait_start = wait_start;
	entry->wait_end = wait_end;
	log->length++;
	Assert(log->length <= log->max_length);
}

void
aio_dev_log_consumption(PgStreamingReadConsumptionLog *log, instr_time consumption_time)
{
	Assert(log);
	log->data[log->length] = consumption_time;
	log->length++;
	Assert(log->length <= log->max_length);
}

void
aio_dev_log_completion(PgStreamingReadCompletionLog *log, instr_time
		submission_time, instr_time completion_time, int prefetch_distance, int cnc, int inflight)
{
	PgStreamingReadCompletionLogItem *entry;

	Assert(log);
	entry = &log->data[log->length];
	entry->submission_time = submission_time;
	entry->completion_time = completion_time;
	entry->prefetch_distance = prefetch_distance;
	entry->cnc = cnc;
	entry->inflight = inflight;

	log->length++;
	Assert(log->length <= log->max_length);
}

bool
aio_dev_allocate_logfile(const char* logfile, FILE **fpout)
{
	*fpout = AllocateFile(logfile, "w");

	if ((*fpout) == NULL)
	{
		ereport(LOG,
					(errcode_for_file_access(),
					errmsg("MELANIE: could not open make cool logfile \"%s\": %m",
									logfile)));
		return false;
	}

	return true;
}

void 
aio_dev_cleanup_logfile(const char* logfile, FILE *fpout)
{
	if (ferror(fpout))
	{
		ereport(LOG,
					(errcode_for_file_access(),
					errmsg("Could not write PGSR logfile \"%s\": %m",
									logfile)));
		FreeFile(fpout);
		unlink(logfile);
	}
	else if (FreeFile(fpout) < 0)
	{
		ereport(LOG,
						(errcode_for_file_access(),
						errmsg("Could not close PGSR logfile \"%s\": %m",
										logfile)));
		unlink(logfile);
	}
}

void
aio_dev_write_wait_log(PgStreamingReadWaitLog *log, FILE *logfile)
{
		fprintf(logfile, "wait_start,wait_end,wait_length,\n");

		for (int i = 0; i < log->length; i++)
		{
			PgStreamingReadWaitLogItem *current_log_entry = &log->data[i];

			instr_time wait_length = current_log_entry->wait_end;
			INSTR_TIME_SUBTRACT(wait_length, current_log_entry->wait_start);

			fprintf(logfile,
							"%lf,%lf,%lf\n",
							INSTR_TIME_GET_MILLISEC(current_log_entry->wait_start),
							INSTR_TIME_GET_MILLISEC(current_log_entry->wait_end),
							INSTR_TIME_GET_MILLISEC(wait_length));
		}

		fputc('E', logfile);
}

void
aio_dev_write_completion_log(PgStreamingReadCompletionLog *log, FILE *logfile, int max_prefetch_distance)
{
	fprintf(logfile, "latency,submission_time,completion_time,prefetch_distance,cnc,inflight,max_prefetch_distance,\n");

	for (int i = 0; i < log->length; i++)
	{
		PgStreamingReadCompletionLogItem *entry = &log->data[i];
		instr_time latency = entry->completion_time;
		INSTR_TIME_SUBTRACT(latency, entry->submission_time);

		fprintf(logfile,
					"%lf,%lf,%lf,%d,%d,%d,%d,\n",
					INSTR_TIME_GET_MILLISEC(latency),
					INSTR_TIME_GET_MILLISEC(entry->submission_time),
					INSTR_TIME_GET_MILLISEC(entry->completion_time),
					entry->prefetch_distance,
					entry->cnc,
					entry->inflight,
					max_prefetch_distance);
	}

	fputc('E', logfile);
}

void
aio_dev_write_consumption_log(PgStreamingReadConsumptionLog *log, FILE *logfile)
{
	fprintf(logfile, "consumption_time,\n");

	for (int i = 0; i < log->length; i++)
		fprintf(logfile, "%lf,\n", INSTR_TIME_GET_MILLISEC(log->data[i]));

	fputc('E', logfile);
}

void
aio_dev_write_log(PgStreamingReadDevLog *dev_log, int max_prefetch_distance)
{
	FILE * wait_logfile, * consumption_logfile, * completion_logfile;

	if (aio_dev_allocate_logfile(PGSR_PFD_WAIT_LOGFILE, &wait_logfile))
	{
		aio_dev_write_wait_log(dev_log->wait_log, wait_logfile);
		aio_dev_cleanup_logfile(PGSR_PFD_WAIT_LOGFILE, wait_logfile);
	}
	
	if (aio_dev_allocate_logfile(PGSR_PFD_COMPLETION_LOGFILE, &completion_logfile))
	{
		aio_dev_write_completion_log(dev_log->completion_log, completion_logfile, max_prefetch_distance);
		aio_dev_cleanup_logfile(PGSR_PFD_COMPLETION_LOGFILE, completion_logfile);
	}

	if (aio_dev_allocate_logfile(PGSR_PFD_CONSUMPTION_LOGFILE, &consumption_logfile))
	{
		aio_dev_write_consumption_log(dev_log->consumption_log, consumption_logfile);
		aio_dev_cleanup_logfile(PGSR_PFD_CONSUMPTION_LOGFILE, consumption_logfile);
	}
}
