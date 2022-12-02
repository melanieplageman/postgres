#include "postgres.h"

#include <unistd.h>

#include "executor/instrument.h"
#include "storage/aio_dev_log.h"
#include "storage/fd.h"
#include "utils/guc.h"

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
					errmsg("Could not allocate PGSR logfile \"%s\": %m",
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

void
aio_dev_write_wait_log(PgStreamingReadWaitLog *log, FILE *logfile)
{
		Assert(log->length > 0);

		fprintf(logfile, "wait_start,wait_end,wait_length\n");

		for (int i = 0; i < log->length; i++)
		{
			PgStreamingReadWaitLogItem *current_log_entry = &log->data[i];

			instr_time wait_length = current_log_entry->wait_end;
			INSTR_TIME_SUBTRACT(wait_length, current_log_entry->wait_start);

			fprintf(logfile,
							"%lf,%lf,%lf\n",
							INSTR_TIME_GET_MICROSEC(current_log_entry->wait_start),
							INSTR_TIME_GET_MICROSEC(current_log_entry->wait_end),
							INSTR_TIME_GET_MICROSEC(wait_length));
		}
}

void
aio_dev_write_completion_log(PgStreamingReadCompletionLog *log, FILE *logfile, int max_prefetch_distance)
{
	instr_time latency;
	instr_time last_valid_submission_time;

	Assert(log->length > 0);

	INSTR_TIME_SET_ZERO(last_valid_submission_time);

	for (int i = 0; i < log->length; i++)
	{
		PgStreamingReadCompletionLogItem *entry = &log->data[i];

		if (!INSTR_TIME_IS_ZERO(entry->submission_time))
		{
			last_valid_submission_time = entry->submission_time;
			break;
		}
	}

	Assert(!INSTR_TIME_IS_ZERO(last_valid_submission_time));

	fprintf(logfile, "latency,submission_time,completion_time,prefetch_distance,cnc,inflight,max_prefetch_distance\n");
	for (int i = 0; i < log->length; i++)
	{
		PgStreamingReadCompletionLogItem *entry = &log->data[i];

		if (INSTR_TIME_IS_ZERO(entry->submission_time))
			entry->submission_time = last_valid_submission_time;
		else
			last_valid_submission_time = entry->submission_time;

		latency = entry->completion_time;
		INSTR_TIME_SUBTRACT(latency, entry->submission_time);

		fprintf(logfile,
					"%lf,%lf,%lf,%d,%d,%d,%d\n",
					INSTR_TIME_GET_MICROSEC(latency),
					INSTR_TIME_GET_MICROSEC(entry->submission_time),
					INSTR_TIME_GET_MICROSEC(entry->completion_time),
					entry->prefetch_distance,
					entry->cnc,
					entry->inflight,
					max_prefetch_distance);
	}
}

void
aio_dev_write_consumption_log(PgStreamingReadConsumptionLog *log, FILE *logfile)
{
	Assert(log->length > 0);

	fprintf(logfile, "consumption_time\n");

	for (int i = 0; i < log->length; i++)
		fprintf(logfile, "%lf\n", INSTR_TIME_GET_MICROSEC(log->data[i]));
}

void
aio_dev_write_log(PgStreamingReadDevLog *dev_log, int max_prefetch_distance)
{
	FILE * wait_logfile, * consumption_logfile, * completion_logfile;

	char *log_dir = "/tmp/pgsr_pfd";

	char wait_filename[MAXPGPATH];
	char completion_filename[MAXPGPATH];
	char consumption_filename[MAXPGPATH];

	snprintf(wait_filename, MAXPGPATH, "%s/%s_%s", log_dir, "wait_log", application_name);

	if (dev_log->wait_log->length > 0 &&
			aio_dev_allocate_logfile(wait_filename, &wait_logfile))
	{
		aio_dev_write_wait_log(dev_log->wait_log, wait_logfile);
		aio_dev_cleanup_logfile(wait_filename, wait_logfile);
	}

	snprintf(completion_filename, MAXPGPATH, "%s/%s_%s", log_dir, "completion_log", application_name);

	if (dev_log->completion_log->length > 0 &&
			aio_dev_allocate_logfile(completion_filename, &completion_logfile))
	{
		aio_dev_write_completion_log(dev_log->completion_log, completion_logfile, max_prefetch_distance);
		aio_dev_cleanup_logfile(completion_filename, completion_logfile);
	}

	snprintf(consumption_filename, MAXPGPATH, "%s/%s_%s", log_dir, "consumption_log", application_name);

	if (dev_log->consumption_log->length > 0 &&
			aio_dev_allocate_logfile(consumption_filename, &consumption_logfile))
	{
		aio_dev_write_consumption_log(dev_log->consumption_log, consumption_logfile);
		aio_dev_cleanup_logfile(consumption_filename, consumption_logfile);
	}
}
