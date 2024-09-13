#include "postgres.h"
#include "access/avpagestream.h"
#include "utils/timestamp.h"

static void avpageinterval_merge(AVPageInterval *target, AVPageInterval *src);
static uint32 avpageinterval_to_drop(AVPageStream *stream);
static uint64 avpages_in_interval(AVPageInterval *interval);
static uint64 avpages_in_interval_older_than_cutoff(AVPageInterval *interval,
		TimestampTz cutoff);

/*
 * Merge the counters from src into target
 */
static void
avpageinterval_merge(AVPageInterval *target, AVPageInterval *src)
{
	target->end_time = src->end_time;
	target->end_lsn = src->end_lsn;

	target->av_pages += src->av_pages;
	target->un_av_pages += src->un_av_pages;
	target->members += src->members;
}

/*
 * Determine which AVPageStream member can be "dropped" and its values merged
 * with its predecessor.
 */
static uint32
avpageinterval_to_drop(AVPageStream *stream)
{
	Assert(stream->length >= AVPAGESTREAM_VOLUME);

	for (size_t i = 0; i < AVPAGESTREAM_VOLUME - 1; i++)
	{
		uint32		section = i / AVPAGESTREAM_CAPACITY_REPEATS;
		uint64		cap = 1 << (AVPAGESTREAM_NSECTIONS - section - 1);

		if (stream->data[i].members >= cap ||
			stream->data[i].members + stream->data[i + 1].members >= cap)
			continue;

		return i + 1;
	}

	return 0;
}

/*
 * Insert a new AVPageInterval into the AVPageStream, making room by merging an
 * existing interval into its predecessor if needed.
 */
void
avpagestream_add_new(AVPageStream *stream,
					 XLogRecPtr start_lsn, TimestampTz start_time)
{
	uint32		drop;
	AVPageInterval *target;
	AVPageInterval entrant = AVPAGES_INIT(start_lsn, start_time);

	/*
	 * If this is the first entry in the stream, fill in the upper bound of
	 * "oldest"
	 */
	if (stream->length == 0)
	{
		Assert(stream->oldest.end_lsn == InvalidXLogRecPtr);
		stream->oldest.end_lsn = start_lsn;
		stream->oldest.end_time = start_time;
	}
	else
	{
		/* Time must forward monotonically on the stream */
		Assert(start_time >= stream->data[stream->length - 1].end_time);
		Assert(start_lsn >= stream->data[stream->length - 1].end_lsn);
	}

	stream->version++;

	if (stream->length < AVPAGESTREAM_VOLUME)
	{
		stream->data[stream->length++] = entrant;
		return;
	}

	drop = avpageinterval_to_drop(stream);

	target = drop == 0 ? &stream->oldest : &stream->data[drop - 1];
	avpageinterval_merge(target, &stream->data[drop]);

	if (drop < stream->length - 1)
		memmove(&stream->data[drop],
				&stream->data[drop + 1],
				sizeof(AVPageInterval) * (stream->length - 1 - drop));

	stream->data[stream->length - 1] = entrant;
}


/*
 * Find and return the AVPageInterval covering page_lsn.
 */
AVPageInterval *
avpagestream_get_setter(AVPageStream *stream, XLogRecPtr page_lsn)
{
	/*
	 * If nothing has been added to this stream, there can be no setter.
	 */
	if (stream->version == 0)
		return NULL;

	/*
	 * We only set the boundaries of the "oldest" member when inserting the
	 * first interval.
	 */
	Assert(stream->length > 0);

	/*
	 * All-visible pages with last modifications before the first entry in the
	 * stream are counted in "oldest".
	 */
	if (page_lsn < stream->data[0].start_lsn)
	{
		Assert(page_lsn >= stream->oldest.start_lsn &&
			   page_lsn <= stream->oldest.end_lsn);

		return &stream->oldest;
	}

	for (size_t i = 0; i < stream->length; i++)
	{
		if (page_lsn > stream->data[i].end_lsn)
			continue;

		Assert(page_lsn > stream->data[i].start_lsn);
		return &stream->data[i];
	}

	/*
	 * The newest interval in the stream should have an open upper bound.
	 */
	Assert(stream->data[stream->length - 1].end_lsn == InvalidXLogRecPtr);
	return &stream->data[stream->length - 1];
}

/*
 * Returns the number of all-visible not all-frozen pages in the interval.
 */
static uint64
avpages_in_interval(AVPageInterval *interval)
{
	if (interval->un_av_pages >= interval->av_pages)
		return 0;
	return interval->av_pages - interval->un_av_pages;
}

/*
 * Uses linear interpolation to calculate the number of all-visible not
 * all-frozen pages older than cutoff.
 */
static uint64
avpages_in_interval_older_than_cutoff(AVPageInterval *interval,
		TimestampTz cutoff)
{
	uint64 time_interval;
	uint64 time_since_start_interval;
	double time_quantile;
	uint64 av_pages;
	double portion_of_av_pages;

	Assert(cutoff >= interval->start_time &&
			cutoff <= interval->end_time);

	time_interval = interval->end_time - interval->start_time;
	time_since_start_interval = cutoff - interval->start_time;

	time_quantile = (double) time_since_start_interval / time_interval;
	Assert(time_quantile >= 0 && time_quantile <= 1);
	av_pages = avpages_in_interval(interval);
	portion_of_av_pages = time_quantile * (double) av_pages;
	return (uint64) portion_of_av_pages;
}

/*
 * Returns the number of all-visible but not all-frozen pages older than the
 * target_time.
 */
uint64
avpages_older_than_cutoff(AVPageStream *stream,
						   TimestampTz target_time)
{
	uint64 total = 0;

	if (stream->length == 0)
	{
		elog(DEBUG1,
			 "Attempt to identify bounds for time: \"%s\" using empty AVPageStream.",
			 timestamptz_to_str(target_time));
		return 0;
	}

	if (target_time < stream->data[0].start_lsn)
	{
		Assert(target_time >= stream->oldest.start_time &&
				target_time <= stream->oldest.end_time);
		return avpages_in_interval_older_than_cutoff(&stream->oldest,
				target_time);
	}

	total += avpages_in_interval(&stream->oldest);

	for (size_t i = 0; i < stream->length; i++)
	{
		if (target_time > stream->data[i].end_time)
		{
			total += avpages_in_interval(&stream->data[i]);
			continue;
		}

		Assert(target_time > stream->data[i].start_time);
		total += avpages_in_interval_older_than_cutoff(&stream->data[i],
				target_time);
		return total;
	}

	/*
	 * The newest interval in the stream should have an open upper bound. We
	 * will have already counted all of its AV pages toward the total.
	 */
	Assert(stream->data[stream->length - 1].end_time == 0);
	return total;
}

