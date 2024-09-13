#include "postgres.h"
#include "access/lsntimestream.h"
#include "inttypes.h"
#include "utils/timestamp.h"

/*
 * LSNTimeStream maintenance helper functions
 */
static uint32 lsntime_to_drop(LSNTimeStream *stream);

pg_attribute_unused()
static void lsntime_insert(LSNTimeStream *stream, XLogRecPtr lsn,
						   TimestampTz time);

/*
 * LSNTimeStream usage helper function
 */
pg_attribute_unused()
static void stream_get_bounds_for_time(const LSNTimeStream *stream,
									   TimestampTz target_time,
									   LSNTime *lower,
									   LSNTime *upper);

/*
 * Returns the index of the LSNTime to drop from a full LSNTimeStream.
 *
 * We find the oldest element that is not at capacity and drop the subsequent
 * LSNTime. If all elements are at capacity drop the oldest LSNTime. This
 * provides the highest accuracy for recent data. See comments on
 * LSNTIMESTREAM_CAPACITY_REPEATS for more on how element capacity is
 * determined.
 */
static uint32
lsntime_to_drop(LSNTimeStream *stream)
{
	/* Don't drop LSNTimes if the array is not full */
	Assert(stream->length == LSNTIMESTREAM_VOLUME);

	for (size_t i = 0; i < LSNTIMESTREAM_VOLUME - 1; i++)
	{
		uint32		section = i / LSNTIMESTREAM_CAPACITY_REPEATS;
		uint32		capacity = 1 << (LSNTIMESTREAM_NSECTIONS - section - 1);

		if (stream->data[i].members >= capacity ||
			stream->data[i].members + stream->data[i + 1].members >= capacity)
			continue;

		return i + 1;
	}

	/*
	 * Once all of the elements have reached capacity, the oldest is dropped.
	 */
	return 0;
}

/*
 * Insert a new LSNTime with the passed-in time and lsn into the LSNTimeStream
 * in the first available element. If there are no empty elements, drop an
 * LSNTime from the stream to make room for the new LSNTime.
 */
static void
lsntime_insert(LSNTimeStream *stream, XLogRecPtr lsn,
			   TimestampTz time)
{
	uint32		drop;
	LSNTime		entrant = LSNTIME_INIT(lsn, time);

	if (stream->length < LSNTIMESTREAM_VOLUME)
	{
		/*
		 * Time must move forward on the stream. If the clock moves backwards,
		 * for example in an NTP correction, we'll just skip inserting this
		 * LSNTime.
		 *
		 * Though time must monotonically increase, it is valid to insert
		 * multiple LSNTimes with the same LSN. Imagine a period of time in
		 * which no new WAL records are inserted.
		 */
		if (stream->length > 0 &&
			(time <= stream->data[stream->length - 1].time ||
			 lsn < stream->data[stream->length - 1].lsn))
		{
			ereport(WARNING,
					errmsg("Won't insert non-monotonic \"%" PRIu64 ", %s\" to LSNTimeStream.",
						   lsn, timestamptz_to_str(time)));
			return;
		}

		stream->data[stream->length++] = entrant;
		return;
	}

	drop = lsntime_to_drop(stream);
	if (drop > 0)
		stream->data[drop - 1].members += stream->data[drop].members;

	/*
	 * If we are dropping any LSNTime except the most recent, shift the data
	 * down.
	 */
	if (drop < stream->length - 1)
		memmove(&stream->data[drop],
				&stream->data[drop + 1],
				sizeof(LSNTime) * (stream->length - 1 - drop));

	stream->data[stream->length - 1] = entrant;
}


/*
 * Returns (into *lower and *upper) the narrowest range of LSNTimes on the
 * stream covering the target_time.
 *
 * If target_time is older than all values on the stream, lower will be
 * invalid. If target_time is newer than all values on the stream, upper will
 * be invalid. If the stream is empty, both upper and lower will be invalid.
 * The caller must check if upper and/or lower's lsn member is
 * InvalidXLogRecPtr to detect whether that bound is valid.
 */
static void
stream_get_bounds_for_time(const LSNTimeStream *stream,
						   TimestampTz target_time,
						   LSNTime *lower,
						   LSNTime *upper)
{
	Assert(lower && upper);

	*lower = LSNTIME_INIT(InvalidXLogRecPtr, 0);
	*upper = LSNTIME_INIT(InvalidXLogRecPtr, 0);

	/*
	 * If the LSNTimeStream has no members, it provides no information about
	 * the range.
	 */
	if (stream->length == 0)
	{
		elog(DEBUG1,
			 "Attempt to identify LSN bounds for time: \"%s\" using empty LSNTimeStream.",
			 timestamptz_to_str(target_time));
		return;
	}

	/*
	 * If the target_time is older than the stream, the oldest member in the
	 * stream is our upper bound.
	 */
	if (target_time <= stream->data[0].time)
	{
		*upper = stream->data[0];
		if (target_time == stream->data[0].time)
			*lower = stream->data[0];
		return;
	}

	/*
	 * Loop through the stream and stop at the first LSNTime newer than or
	 * equal to our target time. Skip the first LSNTime, as we know it is
	 * older than our target time.
	 */
	for (size_t i = 1; i < stream->length; i++)
	{
		if (target_time == stream->data[i].time)
		{
			*lower = stream->data[i];
			*upper = stream->data[i];
			return;
		}

		if (target_time < stream->data[i].time)
		{
			/* Time must increase monotonically on the stream. */
			Assert(stream->data[i - 1].time <
				   stream->data[i].time);
			*lower = stream->data[i - 1];
			*upper = stream->data[i];
			return;
		}
	}

	/*
	 * target_time is newer than the stream, so the newest member in the
	 * stream is our lower bound.
	 */
	*lower = stream->data[stream->length - 1];
}
