#ifndef AVPAGESTREAM_H
#define AVPAGESTREAM_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"

/*
 * An interval of time bounded by start and end times and LSNs. av_pages -
 * un_av_pages is the number of currently all-visible but not all-frozen pages
 * with an LSN in the range starting with start_lsn and ending with end_lsn.
 * av_pages is incremented each time a page with a covered LSN is set
 * all-visible (but not all-frozen) and un_av_pages is decremented each time an
 * all-visible not all-frozen page with an LSN in that interval is modified.
 *
 * The new_* members are incremented and decremented by backends in their local
 * version of an AVPageStream and then those deltas are flushed to the shared
 * version periodically.
 *
 * An AVPageInterval has a number of "logical" members. These are the number of
 * original intervals bounded by start and end lsn and time pairs covered by
 * the range specified by a AVPageInterval's start and end lsn and time. In an
 * AVPageStream we control the size of the ranges using the logical members of
 * the member AVPageIntervals.
 */
typedef struct AVPageInterval
{
	XLogRecPtr	start_lsn;
	TimestampTz start_time;
	XLogRecPtr	end_lsn;
	TimestampTz end_time;
	uint64		av_pages;
	uint64		un_av_pages;
	uint64		members;
} AVPageInterval;

#define AVPAGES_INIT(i_slsn, i_stime) \
	((AVPageInterval) {.start_lsn = (i_slsn), \
	 .start_time = (i_stime), \
	 .end_time = 0, \
	 .end_lsn = InvalidXLogRecPtr, \
	 .av_pages = 0, \
	 .un_av_pages = 0, \
	 .members = 1 })

/*
 * The stream version is the total number of members represented it the stream.
 * It is incremented each time a new member is added. When a new member is
 * added, there is a new interval on the stream. Additionally, we may change
 * the boundaries of existing intervals. When merging local stream counts into
 * the shared counter, if the version is the same, we can use a simple merge
 * protocol.
 *
 * oldest is an interval starting at 0 and ending when the oldest interval in
 * data starts.
 *
 * has_values is used only for local copies of the AVPageStream to avoid
 * unnecessary flushing to a shared AVPageStream.
 */
#define AVPAGESTREAM_VOLUME 16
typedef struct AVPageStream
{
	uint64		version;
	bool		has_values;
	AVPageInterval oldest;
	uint32		length;
	AVPageInterval data[AVPAGESTREAM_VOLUME];
} AVPageStream;

/*
 * Within an AVPageStream, we want the highest accuracy for recent elements.
 * Thus, when the stream is full, we will drop older elements such that older
 * elements represent larger ranges of wall clock time.
 *
 * We'll organize an AVPageStream such that the number of logical members
 * represented by each element of the array decreases as we get closer to the
 * most recent data points, eventually reaching 1. The "capacity" of each
 * element is a power of 2 and each capacity is repeated 2 times, meaning that
 * the maximum capacity of an AVPageInterval in a length 16 AVPageStream is 128.
 * Over time, the number of logical members will trend toward:
 *
 *   [128, 128, 64, 64, 32, 32, 16, 16, 8, 8, 4, 4, 2, 2, 1, 1]
 *
 * Once all of the elements have reached capacity, the oldest element is
 * merged into the "oldest" member of the AVPageStream
 */
#define AVPAGESTREAM_CAPACITY_REPEATS 2
#define AVPAGESTREAM_NSECTIONS \
	(AVPAGESTREAM_VOLUME / AVPAGESTREAM_CAPACITY_REPEATS)

extern void avpagestream_add_new(AVPageStream *stream,
								 XLogRecPtr start_lsn, TimestampTz start_time);
extern AVPageInterval *avpagestream_get_setter(AVPageStream *stream,
											   XLogRecPtr page_lsn);
extern uint64 avpages_older_than_cutoff(AVPageStream *stream,
		TimestampTz target_time);

#endif
