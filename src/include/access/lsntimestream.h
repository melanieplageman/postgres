#ifndef LSNTIMESTREAM_H
#define LSNTIMESTREAM_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"

/*
 * LSNTime are the elements of an LSNTimeStream. For the LSNTimeStream to be
 * meaningful, the lsn should be drawn from a consistent source. For example,
 * each LSNTime could be the insert LSN at a point in time.
 *
 * An LSNTime has a number of "logical" members. These are the number of
 * original lsn, time pairs covered by the range beginning with this LSNTime's
 * time, lsn pair. We control the size of ranges in an LSNTimeStream using the
 * logical members of its constituent LSNTimes.
 */
typedef struct LSNTime
{
	TimestampTz time;
	XLogRecPtr	lsn;
	uint64		members;
} LSNTime;

/*
 * Convenience macro returning an LSNTime with the time and LSN set to the
 * passed in values.
 */
#define LSNTIME_INIT(i_lsn, i_time) \
	((LSNTime) { .lsn = (i_lsn), .time = (i_time), .members = 1})

#define LSNTIMESTREAM_VOLUME 128

/*
 * An LSN time stream is an array consisting of LSNTimes from least to most
 * recent. The array is filled before any element is dropped. Once the
 * LSNTimeStream length == volume (the array is full), an LSNTime is dropped,
 * the subsequent LSNTimes are shifted toward the head by 1, and the new
 * LSNTime is inserted at the tail.
 *
 * Within an LSNTimeStream, we want the highest accuracy for recent elements.
 * Thus, when the stream is full, we will drop older elements such that older
 * LSNtimes represent larger ranges of wall clock time.
 *
 * Use the stream for LSN <-> time conversions.
 */
typedef struct LSNTimeStream
{
	uint32		length;
	LSNTime		data[LSNTIMESTREAM_VOLUME];
} LSNTimeStream;

/*
 * We'll organize an LSNTimeStream such that the number of logical members
 * represented by each element in the array decreases as we get closer to the
 * most recent data points, eventually reaching 1. The "capacity" of each
 * element is a power of 2 and each capacity is repeated 16 times, meaning that
 * the maximum capacity of an element in a length 128 LSNTimeStream is 128.
 * Over time, the number of logical members will trend toward:
 *
 *   [128, 128, (... x 13), 128, 64, 64, (... x 13), 64, ..., 1]
 *
 * Once all of the elements have reached capacity, the oldest LSNTime is
 * dropped. With an insertion interval of 2 minutes, a fully at-capacity
 * LSNTimeStream covers around 6 days of data.
 */
#define LSNTIMESTREAM_CAPACITY_REPEATS 16
#define LSNTIMESTREAM_NSECTIONS \
	(LSNTIMESTREAM_VOLUME / LSNTIMESTREAM_CAPACITY_REPEATS)

/*
 * LSNTimeStream maintenance helper function
 */
extern void lsntime_insert(LSNTimeStream *stream, XLogRecPtr lsn,
						   TimestampTz time);


/*
 * LSNTimeStream usage helper function
 */
extern void stream_get_bounds_for_time(const LSNTimeStream *stream,
									   TimestampTz target_time,
									   LSNTime *lower,
									   LSNTime *upper);

#endif
