/*-------------------------------------------------------------------------
 *
 * sharedtuplestore.c
 *	  Simple mechanism for sharing tuples between backends.
 *
 * This module contains a shared temporary tuple storage mechanism providing
 * a parallel-aware subset of the features of tuplestore.c.  Multiple backends
 * can write to a SharedTuplestore, and then multiple backends can later scan
 * the stored tuples.  Currently, the only scan type supported is a parallel
 * scan where each backend reads an arbitrary subset of the tuples that were
 * written.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/sort/sharedtuplestore.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "storage/lwlock.h"
#include "storage/sharedfileset.h"
#include "utils/sharedtuplestore.h"

/*
 * The size of chunks, in pages.  This is somewhat arbitrarily set to match
 * the size of HASH_CHUNK, so that Parallel Hash obtains new chunks of tuples
 * at approximately the same rate as it allocates new chunks of memory to
 * insert them into.
 */
#define STS_CHUNK_PAGES 4
#define STS_CHUNK_HEADER_SIZE offsetof(SharedTuplestoreChunk, data)
#define STS_CHUNK_DATA_SIZE (STS_CHUNK_PAGES * BLCKSZ - STS_CHUNK_HEADER_SIZE)

/* Chunk written to disk. */
typedef struct SharedTuplestoreChunk
{
	int			ntuples;		/* Number of tuples in this chunk. */
	int			overflow;		/* If overflow, how many including this one? */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} SharedTuplestoreChunk;

/* Per-participant shared state. */
typedef struct SharedTuplestoreParticipant
{
	LWLock		lock;
	BlockNumber read_page;		/* Page number for next read. */
	BlockNumber npages;			/* Number of pages written. */
	bool		writing;		/* Used only for assertions. */
} SharedTuplestoreParticipant;

/* The control object that lives in shared memory. */
struct SharedTuplestore
{
	int			nparticipants;	/* Number of participants that can write. */
	pg_atomic_uint32 exact_tuplenum;
	int			flags;			/* Flag bits from SHARED_TUPLESTORE_XXX */
	size_t		meta_data_size; /* Size of per-tuple header. */
	char		name[NAMEDATALEN];	/* A name for this tuplestore. */

	/* Followed by per-participant shared state. */
	SharedTuplestoreParticipant participants[FLEXIBLE_ARRAY_MEMBER];
};

/* Per-participant state that lives in backend-local memory. */
struct SharedTuplestoreAccessor
{
	int			participant;	/* My participant number. */
	SharedTuplestore *sts;		/* The shared state. */
	SharedFileSet *fileset;		/* The SharedFileSet holding files. */
	MemoryContext context;		/* Memory context for buffers. */

	/* State for reading. */
	int			read_participant;	/* The current participant to read from. */
	BufFile    *read_file;		/* The current file to read from. */
	BufFile *outer_match_status_file; /* currently just used for outermatchstatus file */
	int			read_ntuples_available; /* The number of tuples in chunk. */
	int			read_ntuples;	/* How many tuples have we read from chunk? */
	size_t		read_bytes;		/* How many bytes have we read from chunk? */
	char	   *read_buffer;	/* A buffer for loading tuples. */
	size_t		read_buffer_size;
	BlockNumber read_next_page; /* Lowest block we'll consider reading. */

	/* State for writing. */
	SharedTuplestoreChunk *write_chunk; /* Buffer for writing. */
	BufFile    *write_file;		/* The current file to write to. */
	BlockNumber write_page;		/* The next page to write to. */
	char	   *write_pointer;	/* Current write pointer within chunk. */
	char	   *write_end;		/* One past the end of the current chunk. */
};

static void sts_filename(char *name, SharedTuplestoreAccessor *accessor,
						 int participant);

/*
 * Return the amount of shared memory required to hold SharedTuplestore for a
 * given number of participants.
 */
size_t
sts_estimate(int participants)
{
	return offsetof(SharedTuplestore, participants) +
		sizeof(SharedTuplestoreParticipant) * participants;
}

/*
 * Initialize a SharedTuplestore in existing shared memory.  There must be
 * space for sts_estimate(participants) bytes.  If flags includes the value
 * SHARED_TUPLESTORE_SINGLE_PASS, the files may in future be removed more
 * eagerly (but this isn't yet implemented).
 *
 * Tuples that are stored may optionally carry a piece of fixed sized
 * meta-data which will be retrieved along with the tuple.  This is useful for
 * the hash values used in multi-batch hash joins, but could have other
 * applications.
 *
 * The caller must supply a SharedFileSet, which is essentially a directory
 * that will be cleaned up automatically, and a name which must be unique
 * across all SharedTuplestores created in the same SharedFileSet.
 */
SharedTuplestoreAccessor *
sts_initialize(SharedTuplestore *sts, int participants,
			   int my_participant_number,
			   size_t meta_data_size,
			   int flags,
			   SharedFileSet *fileset,
			   const char *name)
{
	SharedTuplestoreAccessor *accessor;
	int			i;

	Assert(my_participant_number < participants);

	sts->nparticipants = participants;
	pg_atomic_init_u32(&sts->exact_tuplenum, 0);
	sts->meta_data_size = meta_data_size;
	sts->flags = flags;

	if (strlen(name) > sizeof(sts->name) - 1)
		elog(ERROR, "SharedTuplestore name too long");
	strcpy(sts->name, name);

	/*
	 * Limit meta-data so it + tuple size always fits into a single chunk.
	 * sts_puttuple() and sts_read_tuple() could be made to support scenarios
	 * where that's not the case, but it's not currently required. If so,
	 * meta-data size probably should be made variable, too.
	 */
	if (meta_data_size + sizeof(uint32) >= STS_CHUNK_DATA_SIZE)
		elog(ERROR, "meta-data too long");

	for (i = 0; i < participants; ++i)
	{
		LWLockInitialize(&sts->participants[i].lock,
						 LWTRANCHE_SHARED_TUPLESTORE);
		sts->participants[i].read_page = 0;
		sts->participants[i].writing = false;
	}

	accessor = palloc0(sizeof(SharedTuplestoreAccessor));
	accessor->participant = my_participant_number;
	accessor->sts = sts;
	accessor->fileset = fileset;
	accessor->context = CurrentMemoryContext;
	// TODO: should I initialize this here
	accessor->outer_match_status_file = NULL; // TODO: do I need this here. also, if read_file isn't set to NULL here, why?

	return accessor;
}

/*
 * Attach to a SharedTuplestore that has been initialized by another backend,
 * so that this backend can read and write tuples.
 */
SharedTuplestoreAccessor *
sts_attach(SharedTuplestore *sts,
		   int my_participant_number,
		   SharedFileSet *fileset)
{
	SharedTuplestoreAccessor *accessor;

	Assert(my_participant_number < sts->nparticipants);

	accessor = palloc0(sizeof(SharedTuplestoreAccessor));
	accessor->participant = my_participant_number;
	accessor->sts = sts;
	accessor->fileset = fileset;
	accessor->context = CurrentMemoryContext;
	return accessor;
}

static void
sts_flush_chunk(SharedTuplestoreAccessor *accessor)
{
	size_t		size;
	size_t		written;

	size = STS_CHUNK_PAGES * BLCKSZ;
	written = BufFileWrite(accessor->write_file, accessor->write_chunk, size);
	if (written != size)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to temporary file: %m")));
	memset(accessor->write_chunk, 0, size);
	accessor->write_pointer = &accessor->write_chunk->data[0];
	accessor->sts->participants[accessor->participant].npages +=
		STS_CHUNK_PAGES;
}

/*
 * Finish writing tuples.  This must be called by all backends that have
 * written data before any backend begins reading it.
 */
void
sts_end_write(SharedTuplestoreAccessor *accessor)
{
	if (accessor->write_file != NULL)
	{
		sts_flush_chunk(accessor);
		BufFileClose(accessor->write_file);
		pfree(accessor->write_chunk);
		accessor->write_chunk = NULL;
		accessor->write_file = NULL;
		accessor->sts->participants[accessor->participant].writing = false;
	}

}


char *sts_cleanup_STA_outer_match_status_files(SharedTuplestoreAccessor *accessor)
{
//	for (int i = 0; i < accessor->sts->nparticipants; i++)
//	{
//		char bitmap_filename[MAXPGPATH];
//		sts_bitmap_filename(bitmap_filename, accessor, i);
//
//	}
	if (accessor->outer_match_status_file != NULL)
	{
		char bitmap_filename[MAXPGPATH];
		sts_bitmap_filename(bitmap_filename, accessor, accessor->participant);
		BufFileClose(accessor->outer_match_status_file);
		accessor->outer_match_status_file = NULL;
		return "";
	}
	return "";
}

/*
 * Prepare to rescan.  Only one participant must call this.  After it returns,
 * all participants may call sts_begin_parallel_scan() and then loop over
 * sts_parallel_scan_next().  This function must not be called concurrently
 * with a scan, and synchronization to avoid that is the caller's
 * responsibility.
 */
void
sts_reinitialize(SharedTuplestoreAccessor *accessor)
{
	int			i;

	/*
	 * Reset the shared read head for all participants' files.  Also set the
	 * initial chunk size to the minimum (any increases from that size will be
	 * recorded in chunk_expansion_log).
	 */
	for (i = 0; i < accessor->sts->nparticipants; ++i)
	{
		accessor->sts->participants[i].read_page = 0;
		// TODO: do I need to do anything with outer match status file here?
	}
}

/*
 * Begin scanning the contents in parallel.
 */
void
sts_begin_parallel_scan(SharedTuplestoreAccessor *accessor)
{
	int			i PG_USED_FOR_ASSERTS_ONLY;

	/* End any existing scan that was in progress. */
	sts_end_parallel_scan(accessor);

	/*
	 * Any backend that might have written into this shared tuplestore must
	 * have called sts_end_write(), so that all buffers are flushed and the
	 * files have stopped growing.
	 */
	for (i = 0; i < accessor->sts->nparticipants; ++i)
		Assert(!accessor->sts->participants[i].writing);

	/*
	 * We will start out reading the file that THIS backend wrote.  There may
	 * be some caching locality advantage to that.
	 */
	accessor->read_participant = accessor->participant;
	accessor->read_file = NULL;
	accessor->outer_match_status_file = NULL;
	accessor->read_next_page = 0;
}

/*
 * Finish a parallel scan, freeing associated backend-local resources.
 */
void
sts_end_parallel_scan(SharedTuplestoreAccessor *accessor)
{
	/*
	 * Here we could delete all files if SHARED_TUPLESTORE_SINGLE_PASS, but
	 * we'd probably need a reference count of current parallel scanners so we
	 * could safely do it only when the reference count reaches zero.
	 */
	if (accessor->read_file != NULL)
	{
		BufFileClose(accessor->read_file);
		accessor->read_file = NULL;
	}
}

uint32 sts_gettuplenum(SharedTuplestoreAccessor *accessor)
{
	 return pg_atomic_read_u32(&accessor->sts->exact_tuplenum);
}

void
sts_make_STA_outerMatchStatuses(SharedTuplestoreAccessor *accessor, int batchno, char *name)
{
	uint32 tuplenum = sts_gettuplenum(accessor);
	// don't make a file if there are no tuples
	if (tuplenum == 0)
		return;
	sts_bitmap_filename(name, accessor, sts_get_my_participant_number(accessor));
	accessor->outer_match_status_file = BufFileCreateShared(sts_get_fileset(accessor), name);

	uint32 num_to_write = tuplenum / 8 + 1;

	unsigned char byteToWrite = 0;
	BufFileWrite(accessor->outer_match_status_file, &byteToWrite, num_to_write);

	if (BufFileSeek(accessor->outer_match_status_file, 0, 0L, SEEK_SET))
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("could not rewind hash-join temporary file: %m")));
	elog(NOTICE, "sts_make_STA_outerMatchStatuses. batchno %i. final_tuplenum %i. pid %i.", batchno, tuplenum, MyProcPid);
}
BufFile *sts_get_my_STA_outerMatchStatuses(SharedTuplestoreAccessor *accessor)
{
	return accessor->outer_match_status_file;
}

BufFile *sts_get_a_STA_outerMatchStatuses(SharedTuplestoreAccessor *accessor, int participant_num)
{
	char bitmap_filename[MAXPGPATH];
	sts_bitmap_filename(bitmap_filename, accessor, participant_num);
	return BufFileOpenSharedIfExists(accessor->fileset, bitmap_filename);
}


int sts_get_my_participant_number(SharedTuplestoreAccessor *accessor)
{
	return accessor->participant;
}

/*
 * Write a tuple.  If a meta-data size was provided to sts_initialize, then a
 * pointer to meta data of that size must be provided.
 */
void
sts_puttuple(SharedTuplestoreAccessor *accessor, void *meta_data, MinimalTuple tuple, bool count_tuples)
{
	size_t		size;

	/* Do we have our own file yet? */
	if (accessor->write_file == NULL)
	{
		SharedTuplestoreParticipant *participant;
		char		name[MAXPGPATH];

		/* Create one.  Only this backend will write into it. */
		sts_filename(name, accessor, accessor->participant);
		accessor->write_file = BufFileCreateShared(accessor->fileset, name);

		/* Set up the shared state for this backend's file. */
		participant = &accessor->sts->participants[accessor->participant];
		participant->writing = true;	/* for assertions only */
		if (count_tuples == true)
		{
			((tupleMetadata *) meta_data)->tuplenum = pg_atomic_fetch_add_u32(&accessor->sts->exact_tuplenum, 1);
			//((tupleMetadata *) meta_data)->tuplenum = 0x00ADBEEF;
			elog(DEBUG1, "%i.%i.%s.%i.",((tupleMetadata *) meta_data)->tuplenum, accessor->participant, accessor->sts->name, MyProcPid);
		}
	}

	/* Do we have space? */
	// TODO: correctly handle increasing tuplenum and writing tuple metadata in case of this if statement -- oversize tuples
	// I missed some cases
	size = accessor->sts->meta_data_size + tuple->t_len;
	if (accessor->write_pointer + size >= accessor->write_end)
	{
		if (accessor->write_chunk == NULL)
		{
			/* First time through.  Allocate chunk. */
			accessor->write_chunk = (SharedTuplestoreChunk *)
				MemoryContextAllocZero(accessor->context,
									   STS_CHUNK_PAGES * BLCKSZ);
			accessor->write_chunk->ntuples = 0;
			// TODO: count tuples that are written to our little buffer
			// do they only get flushed with sts_flush_chunk?
			accessor->write_pointer = &accessor->write_chunk->data[0];
			accessor->write_end = (char *)
				accessor->write_chunk + STS_CHUNK_PAGES * BLCKSZ;
		}
		else
		{
			/* See if flushing helps. */
			//TODO: this is writing to the file, need to make sure these tuples are counted
			sts_flush_chunk(accessor);
		}

		/* It may still not be enough in the case of a gigantic tuple. */
		if (accessor->write_pointer + size >= accessor->write_end)
		{
			size_t		written;

			/*
			 * We'll write the beginning of the oversized tuple, and then
			 * write the rest in some number of 'overflow' chunks.
			 *
			 * sts_initialize() verifies that the size of the tuple +
			 * meta-data always fits into a chunk. Because the chunk has been
			 * flushed above, we can be sure to have all of a chunk's usable
			 * space available.
			 */
			Assert(accessor->write_pointer + accessor->sts->meta_data_size +
				   sizeof(uint32) < accessor->write_end);
			if (count_tuples == true)
			{
				((tupleMetadata *) meta_data)->tuplenum = pg_atomic_fetch_add_u32(&accessor->sts->exact_tuplenum, 1);
				//((tupleMetadata *) meta_data)->tuplenum = 0x00ADBEEF;
				elog(DEBUG1, "%i.%i.%s.%i.",((tupleMetadata *) meta_data)->tuplenum, accessor->participant, accessor->sts->name, MyProcPid);
			}


			/* Write the meta-data as one chunk. */
			if (accessor->sts->meta_data_size > 0)
				memcpy(accessor->write_pointer, meta_data,
					   accessor->sts->meta_data_size);

			/*
			 * Write as much of the tuple as we can fit. This includes the
			 * tuple's size at the start.
			 */
			written = accessor->write_end - accessor->write_pointer -
				accessor->sts->meta_data_size;
			memcpy(accessor->write_pointer + accessor->sts->meta_data_size,
				   tuple, written);
			++accessor->write_chunk->ntuples;
			size -= accessor->sts->meta_data_size;
			size -= written;
			/* Now write as many overflow chunks as we need for the rest. */
			while (size > 0)
			{
				size_t		written_this_chunk;

				sts_flush_chunk(accessor);

				/*
				 * How many overflow chunks to go?  This will allow readers to
				 * skip all of them at once instead of reading each one.
				 */
				accessor->write_chunk->overflow = (size + STS_CHUNK_DATA_SIZE - 1) /
					STS_CHUNK_DATA_SIZE;
				written_this_chunk =
					Min(accessor->write_end - accessor->write_pointer, size);
				memcpy(accessor->write_pointer, (char *) tuple + written,
					   written_this_chunk);
				accessor->write_pointer += written_this_chunk;
				size -= written_this_chunk;
				written += written_this_chunk;
			}
			return;
		}
	}
	if (count_tuples == true)
	{
		((tupleMetadata *) meta_data)->tuplenum = pg_atomic_fetch_add_u32(&accessor->sts->exact_tuplenum, 1);
		//((tupleMetadata *) meta_data)->tuplenum = 0x00ADBEEF;
		elog(DEBUG1, "%i.%i.%s.%i.", ((tupleMetadata *) meta_data)->tuplenum, accessor->participant, accessor->sts->name,MyProcPid);
	}

	/* Copy meta-data and tuple into buffer. */
	if (accessor->sts->meta_data_size > 0)
		memcpy(accessor->write_pointer, meta_data,
			   accessor->sts->meta_data_size);

	memcpy(accessor->write_pointer + accessor->sts->meta_data_size, tuple,
		   tuple->t_len);
	accessor->write_pointer += size;
	++accessor->write_chunk->ntuples;
}

static MinimalTuple
sts_read_tuple(SharedTuplestoreAccessor *accessor, void *meta_data)
{
	MinimalTuple tuple;
	uint32		size;
	size_t		remaining_size;
	size_t		this_chunk_size;
	char	   *destination;

	/*
	 * We'll keep track of bytes read from this chunk so that we can detect an
	 * overflowing tuple and switch to reading overflow pages.
	 */
	if (accessor->sts->meta_data_size > 0)
	{
		if (BufFileRead(accessor->read_file,
						meta_data,
						accessor->sts->meta_data_size) !=
			accessor->sts->meta_data_size)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from shared tuplestore temporary file"),
					 errdetail_internal("Short read while reading meta-data.")));
		accessor->read_bytes += accessor->sts->meta_data_size;
	}
	if (BufFileRead(accessor->read_file,
					&size,
					sizeof(size)) != sizeof(size))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from shared tuplestore temporary file"),
				 errdetail_internal("Short read while reading size.")));
	accessor->read_bytes += sizeof(size);
	if (size > accessor->read_buffer_size)
	{
		size_t		new_read_buffer_size;

		if (accessor->read_buffer != NULL)
			pfree(accessor->read_buffer);
		new_read_buffer_size = Max(size, accessor->read_buffer_size * 2);
		accessor->read_buffer =
			MemoryContextAlloc(accessor->context, new_read_buffer_size);
		accessor->read_buffer_size = new_read_buffer_size;
	}
	remaining_size = size - sizeof(uint32);
	this_chunk_size = Min(remaining_size,
						  BLCKSZ * STS_CHUNK_PAGES - accessor->read_bytes);
	destination = accessor->read_buffer + sizeof(uint32);
	if (BufFileRead(accessor->read_file,
					destination,
					this_chunk_size) != this_chunk_size)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from shared tuplestore temporary file"),
				 errdetail_internal("Short read while reading tuple.")));
	accessor->read_bytes += this_chunk_size;
	remaining_size -= this_chunk_size;
	destination += this_chunk_size;
	++accessor->read_ntuples;
	//accessor->tuplenum++;

	/* Check if we need to read any overflow chunks. */
	while (remaining_size > 0)
	{
		/* We are now positioned at the start of an overflow chunk. */
		SharedTuplestoreChunk chunk_header;

		if (BufFileRead(accessor->read_file, &chunk_header, STS_CHUNK_HEADER_SIZE) !=
			STS_CHUNK_HEADER_SIZE)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from shared tuplestore temporary file"),
					 errdetail_internal("Short read while reading overflow chunk header.")));
		accessor->read_bytes = STS_CHUNK_HEADER_SIZE;
		if (chunk_header.overflow == 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("unexpected chunk in shared tuplestore temporary file"),
					 errdetail_internal("Expected overflow chunk.")));
		accessor->read_next_page += STS_CHUNK_PAGES;
		this_chunk_size = Min(remaining_size,
							  BLCKSZ * STS_CHUNK_PAGES -
							  STS_CHUNK_HEADER_SIZE);
		if (BufFileRead(accessor->read_file,
						destination,
						this_chunk_size) != this_chunk_size)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from shared tuplestore temporary file"),
					 errdetail_internal("Short read while reading tuple.")));
		accessor->read_bytes += this_chunk_size;
		remaining_size -= this_chunk_size;
		destination += this_chunk_size;

		/*
		 * These will be used to count regular tuples following the oversized
		 * tuple that spilled into this overflow chunk.
		 */
		accessor->read_ntuples = 0;
		accessor->read_ntuples_available = chunk_header.ntuples;
	}

	tuple = (MinimalTuple) accessor->read_buffer;
	tuple->t_len = size;

	return tuple;
}

static MinimalTuple get_next_mintup(BufFile *file, void *meta_data, size_t meta_data_size)
{
	uint32 size;
	MinimalTuple tuple;
	size_t nread;
	if ((BufFileRead(file, meta_data, meta_data_size)) != meta_data_size)
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("could not read from shared tuplestore temporary file"),
						errdetail_internal("Short read while reading meta-data.")));
	if ((BufFileRead(file, &size, sizeof(size))) != sizeof(size))
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("could not read from shared tuplestore temporary file"),
						errdetail_internal("Short read while reading size.")));
	tuple = (MinimalTuple) palloc(size);
	tuple->t_len = size;
	nread = BufFileRead(file,
						(void *) ((char *) tuple + sizeof(uint32)),
						size - sizeof(uint32));
	if (nread != size - sizeof(uint32))
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("could not read from hash-join temporary file: %m")));
	return tuple;
}
static BufFile *rewindOuterMatchStatus(BufFile *bufFile)
{
	if (bufFile == NULL)
		return NULL;
	if (BufFileSeek(bufFile, 0, 0L, SEEK_SET))
		ereport(ERROR,
				(errcode_for_file_access(),
						errmsg("could not rewind hash-join temporary file: %m")));
	return bufFile;
}

void
populate_outer_match_statuses(SharedTuplestoreAccessor *accessor, BufFile *outer_match_statuses[])
{
	int j = 0;
	for (int i = 0; i < accessor->sts->nparticipants; i++)
	{
		char bitmap_filename[MAXPGPATH];
		sts_bitmap_filename(bitmap_filename, accessor, i);
		BufFile *file = BufFileOpenSharedIfExists(accessor->fileset, bitmap_filename);
		if (file != NULL)
			outer_match_statuses[j++] = file;
	}
}

void
close_outer_match_statuses(SharedTuplestoreAccessor *accessor, BufFile *outer_match_statuses[], int length)
{
	for (int i = 0; i < length; i++)
	{
		BufFileClose(outer_match_statuses[i]);
	}
}


/*
 * Only the last worker will be calling this
 */
void
combine_outer_match_statuses(SharedTuplestoreAccessor *accessor, BufFile *outer_match_statuses[], int length, size_t num_bytes, int batchno, BufFile **combined_bitmap_file)
{
	BufFile *first_file = outer_match_statuses[0];
	// make an output file for now
	BufFile *combined_file = *combined_bitmap_file;

	unsigned char current_byte_to_write = 0;
	unsigned char current_byte_to_read = 0;
	for(int64 cur = 0; cur < num_bytes; cur++) // make it while not EOF
	{
		BufFileRead(first_file, &current_byte_to_write, 1);
		BufFile *file1 = NULL;
		for (int k = 1; k < length; k++)
		{
			file1 = outer_match_statuses[k];
			elog(NOTICE, "in combine_outer_match_statuses. batchno %i. pid %i.", batchno, MyProcPid);
			BufFileRead(file1, &current_byte_to_read, 1);
			current_byte_to_write = current_byte_to_write | current_byte_to_read;
		}
		BufFileWrite(combined_file, &current_byte_to_write, 1);
	}

	rewindOuterMatchStatus(combined_file);
	size_t bytes_used = BufFileBytesUsed(combined_file);
	if (bytes_used > 0)
	{
		elog(LOG, "numbytes is %ld", bytes_used);
		char buf[MAXPGPATH];
		BufFileRead(combined_file, buf, bytes_used);
		// TODO: fix the name generation code
		for (int64 cur = 0; cur < bytes_used; cur++)
			elog(LOG, "outer match status file %hhu", buf[cur]);
	}
}

/*
 * One worker will loop through all of the outer match status files and combine them into a single bitmap
 * then it will loop through the outer batch file and emit tuples based on the match status in the bitmap
 */
void
print_tuplenums(SharedTuplestoreAccessor *accessor, BufFile *outer_match_statuses[], int length, size_t num_bytes, int batchno)
{
	MinimalTuple tuple;
	bool flag = false;

	// TODO: do I need to go through all read_files (in each participant?) to make sure I am getting all the tuples?

	BufFile *combined_bitmap_file = BufFileCreateTemp(false);
	combine_outer_match_statuses(accessor, outer_match_statuses, length, num_bytes, batchno, &combined_bitmap_file);
	rewindOuterMatchStatus(combined_bitmap_file);
	// TODO: can I do something better since I know only participants attached to the barrier will be here for now?
	for (int i = 0; i < accessor->sts->nparticipants; i++)
	{
		bool file_present = false;
		uint32 final_tuplenum = sts_gettuplenum(accessor);
		BufFile *read_file;
		tupleMetadata metadata;

		char name[MAXPGPATH];
		sts_filename(name, accessor, i);
		read_file = BufFileOpenSharedIfExists(accessor->fileset, name);
		if (read_file == NULL) {
			elog(LOG, "pid %i. batchno %s. participant %i file missing.", MyProcPid, name, i);
			continue;
		}
		else {
			flag = true;
		}
		SharedTuplestoreChunk chunkheader;
		if (BufFileSeek(read_file, 0, 0L, SEEK_SET))
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("could not rewind shared outer temporary file: %m")));

		if (BufFileRead(read_file, &chunkheader, sizeof(SharedTuplestoreChunk)) != sizeof(SharedTuplestoreChunk))
			ereport(ERROR,
					(errcode_for_file_access(),
							errmsg("could not read shared tuplestore chunk header: %m")));
		for(int j = 0; j < chunkheader.ntuples; j++)
		{
			tuple = get_next_mintup(read_file, &metadata, accessor->sts->meta_data_size);
			// TODO: find a way to get the tuple's value

			if (combined_bitmap_file != NULL)
			{
				file_present = true;
				int bytenum = (metadata.tuplenum) / 8;
				unsigned char bit = (metadata.tuplenum) % 8;
				unsigned char byte_to_check = 0;

				elog(DEBUG1, "bytenum is %i.", bytenum);
				// seek to byte to check
				if (BufFileSeek(combined_bitmap_file, 0, bytenum, SEEK_SET))
					ereport(ERROR,
							(errcode_for_file_access(),
									errmsg("could not rewind shared outer temporary file: %m")));
				// read byte containing ntuple bit
				if (BufFileRead(combined_bitmap_file, &byte_to_check, 1) == 0)
					ereport(ERROR,
							(errcode_for_file_access(),
									errmsg("could not read byte in outer match status bitmap: %m. batchno %i", batchno)));
				// if bit is set
				bool match = false;
				if (((byte_to_check) >> bit) & 1)
				{
					elog(DEBUG1, "bit is set");
					match = true;
				}

				elog(LOG, "ExecParallelHashJoinNewBatch. tupleid: %i. match_status %i. read_byteval %hhu. bytenum %i. bitnum %hhu. sts_filename %s. pid %i.",
					 metadata.tuplenum, match, byte_to_check, bytenum, bit, name, MyProcPid);
				elog(LOG,
					 "ProbeEnd. batchno %s. final_tuplenum %i. tupleid %i. tupleval ?. match_status %i. bytenum %i. read_byteval %hhu. bitnum %i. pid %i.",
					 name,
					 final_tuplenum,
					 metadata.tuplenum,
					 match,
					 bytenum,
					 byte_to_check,
					 bit,
					 MyProcPid);

			}
			else
			{
				elog(LOG, "ExecParallelHashJoinNewBatch. tupleid: %i. outermatchstatus file is NULL. sts_filename %s. pid %i.",
					 metadata.tuplenum, name, MyProcPid);
			}
			elog(LOG, "ExecParallelHashJoinNewBatch. tupleid: %i. sts_filename %s. pid %i.",
				 metadata.tuplenum, name, MyProcPid);
		}
		BufFileClose(read_file);
		elog(LOG, "pid %i. batchno %s. outermatchstatus for participant %i is %i.", MyProcPid, name, i, file_present);
	}
	if (flag == false)
		elog(LOG, "all batch files empty for batchno %i. pid %i.", batchno, MyProcPid);
	BufFileClose(combined_bitmap_file);
}

/*
 * Get the next tuple in the current parallel scan.
 */
MinimalTuple
sts_parallel_scan_next(SharedTuplestoreAccessor *accessor, void *meta_data, bool is_outer)
{
	SharedTuplestoreParticipant *p;
	BlockNumber read_page;
	bool		eof;

	for (;;)
	{
		/* Can we read more tuples from the current chunk? */
		if (accessor->read_ntuples < accessor->read_ntuples_available)
			return sts_read_tuple(accessor, meta_data);

		/* Find the location of a new chunk to read. */
		p = &accessor->sts->participants[accessor->read_participant];

		LWLockAcquire(&p->lock, LW_EXCLUSIVE);
		/* We can skip directly past overflow pages we know about. */
		if (p->read_page < accessor->read_next_page)
			p->read_page = accessor->read_next_page;
		eof = p->read_page >= p->npages;
		if (!eof)
		{
			/* Claim the next chunk. */
			read_page = p->read_page;
			/* Advance the read head for the next reader. */
			p->read_page += STS_CHUNK_PAGES;
			accessor->read_next_page = p->read_page;
		}
		LWLockRelease(&p->lock);

		if (!eof)
		{
			SharedTuplestoreChunk chunk_header;

			/* Make sure we have the file open. */
			if (accessor->read_file == NULL)
			{
				char		name[MAXPGPATH];

				sts_filename(name, accessor, accessor->read_participant);
				elog(NOTICE, "sts_parallel_scan_next. participant %i. opening read file %s. pid %i. is_outer: %i",
						accessor->participant, name, MyProcPid, is_outer);
				accessor->read_file =
						BufFileOpenShared(accessor->fileset, name, is_outer);
			}

			/* Seek and load the chunk header. */
			if (BufFileSeekBlock(accessor->read_file, read_page) != 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from shared tuplestore temporary file"),
						 errdetail_internal("Could not seek to next block.")));
			if (BufFileRead(accessor->read_file, &chunk_header,
							STS_CHUNK_HEADER_SIZE) != STS_CHUNK_HEADER_SIZE)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from shared tuplestore temporary file"),
						 errdetail_internal("Short read while reading chunk header.")));

			/*
			 * If this is an overflow chunk, we skip it and any following
			 * overflow chunks all at once.
			 */
			if (chunk_header.overflow > 0)
			{
				accessor->read_next_page = read_page +
					chunk_header.overflow * STS_CHUNK_PAGES;
				continue;
			}

			accessor->read_ntuples = 0;
			accessor->read_ntuples_available = chunk_header.ntuples;
			accessor->read_bytes = STS_CHUNK_HEADER_SIZE;

			/* Go around again, so we can get a tuple from this chunk. */
		}
		else
		{
			if (accessor->read_file != NULL)
			{
				BufFileClose(accessor->read_file);
				accessor->read_file = NULL;
			}

			/*
			 * Try the next participant's file.  If we've gone full circle,
			 * we're done.
			 */
			accessor->read_participant = (accessor->read_participant + 1) %
				accessor->sts->nparticipants;
			if (accessor->read_participant == accessor->participant)
				break;
			accessor->read_next_page = 0;

			/* Go around again, so we can get a chunk from this file. */
		}
	}

	return NULL;
}

SharedFileSet *sts_get_fileset(SharedTuplestoreAccessor *accessor)
{
	return accessor->fileset;
}
void
sts_bitmap_filename(char *name, SharedTuplestoreAccessor *accessor, int participant)
{
	snprintf(name, MAXPGPATH, "%s.p%d.bitmap", accessor->sts->name, participant);
}

/*
 * Create the name used for the BufFile that a given participant will write.
 */
static void
sts_filename(char *name, SharedTuplestoreAccessor *accessor, int participant)
{
	snprintf(name, MAXPGPATH, "%s.p%d", accessor->sts->name, participant);
}
