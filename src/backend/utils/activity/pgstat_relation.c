/* -------------------------------------------------------------------------
 *
 * pgstat_relation.c
 *	  Implementation of relation statistics.
 *
 * This file contains the implementation of function relation. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_relation.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/partition.h"
#include "postmaster/autovacuum.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "catalog/catalog.h"


/* Record that's written to 2PC state file when pgstat state is persisted */
typedef struct TwoPhasePgStatRecord
{
	PgStat_Counter tuples_inserted; /* tuples inserted in xact */
	PgStat_Counter tuples_updated;	/* tuples updated in xact */
	PgStat_Counter tuples_deleted;	/* tuples deleted in xact */
	/* tuples i/u/d prior to truncate/drop */
	PgStat_Counter inserted_pre_truncdrop;
	PgStat_Counter updated_pre_truncdrop;
	PgStat_Counter deleted_pre_truncdrop;
	Oid			id;				/* table's OID */
	bool		shared;			/* is it a shared catalog? */
	bool		truncdropped;	/* was the relation truncated/dropped? */
} TwoPhasePgStatRecord;


static PgStat_TableStatus *pgstat_prep_relation_pending(Oid rel_id, bool isshared);
static void add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level);
static void ensure_tabstat_xact_level(PgStat_TableStatus *pgstat_info);
static void save_truncdrop_counters(PgStat_TableXactStatus *trans, bool is_drop);
static void restore_truncdrop_counters(PgStat_TableXactStatus *trans);

static void pgstat_combine_vacuum_stats(PgStat_Frz *next, PgStat_Frz *oldest);

static float pgstat_frz_error_rate_internal(PgStat_StatTabEntry *tabentry);

static XLogRecPtr target_page_freeze_duration_lsns( PgStat_StatTabEntry *tabentry,
		TimestampTz current_time,
		XLogRecPtr current_lsn);


static void vac_stats_regress(PgStat_Unfrz *unfreezes, int nunfreezes,
		float *b, float *m);

/*
 * Copy stats between relations. This is used for things like REINDEX
 * CONCURRENTLY.
 */
void
pgstat_copy_relation_stats(Relation dst, Relation src)
{
	PgStat_StatTabEntry *srcstats;
	PgStatShared_Relation *dstshstats;
	PgStat_EntryRef *dst_ref;

	srcstats = pgstat_fetch_stat_tabentry_ext(src->rd_rel->relisshared,
											  RelationGetRelid(src));
	if (!srcstats)
		return;

	dst_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION,
										  dst->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
										  RelationGetRelid(dst),
										  false);

	dstshstats = (PgStatShared_Relation *) dst_ref->shared_stats;
	dstshstats->stats = *srcstats;

	pgstat_unlock_entry(dst_ref);
}

/*
 * Initialize a relcache entry to count access statistics.  Called whenever a
 * relation is opened.
 *
 * We assume that a relcache entry's pgstat_info field is zeroed by relcache.c
 * when the relcache entry is made; thereafter it is long-lived data.
 *
 * This does not create a reference to a stats entry in shared memory, nor
 * allocate memory for the pending stats. That happens in
 * pgstat_assoc_relation().
 */
void
pgstat_init_relation(Relation rel)
{
	char		relkind = rel->rd_rel->relkind;

	/*
	 * We only count stats for relations with storage and partitioned tables
	 */
	if (!RELKIND_HAS_STORAGE(relkind) && relkind != RELKIND_PARTITIONED_TABLE)
	{
		rel->pgstat_enabled = false;
		rel->pgstat_info = NULL;
		return;
	}

	if (!pgstat_track_counts)
	{
		if (rel->pgstat_info)
			pgstat_unlink_relation(rel);

		/* We're not counting at all */
		rel->pgstat_enabled = false;
		rel->pgstat_info = NULL;
		return;
	}

	rel->pgstat_enabled = true;
}

/*
 * Prepare for statistics for this relation to be collected.
 *
 * This ensures we have a reference to the stats entry before stats can be
 * generated. That is important because a relation drop in another connection
 * could otherwise lead to the stats entry being dropped, which then later
 * would get recreated when flushing stats.
 *
 * This is separate from pgstat_init_relation() as it is not uncommon for
 * relcache entries to be opened without ever getting stats reported.
 */
void
pgstat_assoc_relation(Relation rel)
{
	Assert(rel->pgstat_enabled);
	Assert(rel->pgstat_info == NULL);

	/* Else find or make the PgStat_TableStatus entry, and update link */
	rel->pgstat_info = pgstat_prep_relation_pending(RelationGetRelid(rel),
													rel->rd_rel->relisshared);

	/* don't allow link a stats to multiple relcache entries */
	Assert(rel->pgstat_info->relation == NULL);

	/* mark this relation as the owner */
	rel->pgstat_info->relation = rel;
}

/*
 * Break the mutual link between a relcache entry and pending stats entry.
 * This must be called whenever one end of the link is removed.
 */
void
pgstat_unlink_relation(Relation rel)
{
	/* remove the link to stats info if any */
	if (rel->pgstat_info == NULL)
		return;

	/* link sanity check */
	Assert(rel->pgstat_info->relation == rel);
	rel->pgstat_info->relation = NULL;
	rel->pgstat_info = NULL;
}

/*
 * Ensure that stats are dropped if transaction aborts.
 */
void
pgstat_create_relation(Relation rel)
{
	pgstat_create_transactional(PGSTAT_KIND_RELATION,
								rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
								RelationGetRelid(rel));
}

/*
 * Ensure that stats are dropped if transaction commits.
 */
void
pgstat_drop_relation(Relation rel)
{
	int			nest_level = GetCurrentTransactionNestLevel();
	PgStat_TableStatus *pgstat_info;

	pgstat_drop_transactional(PGSTAT_KIND_RELATION,
							  rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId,
							  RelationGetRelid(rel));

	if (!pgstat_should_count_relation(rel))
		return;

	/*
	 * Transactionally set counters to 0. That ensures that accesses to
	 * pg_stat_xact_all_tables inside the transaction show 0.
	 */
	pgstat_info = rel->pgstat_info;
	if (pgstat_info->trans &&
		pgstat_info->trans->nest_level == nest_level)
	{
		save_truncdrop_counters(pgstat_info->trans, true);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

/*
 * Given two adjacent PgStat_Frz, combine the data from the older PgStat_Frz
 * into the newer one. This is used when PgStat_StatTabEntry has filled and we
 * need to free up a spot for an imminent vacuum.
 */
static void
pgstat_combine_vacuum_stats(PgStat_Frz *next, PgStat_Frz *oldest)
{
	next->start_lsn = oldest->start_lsn;
	next->start_time = oldest->start_time;

	next->count = oldest->count + 1;
	// TODO: I left the target_page_freeze_duration_lsns to be the most recent
	// calculated one. don't think that is right

	next->sum_page_age_threshold += oldest->sum_page_age_threshold;

	if (oldest->freezes > 0)
	{
		next->freezes += oldest->freezes;

		/*
		 * We only track page age when freezing tuples on a page during
		 * vacuum, not when simply setting a page frozen in the VM.
		 */
		next->sum_page_age_lsns += oldest->sum_page_age_lsns;
		next->max_frz_page_age = Max(next->max_frz_page_age,
									 oldest->max_frz_page_age);

		next->min_frz_page_age = Min(next->min_frz_page_age,
									 oldest->min_frz_page_age);
	}

	next->min_page_age = Min(next->min_page_age, oldest->min_page_age);
	next->max_page_age = Max(next->max_page_age, oldest->max_page_age);

	next->vm_page_freezes += oldest->vm_page_freezes;

	if (oldest->unfreezes > 0)
	{
		next->unfreezes += oldest->unfreezes;
		next->early_unfreezes += oldest->early_unfreezes;
		next->total_frozen_duration_lsns += oldest->total_frozen_duration_lsns;
		next->max_frozen_duration_lsns = Max(next->max_frozen_duration_lsns,
											 oldest->max_frozen_duration_lsns);

		next->min_frozen_duration_lsns = Min(next->min_frozen_duration_lsns,
											 oldest->min_frozen_duration_lsns);
	}

	/*
	 * Though the total number of pages (and frozen pages) in the relation at
	 * the beginning and end of vacuum does not mean anything on its own when
	 * combined across entries, we use these numbers to calculate ratios, so
	 * we still must sum them.
	 */
	next->frozen_pages_end += oldest->frozen_pages_end;
	next->frozen_pages_start += oldest->frozen_pages_start;

	next->relsize_end += oldest->relsize_end;
	next->relsize_start += oldest->relsize_start;

	next->scanned_pages += oldest->scanned_pages;

	next->freeze_fpis += oldest->freeze_fpis;

	next->b_sum += oldest->b_sum;
	next->m_sum += oldest->m_sum;
}


/*
 * At the beginning of a vacuum, set up a PgStat_Frz. If there are no free
 * buckets in PgStat_StatTabEntry->frz_buckets, combine two PgStat_Frz entries
 * into a single bucket -- being mindful not to combine PgStat_Frz ending
 * before now - target_page_freeze_duration with those ending after.
 */
XLogRecPtr
pgstat_setup_vacuum_frz_stats(Oid tableoid, bool shared)
{
	PgStat_EntryRef *entry_ref;
	PgStat_StatTabEntry *tabentry;
	PgStat_Frz *current;
	XLogRecPtr	insert_lsn;
	XLogRecPtr page_age_threshold;
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);
	XLogRecPtr min_page_age;
	XLogRecPtr guc_lsns;
	TimestampTz ts = GetCurrentTimestamp();
	float err;

	if (!pgstat_track_counts)
		return 0;

	/* Use exact (not approximate) insert LSN at vacuum start/end */
	insert_lsn = GetXLogInsertRecPtr();

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION,
											dboid, tableoid, false);

	Assert(entry_ref != NULL && entry_ref->shared_stats != NULL);

	tabentry = &((PgStatShared_Relation *) entry_ref->shared_stats)->stats;

	current = &tabentry->frz_buckets[tabentry->frz_current];

	/*
	 * Calculate regression model based on just ended vacuum
	 */
	vac_stats_regress(tabentry->unfrzs, tabentry->nunfrz_used, &current->b_sum,
			&current->m_sum);

	min_page_age = tabentry->frz_nbuckets_used <= 0 ? InvalidXLogRecPtr :
		current->min_frz_page_age;

	/* Calculate our error rate so far. */
	err = pgstat_frz_error_rate_internal(tabentry);

	/* Calculate our recent LSN generation rate */
	guc_lsns = target_page_freeze_duration_lsns(tabentry, ts, insert_lsn);

	current->target_page_freeze_duration_lsns = guc_lsns;

	/*
	 * For the first vacuum, freeze all visible and all frozen. For subsequent
	 * vacuums, if there have been no unfreezes, decrease the threshold. If
	 * there are few errors, use the previous threshold. Otherwise, use the
	 * data from past unfreezes and the recent lsn generation rate to calculate
	 * a threshold.
	 */
	if (tabentry->frz_nbuckets_used == 0)
		page_age_threshold = 0;
	else if (tabentry->nunfrz_used <= 2)
		page_age_threshold = min_page_age * 0.8;
	else if (err <= 0.1)
		page_age_threshold = current->sum_page_age_threshold /
			current->count;
	else
		page_age_threshold = (guc_lsns - current->b_sum) /
			current->m_sum;

	/*
	 * While free buckets remain, simply use the next bucket for the next
	 * freeze period.
	 */
	if (tabentry->frz_nbuckets_used < VAC_FRZ_STATS_MAX_NBUCKETS)
	{
		tabentry->frz_current = tabentry->frz_nbuckets_used;
		tabentry->frz_nbuckets_used++;
	}
	else
	{
		PgStat_Frz *oldest;
		PgStat_Frz *next;
		int			next_idx;
		TimestampTz cutoff;

		/*
		 * We want pages to stay frozen for at least
		 * target_page_freeze_duration. If they are unfrozen before that, it
		 * is an early unfreeze. We want all earlier unfreezes to be correctly
		 * attributed to a vacuum. So, don't combine vacuums which ended
		 * before the cutoff with those that ended after. cutoff is how long
		 * ago a pages would be allowed to have been unfrozen to not be
		 * considered an early unfreeze.
		 */
		cutoff = ts - (target_page_freeze_duration * USECS_PER_SEC);

		next_idx = (tabentry->frz_oldest + 1) % VAC_FRZ_STATS_MAX_NBUCKETS;

		oldest = &tabentry->frz_buckets[tabentry->frz_oldest];
		next = &tabentry->frz_buckets[next_idx];

		/*
		 * If oldest is old enough but next is not old enough, we can't just
		 * combine them. instead combine next and next next, then copy oldest
		 * into next.
		 */
		if (oldest->end_time < cutoff && next->end_time > cutoff)
		{
			int			next_next_idx = (next_idx + 1) % VAC_FRZ_STATS_MAX_NBUCKETS;
			PgStat_Frz *next_next = &tabentry->frz_buckets[next_next_idx];

			pgstat_combine_vacuum_stats(next_next, next);

			memcpy(next, oldest, sizeof(PgStat_Frz));
		}
		else
			pgstat_combine_vacuum_stats(next, oldest);

		tabentry->frz_current = tabentry->frz_oldest;
		tabentry->frz_oldest = next_idx;
	}

	Assert(tabentry->frz_current < VAC_FRZ_STATS_MAX_NBUCKETS);
	Assert(tabentry->frz_oldest < VAC_FRZ_STATS_MAX_NBUCKETS);

	current = &tabentry->frz_buckets[tabentry->frz_current];
	memset(current, 0, sizeof(PgStat_Frz));
	current->start_lsn = insert_lsn;
	current->count = 1;
	current->start_time = ts;
	current->sum_page_age_threshold = page_age_threshold;

	pgstat_unlock_entry(entry_ref);

	/*
	 * Flush IO stats at the beginning of the vacuum after setting the start
	 * time and start LSN for this vacuum. This ensures that pages that are
	 * unfrozen before the end of the vacuum are still attributed as an
	 * unfreeze to that vacuum.
	 */
	pgstat_flush_io(false);

	return page_age_threshold;
}

/*
 * The LSN generation rate in LSNs/microsecond starting from either database
 * startup time or the beginning of the previous vacuum and spanning to current
 * time.
 */
static XLogRecPtr
target_page_freeze_duration_lsns( PgStat_StatTabEntry *tabentry,
		TimestampTz current_time,
		XLogRecPtr current_lsn)
{
	PgStat_Frz *previous;
	int previous_idx;

	TimestampTz start_time;
	XLogRecPtr start_lsn;
	XLogRecPtr guc_lsns;

	int64 guc_usecs;

	int64		time_elapsed;
	int64		lsns_elapsed;

	if (tabentry->frz_nbuckets_used <= 1)
	{
		start_time = PgStartTime;
		start_lsn = PgStartLSN;
	}
	else
	{
		Assert(tabentry->frz_current >= 0);
		if (tabentry->frz_current == 0)
			previous_idx = VAC_FRZ_STATS_MAX_NBUCKETS - 1;
		else
			previous_idx = tabentry->frz_current - 1;

		previous = &tabentry->frz_buckets[previous_idx];

		/* Previous must be a completed freeze bucket */
		Assert(previous->start_lsn != InvalidXLogRecPtr &&
				previous->end_lsn != InvalidXLogRecPtr);

		start_time = previous->start_time;
		start_lsn = previous->start_lsn;
	}

	time_elapsed = current_time - start_time;
	lsns_elapsed = current_lsn - start_lsn;

	/* time_elapsed is in usecs so convert guc */
	guc_usecs = target_page_freeze_duration * USECS_PER_SEC;

	guc_lsns = (XLogRecPtr) ((float) lsns_elapsed / time_elapsed) * guc_usecs;

	return guc_lsns;
}

static void
vac_stats_regress(PgStat_Unfrz *unfreezes, int nunfreezes,
		float *b, float *m)
{
	float x_avg;
	float y_avg;
	/* page_age is x and frz_duration is y */
	double x_sum = 0;
	double y_sum = 0;
	float m_numerator = 0;
	float m_denom = 0;

	if (nunfreezes <= 2)
	{
		*b = 0;
		*m = 0;
		return;
	}

	for (int i = 0; i < nunfreezes; i++)
	{
		PgStat_Unfrz *cur = &unfreezes[i];
		x_sum += cur->page_age;
		y_sum += cur->frz_duration;
	}

	x_avg = (float) x_sum / nunfreezes;
	y_avg = (float) y_sum / nunfreezes;

	for (int i = 0; i < nunfreezes; i++)
	{
		PgStat_Unfrz *cur = &unfreezes[i];

		m_numerator +=
			(cur->page_age - x_avg) * (cur->frz_duration - y_avg);

		m_denom += (cur->page_age - x_avg) * (cur->page_age - x_avg);
	}

	*m = (float) m_numerator / m_denom;

	*b = (float) y_avg - ((*m) * x_avg);
}

float
pgstat_frz_error_rate_internal(PgStat_StatTabEntry *tabentry)
{
	int64		early_unfreezes = 0;
	int64		freezes = 0;
	bool		skip_oldest;
	int			i;
	/*
	 * When calculating the freeze error rate, once we have used all available
	 * buckets, we restrict the calculation to exclude the oldest period to
	 * avoid a feedback loop where a high error rate causes us to freeze less
	 * and then because we freeze less we have less new data causing the old
	 * data to be over-represented. This could be done with a weighted average
	 * but, for now, simply exclude the oldest entry.
	 */
	skip_oldest = tabentry->frz_nbuckets_used >= VAC_FRZ_STATS_MAX_NBUCKETS;

	for (i = skip_oldest; i < tabentry->frz_nbuckets_used; i++)
	{
		PgStat_Frz *frz;

		frz = &tabentry->frz_buckets[(tabentry->frz_oldest + i) % VAC_FRZ_STATS_MAX_NBUCKETS];

		/*
		 * We don't include the current freeze period in our error rate rate
		 * calculation, as we have yet to see the consequences of those freeze
		 * decisions and including it will skew the numbers.
		 */
		if (frz->start_lsn == InvalidXLogRecPtr ||
			frz->end_lsn == InvalidXLogRecPtr)
			continue;

		/*
		 * We use vm_page_freezes, even though those are not all subject to
		 * the opportunistic freezing algorithm (and thus not in our power to
		 * decide whether or not to freeze) because we cannot distinguish
		 * between an unfreeze of a page that was opportunistically frozen and
		 * one that was marked frozen in the VM because it happened to qualify
		 * as all frozen.
		 */
		freezes += frz->vm_page_freezes;

		/*
		 * We care only about early_unfreezes, those pages which were unfrozen
		 * before target_page_freeze_duration had elapsed.
		 */
		early_unfreezes += frz->early_unfreezes;
	}

	Assert(early_unfreezes <= freezes);

	if (freezes == 0)
		return 0;

	return (float) early_unfreezes / freezes;
}


/*
 * Calculate the average error rate for recorded freeze periods.
 *
 *  MTODO: should this be done in the cached backend local copy or the copy in
 *  shared memory.
 */
float
pgstat_frz_error_rate(Oid tableoid)
{
	PgStat_StatTabEntry *tabentry;

	if (!pgstat_track_counts)
		return 0;

	if ((tabentry = pgstat_fetch_stat_tabentry(tableoid)) == NULL)
		return 0;

	if (tabentry->frz_nbuckets_used == 0)
		return 0;

	return pgstat_frz_error_rate_internal(tabentry);
}

/* void pick_samples(double mean, double stddev, size_t n, double *result) { */
/* 	while (n > 0) { */
/* 		double u1 = rand(); */
/* 		double u2 = rand(); */

/* 		double pi = 3.14; */

/* 		double z1 = sqrt(-2 * ln(u1)) * cos(2 * pi * u2); */
/* 		z1 = mean + stddev * z1; */

/* 		double z2 = sqrt(-2 * ln(u1)) * sin(2 * pi * u2); */
/* 		z2 = mean + stddev * z2; */

/* 		result[--n] = z1; */
/* 		result[--n] = z2; */
/* 	} */

/* 	sort(result); */
/* } */

/* void */
/* pgstat_count_page_frz() */
/* { */
/* 	double u1 = rand(); */
/* 	double u2 = rand(); */
/* 	double z1 = sqrt(-2 * ln(u1)) * cos(2 * pi * u2); */
/* 	double z2 = sqrt(-2 * ln(u1)) * sin(2 * pi * u2); */
/* } */

/*
 * MTODO: if checksums are on, we can bail out of the loop when start lsn >
 * page_lsn and use the page_lsn to calculate the all visible duration for the
 * page.
 */
void
pgstat_count_page_unvis(Oid tableoid, bool shared,
						   XLogRecPtr page_lsn, XLogRecPtr insert_lsn)
{
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);
	PgStat_EntryRef *entry_ref;
	PgStat_StatTabEntry *tabentry;

	if (!pgstat_track_counts)
		return;

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION,
											dboid, tableoid, false);

	tabentry = &((PgStatShared_Relation *) entry_ref->shared_stats)->stats;

	for (int i = 0; i < tabentry->frz_nbuckets_used; i++)
	{
		PgStat_Frz *frz;
		XLogRecPtr	page_av_duration_lsns;
		XLogRecPtr	recent_lsn;
		int frz_idx = (tabentry->frz_oldest + i) % VAC_FRZ_STATS_MAX_NBUCKETS;

		frz = &tabentry->frz_buckets[frz_idx];

		/* no entry should have been added without a start lsn */
		Assert(frz->start_lsn != InvalidXLogRecPtr);


		if (frz->end_lsn != InvalidXLogRecPtr && frz->end_lsn < page_lsn)
			continue;

		/*
		 * We know the page was set all visible sometime between the start and
		 * end of this vacuum. Try using a value halfway in between
		 */
		if (frz->end_lsn == InvalidXLogRecPtr)
			recent_lsn = insert_lsn;
		else
			recent_lsn = frz->end_lsn;

		page_av_duration_lsns = insert_lsn - ((frz->start_lsn + recent_lsn) / 2);

		frz->unavs++;

		frz->sum_av_duration_lsns += page_av_duration_lsns;

		frz->sum_sq_av_duration_lsns += (page_av_duration_lsns * page_av_duration_lsns);

		if (page_av_duration_lsns > frz->target_page_freeze_duration_lsns)
			frz->missed_freezes++;

		break;
	}

	tabentry->most_recent_page_mod =
		Min(tabentry->most_recent_page_mod, insert_lsn);

	/*
	 * If the page is older than any of our currently tracked vacuums, we
	 * aren't going to count it. We are only concerned with the efficacy of
	 * our more recent vacuums. If a very old page is being unfrozen, that is
	 * fine anyway.
	 */

	pgstat_unlock_entry(entry_ref);
}


/*
 * When a frozen page from a table with oid tableoid is modified, the page LSN
 * before modification is passed into this function. This LSN is used to
 * identify which bucket contains stats from the freeze period in which this
 * page was frozen. Then that bucket's unfreeze counter incremented. If the
 * page did not stay frozen for target_page_freeze_duration amount of time, it
 * is also counted as an early unfreeze.
 *
 * MTODO: instead of accessing the table in shared memory, this should be
 * cached locally and refetched when counting an unfreeze which is newer than
 * any of its local recorded freeze periods.
 */
void
pgstat_count_page_unfreeze(Oid tableoid, bool shared,
						   XLogRecPtr page_lsn, XLogRecPtr insert_lsn)
{
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);
	PgStat_EntryRef *entry_ref;
	PgStat_StatTabEntry *tabentry;
	PgStat_Unfrz *unfrz;
	TimestampTz current_time;

	if (!pgstat_track_counts)
		return;

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION,
											dboid, tableoid, false);

	tabentry = &((PgStatShared_Relation *) entry_ref->shared_stats)->stats;

	if (tabentry->nunfrz_used < VAC_NUM_UNFRZ_STATS)
	{
		tabentry->nunfrz_used++;
		tabentry->current_unfrz++;
	}
	else
		tabentry->current_unfrz = (tabentry->current_unfrz + 1) %
			VAC_NUM_UNFRZ_STATS;

	unfrz = &tabentry->unfrzs[tabentry->current_unfrz];

	current_time = GetCurrentTimestamp();

	/*
	 * Loop through the freeze stats stored in the ring, starting with the
	 * oldest. By starting with the oldest, and since they are in order, we
	 * know that we will run into the bucket containing the period in which
	 * our page was frozen.
	 */
	for (int i = 0; i < tabentry->frz_nbuckets_used; i++)
	{
		PgStat_Frz *frz;
		XLogRecPtr	end_lsn;
		TimestampTz end_time;
		int64		time_elapsed;
		int64		lsns_elapsed;
		int64		frz_lsn;
		int64		page_frz_time;
		int64		page_frz_duration_usec;
		XLogRecPtr	page_frz_duration_lsns;
		int frz_idx = (tabentry->frz_oldest + i) % VAC_FRZ_STATS_MAX_NBUCKETS;

		frz = &tabentry->frz_buckets[frz_idx];

		/* no entry should have been added without a start lsn */
		Assert(frz->start_lsn != InvalidXLogRecPtr);

		/*
		 * If the bucket starts after our page LSN, we know we have passed any
		 * freeze bucket containing the freeze period in which our page could
		 * have been frozen, so we are done.
		 */
		if (frz->start_lsn >= page_lsn)
			break;

		/*
		 * If this is a past sample (not a current, unfinished sample) and it
		 * ended before our page was frozen, we know our page was not frozen
		 * by this sample.
		 */
		if (frz->end_lsn != InvalidXLogRecPtr && frz->end_lsn < page_lsn)
			continue;

		/*
		 * We've found the bucket to which this page LSN belongs. If this
		 * entry isn't over, then let's use the current time as the end time
		 * for the purpose of calculation.
		 */
		if (frz->end_lsn == InvalidXLogRecPtr)
		{
			end_lsn = insert_lsn;
			end_time = current_time;
		}
		else
		{
			end_lsn = frz->end_lsn;
			end_time = frz->end_time;
		}

		/* Time in microseconds covered by the freeze bucket */
		time_elapsed = end_time - frz->start_time;
		/* LSNs covered by the freeze bucket */
		lsns_elapsed = end_lsn - frz->start_lsn;

		/* How many LSNs into the bucket was the page frozen */
		frz_lsn = page_lsn - frz->start_lsn;

		/*
		 * Time that corresponds to page LSN at which the page was frozen;
		 * basically the time at which the page was frozen
		 */
		page_frz_time = (float) frz_lsn /
			lsns_elapsed * time_elapsed + frz->start_time;

		/* amount of time page stayed frozen (in microseconds) */
		page_frz_duration_usec = current_time - page_frz_time;

		/*
		 * Depending on the LSN generation rate, if the page was frozen close
		 * to the end of the bucket, page_frz_duration_usec may be negative.
		 */
		page_frz_duration_usec = Max(page_frz_duration_usec, 0);

		page_frz_duration_lsns = insert_lsn - page_lsn;

		unfrz->unfreeze_lsn = insert_lsn;
		unfrz->frz_duration = page_frz_duration_lsns;

		/*
		 * If the page stayed frozen less than target page freeze duration, it
		 * is an early unfreeze. Note that target_page_freeze_duration is in
		 * seconds.
		 * TODO: try alternate way which is page_frz_duration_lsns <
		 * frz->target_page_freeze_duration_lsns
		 */
		if (page_frz_duration_usec < target_page_freeze_duration * USECS_PER_SEC)
		{

			XLogRecPtr upper_bound;
			XLogRecPtr lower_bound;

			frz->early_unfreezes++;

			/*
			 * Because the page was frozen by vac, frz, the newest it could
			 * have been is greater of the threshold of that vac and min page
			 * age. The oldest it could be is the oldest page frozen by that
			 * vac. Because we were wrong, let's use the lower bound of how old
			 * the page could have been to correct for that and move us toward
			 * freezing less.
			 */
			lower_bound = frz->sum_page_age_threshold / frz->count;
			lower_bound = Min(frz->min_frz_page_age, lower_bound);
			upper_bound = frz->max_frz_page_age;

			/* pick a point halfway between the two */
			unfrz->page_age = (XLogRecPtr )((float) (upper_bound - lower_bound) / 2);
			unfrz->page_age = Max(unfrz->page_age, 0);

			unfrz->page_age = lower_bound;
		}
		else
		{
			/*
			 * The model was right, so calculate page age given freeze duration
			 */
			unfrz->page_age = (frz->target_page_freeze_duration_lsns
					- (frz->b_sum / frz->count)) /
				(frz->m_sum / frz->count);
		}

		frz->unfreezes++;

		frz->total_frozen_duration_lsns += page_frz_duration_lsns;

		if (frz->max_frozen_duration_lsns == InvalidXLogRecPtr ||
			page_frz_duration_lsns > frz->max_frozen_duration_lsns)
			frz->max_frozen_duration_lsns = page_frz_duration_lsns;

		if (frz->min_frozen_duration_lsns == InvalidXLogRecPtr ||
			page_frz_duration_lsns < frz->min_frozen_duration_lsns)
			frz->min_frozen_duration_lsns = page_frz_duration_lsns;

		break;
	}

	tabentry->most_recent_page_mod =
		Min(tabentry->most_recent_page_mod, insert_lsn);

	/*
	 * If the page is older than any of our currently tracked vacuums, we
	 * aren't going to count it. We are only concerned with the efficacy of
	 * our more recent vacuums. If a very old page is being unfrozen, that is
	 * fine anyway.
	 */

	pgstat_unlock_entry(entry_ref);
}


/*
 * Report that the table was just vacuumed and flush IO statistics.
 *
 * vacrel is an input parameter only and will not be modified by
 * pgstat_report_vacuum().
 */
void
pgstat_report_vacuum(Oid tableoid, bool shared, LVRelState *vacrel,
					 BlockNumber orig_rel_pages,
					 BlockNumber new_rel_all_frozen)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_Relation *shtabentry;
	PgStat_StatTabEntry *tabentry;
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);
	TimestampTz ts;
	XLogRecPtr	end_lsn;
	PgStat_Frz *vacstat;

	if (!pgstat_track_counts)
		return;

	/* Store the data in the table's hash table entry. */
	ts = GetCurrentTimestamp();

	/* Don't use an approximate insert LSN for vacuum start and end */
	end_lsn = GetXLogInsertRecPtr();

	/* block acquiring lock for the same reason as pgstat_report_autovac() */
	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION,
											dboid, tableoid, false);

	shtabentry = (PgStatShared_Relation *) entry_ref->shared_stats;
	tabentry = &shtabentry->stats;

	tabentry->live_tuples = Max(vacrel->new_live_tuples, 0);

	/*
	 * Deliberately avoid telling the stats system about LP_DEAD items that
	 * remain in the table due to VACUUM bypassing index and heap vacuuming.
	 * ANALYZE will consider the remaining LP_DEAD items to be dead "tuples".
	 * It seems like a good idea to err on the side of not vacuuming again too
	 * soon in cases where the failsafe prevented significant amounts of heap
	 * vacuuming.
	 */
	tabentry->dead_tuples = vacrel->recently_dead_tuples +
		vacrel->missed_dead_tuples;

	vacstat = &tabentry->frz_buckets[tabentry->frz_current];
	vacstat->end_lsn = end_lsn;
	vacstat->end_time = ts;

	vacstat->scanned_pages = vacrel->scanned_pages;
	vacstat->relsize_end = vacrel->rel_pages;
	vacstat->relsize_start = orig_rel_pages;

	vacstat->frozen_pages_end = new_rel_all_frozen;
	vacstat->frozen_pages_start = vacrel->already_frozen_pages;

	vacstat->freezes = vacrel->pages_frozen;
	vacstat->sum_page_age_lsns = vacrel->sum_frozen_page_ages;
	vacstat->vm_page_freezes = vacrel->vm_pages_frozen;
	vacstat->max_frz_page_age = vacrel->max_frz_page_age;
	vacstat->min_frz_page_age = vacrel->min_frz_page_age;
	vacstat->min_page_age = vacrel->min_page_age;
	vacstat->max_page_age = vacrel->max_page_age;
	vacstat->freeze_fpis = vacrel->freeze_fpis;

	/*
	 * It is quite possible that a non-aggressive VACUUM ended up skipping
	 * various pages, however, we'll zero the insert counter here regardless.
	 * It's currently used only to track when we need to perform an "insert"
	 * autovacuum, which are mainly intended to freeze newly inserted tuples.
	 * Zeroing this may just mean we'll not try to vacuum the table again
	 * until enough tuples have been inserted to trigger another insert
	 * autovacuum.  An anti-wraparound autovacuum will catch any persistent
	 * stragglers.
	 */
	tabentry->ins_since_vacuum = 0;

	if (IsAutoVacuumWorkerProcess())
	{
		tabentry->last_autovacuum_time = ts;
		tabentry->autovacuum_count++;
	}
	else
	{
		tabentry->last_vacuum_time = ts;
		tabentry->vacuum_count++;
	}

	pgstat_unlock_entry(entry_ref);

	/*
	 * Flush IO statistics now. pgstat_report_stat() will flush IO stats,
	 * however this will not be called until after an entire autovacuum cycle
	 * is done -- which will likely vacuum many relations -- or until the
	 * VACUUM command has processed all tables and committed.
	 */
	pgstat_flush_io(false);
}

/*
 * Report that the table was just analyzed and flush IO statistics.
 *
 * Caller must provide new live- and dead-tuples estimates, as well as a
 * flag indicating whether to reset the mod_since_analyze counter.
 */
void
pgstat_report_analyze(Relation rel,
					  PgStat_Counter livetuples, PgStat_Counter deadtuples,
					  bool resetcounter)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_Relation *shtabentry;
	PgStat_StatTabEntry *tabentry;
	Oid			dboid = (rel->rd_rel->relisshared ? InvalidOid : MyDatabaseId);

	if (!pgstat_track_counts)
		return;

	/*
	 * Unlike VACUUM, ANALYZE might be running inside a transaction that has
	 * already inserted and/or deleted rows in the target table. ANALYZE will
	 * have counted such rows as live or dead respectively. Because we will
	 * report our counts of such rows at transaction end, we should subtract
	 * off these counts from the update we're making now, else they'll be
	 * double-counted after commit.  (This approach also ensures that the
	 * shared stats entry ends up with the right numbers if we abort instead
	 * of committing.)
	 *
	 * Waste no time on partitioned tables, though.
	 */
	if (pgstat_should_count_relation(rel) &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
	{
		PgStat_TableXactStatus *trans;

		for (trans = rel->pgstat_info->trans; trans; trans = trans->upper)
		{
			livetuples -= trans->tuples_inserted - trans->tuples_deleted;
			deadtuples -= trans->tuples_updated + trans->tuples_deleted;
		}
		/* count stuff inserted by already-aborted subxacts, too */
		deadtuples -= rel->pgstat_info->counts.delta_dead_tuples;
		/* Since ANALYZE's counts are estimates, we could have underflowed */
		livetuples = Max(livetuples, 0);
		deadtuples = Max(deadtuples, 0);
	}

	/* block acquiring lock for the same reason as pgstat_report_autovac() */
	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RELATION, dboid,
											RelationGetRelid(rel),
											false);
	/* can't get dropped while accessed */
	Assert(entry_ref != NULL && entry_ref->shared_stats != NULL);

	shtabentry = (PgStatShared_Relation *) entry_ref->shared_stats;
	tabentry = &shtabentry->stats;

	tabentry->live_tuples = livetuples;
	tabentry->dead_tuples = deadtuples;

	/*
	 * If commanded, reset mod_since_analyze to zero.  This forgets any
	 * changes that were committed while the ANALYZE was in progress, but we
	 * have no good way to estimate how many of those there were.
	 */
	if (resetcounter)
		tabentry->mod_since_analyze = 0;

	if (IsAutoVacuumWorkerProcess())
	{
		tabentry->last_autoanalyze_time = GetCurrentTimestamp();
		tabentry->autoanalyze_count++;
	}
	else
	{
		tabentry->last_analyze_time = GetCurrentTimestamp();
		tabentry->analyze_count++;
	}

	pgstat_unlock_entry(entry_ref);

	/* see pgstat_report_vacuum() */
	pgstat_flush_io(false);
}

/*
 * count a tuple insertion of n tuples
 */
void
pgstat_count_heap_insert(Relation rel, PgStat_Counter n)
{
	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		ensure_tabstat_xact_level(pgstat_info);
		pgstat_info->trans->tuples_inserted += n;
	}
}

/*
 * count a tuple update
 */
void
pgstat_count_heap_update(Relation rel, bool hot, bool newpage)
{
	Assert(!(hot && newpage));

	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		ensure_tabstat_xact_level(pgstat_info);
		pgstat_info->trans->tuples_updated++;

		/*
		 * tuples_hot_updated and tuples_newpage_updated counters are
		 * nontransactional, so just advance them
		 */
		if (hot)
			pgstat_info->counts.tuples_hot_updated++;
		else if (newpage)
			pgstat_info->counts.tuples_newpage_updated++;
	}
}

/*
 * count a tuple deletion
 */
void
pgstat_count_heap_delete(Relation rel)
{
	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		ensure_tabstat_xact_level(pgstat_info);
		pgstat_info->trans->tuples_deleted++;
	}
}

/*
 * update tuple counters due to truncate
 */
void
pgstat_count_truncate(Relation rel)
{
	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		ensure_tabstat_xact_level(pgstat_info);
		save_truncdrop_counters(pgstat_info->trans, false);
		pgstat_info->trans->tuples_inserted = 0;
		pgstat_info->trans->tuples_updated = 0;
		pgstat_info->trans->tuples_deleted = 0;
	}
}

/*
 * update dead-tuples count
 *
 * The semantics of this are that we are reporting the nontransactional
 * recovery of "delta" dead tuples; so delta_dead_tuples decreases
 * rather than increasing, and the change goes straight into the per-table
 * counter, not into transactional state.
 */
void
pgstat_update_heap_dead_tuples(Relation rel, int delta)
{
	if (pgstat_should_count_relation(rel))
	{
		PgStat_TableStatus *pgstat_info = rel->pgstat_info;

		pgstat_info->counts.delta_dead_tuples -= delta;
	}
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * the collected statistics for one table or NULL. NULL doesn't mean
 * that the table doesn't exist, just that there are no statistics, so the
 * caller is better off to report ZERO instead.
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry(Oid relid)
{
	return pgstat_fetch_stat_tabentry_ext(IsSharedRelation(relid), relid);
}

/*
 * More efficient version of pgstat_fetch_stat_tabentry(), allowing to specify
 * whether the to-be-accessed table is a shared relation or not.
 */
PgStat_StatTabEntry *
pgstat_fetch_stat_tabentry_ext(bool shared, Oid reloid)
{
	Oid			dboid = (shared ? InvalidOid : MyDatabaseId);

	return (PgStat_StatTabEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_RELATION, dboid, reloid);
}

/*
 * find any existing PgStat_TableStatus entry for rel
 *
 * Find any existing PgStat_TableStatus entry for rel_id in the current
 * database. If not found, try finding from shared tables.
 *
 * If an entry is found, copy it and increment the copy's counters with their
 * subtransaction counterparts, then return the copy.  The caller may need to
 * pfree() the copy.
 *
 * If no entry found, return NULL, don't create a new one.
 */
PgStat_TableStatus *
find_tabstat_entry(Oid rel_id)
{
	PgStat_EntryRef *entry_ref;
	PgStat_TableXactStatus *trans;
	PgStat_TableStatus *tabentry = NULL;
	PgStat_TableStatus *tablestatus = NULL;

	entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, MyDatabaseId, rel_id);
	if (!entry_ref)
	{
		entry_ref = pgstat_fetch_pending_entry(PGSTAT_KIND_RELATION, InvalidOid, rel_id);
		if (!entry_ref)
			return tablestatus;
	}

	tabentry = (PgStat_TableStatus *) entry_ref->pending;
	tablestatus = palloc(sizeof(PgStat_TableStatus));
	*tablestatus = *tabentry;

	/*
	 * Reset tablestatus->trans in the copy of PgStat_TableStatus as it may
	 * point to a shared memory area.  Its data is saved below, so removing it
	 * does not matter.
	 */
	tablestatus->trans = NULL;

	/*
	 * Live subtransaction counts are not included yet.  This is not a hot
	 * code path so reconcile tuples_inserted, tuples_updated and
	 * tuples_deleted even if the caller may not be interested in this data.
	 */
	for (trans = tabentry->trans; trans != NULL; trans = trans->upper)
	{
		tablestatus->counts.tuples_inserted += trans->tuples_inserted;
		tablestatus->counts.tuples_updated += trans->tuples_updated;
		tablestatus->counts.tuples_deleted += trans->tuples_deleted;
	}

	return tablestatus;
}

/*
 * Perform relation stats specific end-of-transaction work. Helper for
 * AtEOXact_PgStat.
 *
 * Transfer transactional insert/update counts into the base tabstat entries.
 * We don't bother to free any of the transactional state, since it's all in
 * TopTransactionContext and will go away anyway.
 */
void
AtEOXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit)
{
	PgStat_TableXactStatus *trans;

	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat;

		Assert(trans->nest_level == 1);
		Assert(trans->upper == NULL);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);
		/* restore pre-truncate/drop stats (if any) in case of aborted xact */
		if (!isCommit)
			restore_truncdrop_counters(trans);
		/* count attempted actions regardless of commit/abort */
		tabstat->counts.tuples_inserted += trans->tuples_inserted;
		tabstat->counts.tuples_updated += trans->tuples_updated;
		tabstat->counts.tuples_deleted += trans->tuples_deleted;
		if (isCommit)
		{
			tabstat->counts.truncdropped = trans->truncdropped;
			if (trans->truncdropped)
			{
				/* forget live/dead stats seen by backend thus far */
				tabstat->counts.delta_live_tuples = 0;
				tabstat->counts.delta_dead_tuples = 0;
			}
			/* insert adds a live tuple, delete removes one */
			tabstat->counts.delta_live_tuples +=
				trans->tuples_inserted - trans->tuples_deleted;
			/* update and delete each create a dead tuple */
			tabstat->counts.delta_dead_tuples +=
				trans->tuples_updated + trans->tuples_deleted;
			/* insert, update, delete each count as one change event */
			tabstat->counts.changed_tuples +=
				trans->tuples_inserted + trans->tuples_updated +
				trans->tuples_deleted;
		}
		else
		{
			/* inserted tuples are dead, deleted tuples are unaffected */
			tabstat->counts.delta_dead_tuples +=
				trans->tuples_inserted + trans->tuples_updated;
			/* an aborted xact generates no changed_tuple events */
		}
		tabstat->trans = NULL;
	}
}

/*
 * Perform relation stats specific end-of-sub-transaction work. Helper for
 * AtEOSubXact_PgStat.
 *
 * Transfer transactional insert/update counts into the next higher
 * subtransaction state.
 */
void
AtEOSubXact_PgStat_Relations(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth)
{
	PgStat_TableXactStatus *trans;
	PgStat_TableXactStatus *next_trans;

	for (trans = xact_state->first; trans != NULL; trans = next_trans)
	{
		PgStat_TableStatus *tabstat;

		next_trans = trans->next;
		Assert(trans->nest_level == nestDepth);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);

		if (isCommit)
		{
			if (trans->upper && trans->upper->nest_level == nestDepth - 1)
			{
				if (trans->truncdropped)
				{
					/* propagate the truncate/drop status one level up */
					save_truncdrop_counters(trans->upper, false);
					/* replace upper xact stats with ours */
					trans->upper->tuples_inserted = trans->tuples_inserted;
					trans->upper->tuples_updated = trans->tuples_updated;
					trans->upper->tuples_deleted = trans->tuples_deleted;
				}
				else
				{
					trans->upper->tuples_inserted += trans->tuples_inserted;
					trans->upper->tuples_updated += trans->tuples_updated;
					trans->upper->tuples_deleted += trans->tuples_deleted;
				}
				tabstat->trans = trans->upper;
				pfree(trans);
			}
			else
			{
				/*
				 * When there isn't an immediate parent state, we can just
				 * reuse the record instead of going through a palloc/pfree
				 * pushup (this works since it's all in TopTransactionContext
				 * anyway).  We have to re-link it into the parent level,
				 * though, and that might mean pushing a new entry into the
				 * pgStatXactStack.
				 */
				PgStat_SubXactStatus *upper_xact_state;

				upper_xact_state = pgstat_get_xact_stack_level(nestDepth - 1);
				trans->next = upper_xact_state->first;
				upper_xact_state->first = trans;
				trans->nest_level = nestDepth - 1;
			}
		}
		else
		{
			/*
			 * On abort, update top-level tabstat counts, then forget the
			 * subtransaction
			 */

			/* first restore values obliterated by truncate/drop */
			restore_truncdrop_counters(trans);
			/* count attempted actions regardless of commit/abort */
			tabstat->counts.tuples_inserted += trans->tuples_inserted;
			tabstat->counts.tuples_updated += trans->tuples_updated;
			tabstat->counts.tuples_deleted += trans->tuples_deleted;
			/* inserted tuples are dead, deleted tuples are unaffected */
			tabstat->counts.delta_dead_tuples +=
				trans->tuples_inserted + trans->tuples_updated;
			tabstat->trans = trans->upper;
			pfree(trans);
		}
	}
}

/*
 * Generate 2PC records for all the pending transaction-dependent relation
 * stats.
 */
void
AtPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
{
	PgStat_TableXactStatus *trans;

	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat PG_USED_FOR_ASSERTS_ONLY;
		TwoPhasePgStatRecord record;

		Assert(trans->nest_level == 1);
		Assert(trans->upper == NULL);
		tabstat = trans->parent;
		Assert(tabstat->trans == trans);

		record.tuples_inserted = trans->tuples_inserted;
		record.tuples_updated = trans->tuples_updated;
		record.tuples_deleted = trans->tuples_deleted;
		record.inserted_pre_truncdrop = trans->inserted_pre_truncdrop;
		record.updated_pre_truncdrop = trans->updated_pre_truncdrop;
		record.deleted_pre_truncdrop = trans->deleted_pre_truncdrop;
		record.id = tabstat->id;
		record.shared = tabstat->shared;
		record.truncdropped = trans->truncdropped;

		RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0,
							   &record, sizeof(TwoPhasePgStatRecord));
	}
}

/*
 * All we need do here is unlink the transaction stats state from the
 * nontransactional state.  The nontransactional action counts will be
 * reported to the stats system immediately, while the effects on live and
 * dead tuple counts are preserved in the 2PC state file.
 *
 * Note: AtEOXact_PgStat_Relations is not called during PREPARE.
 */
void
PostPrepare_PgStat_Relations(PgStat_SubXactStatus *xact_state)
{
	PgStat_TableXactStatus *trans;

	for (trans = xact_state->first; trans != NULL; trans = trans->next)
	{
		PgStat_TableStatus *tabstat;

		tabstat = trans->parent;
		tabstat->trans = NULL;
	}
}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Load the saved counts into our local pgstats state.
 */
void
pgstat_twophase_postcommit(TransactionId xid, uint16 info,
						   void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = pgstat_prep_relation_pending(rec->id, rec->shared);

	/* Same math as in AtEOXact_PgStat, commit case */
	pgstat_info->counts.tuples_inserted += rec->tuples_inserted;
	pgstat_info->counts.tuples_updated += rec->tuples_updated;
	pgstat_info->counts.tuples_deleted += rec->tuples_deleted;
	pgstat_info->counts.truncdropped = rec->truncdropped;
	if (rec->truncdropped)
	{
		/* forget live/dead stats seen by backend thus far */
		pgstat_info->counts.delta_live_tuples = 0;
		pgstat_info->counts.delta_dead_tuples = 0;
	}
	pgstat_info->counts.delta_live_tuples +=
		rec->tuples_inserted - rec->tuples_deleted;
	pgstat_info->counts.delta_dead_tuples +=
		rec->tuples_updated + rec->tuples_deleted;
	pgstat_info->counts.changed_tuples +=
		rec->tuples_inserted + rec->tuples_updated +
		rec->tuples_deleted;
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * Load the saved counts into our local pgstats state, but treat them
 * as aborted.
 */
void
pgstat_twophase_postabort(TransactionId xid, uint16 info,
						  void *recdata, uint32 len)
{
	TwoPhasePgStatRecord *rec = (TwoPhasePgStatRecord *) recdata;
	PgStat_TableStatus *pgstat_info;

	/* Find or create a tabstat entry for the rel */
	pgstat_info = pgstat_prep_relation_pending(rec->id, rec->shared);

	/* Same math as in AtEOXact_PgStat, abort case */
	if (rec->truncdropped)
	{
		rec->tuples_inserted = rec->inserted_pre_truncdrop;
		rec->tuples_updated = rec->updated_pre_truncdrop;
		rec->tuples_deleted = rec->deleted_pre_truncdrop;
	}
	pgstat_info->counts.tuples_inserted += rec->tuples_inserted;
	pgstat_info->counts.tuples_updated += rec->tuples_updated;
	pgstat_info->counts.tuples_deleted += rec->tuples_deleted;
	pgstat_info->counts.delta_dead_tuples +=
		rec->tuples_inserted + rec->tuples_updated;
}

/*
 * Flush out pending stats for the entry
 *
 * If nowait is true, this function returns false if lock could not
 * immediately acquired, otherwise true is returned.
 *
 * Some of the stats are copied to the corresponding pending database stats
 * entry when successfully flushing.
 */
bool
pgstat_relation_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	static const PgStat_TableCounts all_zeroes;
	Oid			dboid;
	PgStat_TableStatus *lstats; /* pending stats entry  */
	PgStatShared_Relation *shtabstats;
	PgStat_StatTabEntry *tabentry;	/* table entry of shared stats */
	PgStat_StatDBEntry *dbentry;	/* pending database entry */

	dboid = entry_ref->shared_entry->key.dboid;
	lstats = (PgStat_TableStatus *) entry_ref->pending;
	shtabstats = (PgStatShared_Relation *) entry_ref->shared_stats;

	/*
	 * Ignore entries that didn't accumulate any actual counts, such as
	 * indexes that were opened by the planner but not used.
	 */
	if (memcmp(&lstats->counts, &all_zeroes,
			   sizeof(PgStat_TableCounts)) == 0)
	{
		return true;
	}

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

	/* add the values to the shared entry. */
	tabentry = &shtabstats->stats;

	tabentry->numscans += lstats->counts.numscans;
	if (lstats->counts.numscans)
	{
		TimestampTz t = GetCurrentTransactionStopTimestamp();

		if (t > tabentry->lastscan)
			tabentry->lastscan = t;
	}
	tabentry->tuples_returned += lstats->counts.tuples_returned;
	tabentry->tuples_fetched += lstats->counts.tuples_fetched;
	tabentry->tuples_inserted += lstats->counts.tuples_inserted;
	tabentry->tuples_updated += lstats->counts.tuples_updated;
	tabentry->tuples_deleted += lstats->counts.tuples_deleted;
	tabentry->tuples_hot_updated += lstats->counts.tuples_hot_updated;
	tabentry->tuples_newpage_updated += lstats->counts.tuples_newpage_updated;

	/*
	 * If table was truncated/dropped, first reset the live/dead counters.
	 */
	if (lstats->counts.truncdropped)
	{
		tabentry->live_tuples = 0;
		tabentry->dead_tuples = 0;
		tabentry->ins_since_vacuum = 0;
	}

	tabentry->live_tuples += lstats->counts.delta_live_tuples;
	tabentry->dead_tuples += lstats->counts.delta_dead_tuples;
	tabentry->mod_since_analyze += lstats->counts.changed_tuples;
	tabentry->ins_since_vacuum += lstats->counts.tuples_inserted;
	tabentry->blocks_fetched += lstats->counts.blocks_fetched;
	tabentry->blocks_hit += lstats->counts.blocks_hit;

	/* Clamp live_tuples in case of negative delta_live_tuples */
	tabentry->live_tuples = Max(tabentry->live_tuples, 0);
	/* Likewise for dead_tuples */
	tabentry->dead_tuples = Max(tabentry->dead_tuples, 0);

	pgstat_unlock_entry(entry_ref);

	/* The entry was successfully flushed, add the same to database stats */
	dbentry = pgstat_prep_database_pending(dboid);
	dbentry->tuples_returned += lstats->counts.tuples_returned;
	dbentry->tuples_fetched += lstats->counts.tuples_fetched;
	dbentry->tuples_inserted += lstats->counts.tuples_inserted;
	dbentry->tuples_updated += lstats->counts.tuples_updated;
	dbentry->tuples_deleted += lstats->counts.tuples_deleted;
	dbentry->blocks_fetched += lstats->counts.blocks_fetched;
	dbentry->blocks_hit += lstats->counts.blocks_hit;

	return true;
}

void
pgstat_relation_delete_pending_cb(PgStat_EntryRef *entry_ref)
{
	PgStat_TableStatus *pending = (PgStat_TableStatus *) entry_ref->pending;

	if (pending->relation)
		pgstat_unlink_relation(pending->relation);
}

/*
 * Find or create a PgStat_TableStatus entry for rel. New entry is created and
 * initialized if not exists.
 */
static PgStat_TableStatus *
pgstat_prep_relation_pending(Oid rel_id, bool isshared)
{
	PgStat_EntryRef *entry_ref;
	PgStat_TableStatus *pending;

	entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_RELATION,
										  isshared ? InvalidOid : MyDatabaseId,
										  rel_id, NULL);
	pending = entry_ref->pending;
	pending->id = rel_id;
	pending->shared = isshared;

	return pending;
}

/*
 * add a new (sub)transaction state record
 */
static void
add_tabstat_xact_level(PgStat_TableStatus *pgstat_info, int nest_level)
{
	PgStat_SubXactStatus *xact_state;
	PgStat_TableXactStatus *trans;

	/*
	 * If this is the first rel to be modified at the current nest level, we
	 * first have to push a transaction stack entry.
	 */
	xact_state = pgstat_get_xact_stack_level(nest_level);

	/* Now make a per-table stack entry */
	trans = (PgStat_TableXactStatus *)
		MemoryContextAllocZero(TopTransactionContext,
							   sizeof(PgStat_TableXactStatus));
	trans->nest_level = nest_level;
	trans->upper = pgstat_info->trans;
	trans->parent = pgstat_info;
	trans->next = xact_state->first;
	xact_state->first = trans;
	pgstat_info->trans = trans;
}

/*
 * Add a new (sub)transaction record if needed.
 */
static void
ensure_tabstat_xact_level(PgStat_TableStatus *pgstat_info)
{
	int			nest_level = GetCurrentTransactionNestLevel();

	if (pgstat_info->trans == NULL ||
		pgstat_info->trans->nest_level != nest_level)
		add_tabstat_xact_level(pgstat_info, nest_level);
}

/*
 * Whenever a table is truncated/dropped, we save its i/u/d counters so that
 * they can be cleared, and if the (sub)xact that executed the truncate/drop
 * later aborts, the counters can be restored to the saved (pre-truncate/drop)
 * values.
 *
 * Note that for truncate we do this on the first truncate in any particular
 * subxact level only.
 */
static void
save_truncdrop_counters(PgStat_TableXactStatus *trans, bool is_drop)
{
	if (!trans->truncdropped || is_drop)
	{
		trans->inserted_pre_truncdrop = trans->tuples_inserted;
		trans->updated_pre_truncdrop = trans->tuples_updated;
		trans->deleted_pre_truncdrop = trans->tuples_deleted;
		trans->truncdropped = true;
	}
}

/*
 * restore counters when a truncate aborts
 */
static void
restore_truncdrop_counters(PgStat_TableXactStatus *trans)
{
	if (trans->truncdropped)
	{
		trans->tuples_inserted = trans->inserted_pre_truncdrop;
		trans->tuples_updated = trans->updated_pre_truncdrop;
		trans->tuples_deleted = trans->deleted_pre_truncdrop;
	}
}
