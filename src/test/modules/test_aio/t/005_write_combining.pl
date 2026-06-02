# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('write-combining');
$node->init();

$node->append_conf(
	'postgresql.conf', qq(
shared_buffers = '128MB'
max_wal_size = '4GB'
checkpoint_timeout = '1h'
checkpoint_completion_target = 0.0
autovacuum = off
io_combine_limit = '128kB'
eager_clean_max_batch_size = 128
bgwriter_lru_maxpages = 0
fsync = off
synchronous_commit = off
));

$node->start();

$node->safe_psql('postgres', 'CREATE EXTENSION test_aio');
$node->safe_psql('postgres', 'CREATE EXTENSION pg_prewarm');

my $block_size = $node->safe_psql('postgres',
	"SELECT current_setting('block_size')::int");

test_bgwriter_combines_writes($node, $block_size);

$node->stop();

done_testing();

sub io_stat_writes
{
	my ($node, $backend_type, $context) = @_;

	my $result = $node->safe_psql(
		'postgres', qq(
SELECT COALESCE(sum(writes), 0)::bigint,
       COALESCE(sum(write_bytes), 0)::bigint,
       COALESCE((sum(write_bytes) / NULLIF(sum(writes), 0))::bigint, 0)
FROM pg_stat_io
WHERE backend_type = '$backend_type'
  AND object = 'relation'
  AND context = '$context';
));

	return split /\|/, $result;
}

sub assert_writes
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $label, $backend_type, $context, $expected_writes, $expected_bytes) = @_;
	my ($writes, $write_bytes, $avg_write_bytes) =
	  io_stat_writes($node, $backend_type, $context);

	diag "$label: writes=$writes write_bytes=$write_bytes avg_write_bytes=$avg_write_bytes";

	is($writes, $expected_writes, "$label write count");
	is($write_bytes, $expected_bytes, "$label write bytes");
}

sub assert_blocks_dirty
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $table, $blocks, $expected, $label) = @_;

	is($node->safe_psql('postgres',
		"SELECT true = ALL (rel_blocks_are_dirty('$table', ARRAY[$blocks]))"),
		$expected, $label);
}

sub assert_any_blocks_dirty
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($node, $table, $blocks, $expected, $label) = @_;

	is($node->safe_psql('postgres',
		"SELECT true = ANY (rel_blocks_are_dirty('$table', ARRAY[$blocks]))"),
		$expected, $label);
}

sub flush_and_reset_io_stats
{
	my ($node, $psql) = @_;

	$psql->query_safe('SELECT pg_stat_force_next_flush()');
	$node->safe_psql('postgres', "SELECT pg_stat_reset_shared('io')");
}

sub dirty_blocks
{
	my ($psql, $table, $blocks) = @_;

	$psql->query_safe(
		"SELECT prepare_rel_blocks_for_bgwriter('$table', ARRAY[$blocks])");
}

sub run_bgwriter_cleaner
{
	my $psql = shift;

	$psql->query_safe('SELECT run_bgwriter_cleaner(1000)');
	$psql->query_safe('SELECT pg_stat_force_next_flush()');
}

sub test_bgwriter_combines_writes
{
	my ($node, $block_size) = @_;
	my $psql = $node->background_psql('postgres', on_error_stop => 0);

	# Keep unrelated dirty buffers out of the client-backend write statistics.
	$node->safe_psql('postgres', 'CHECKPOINT');

	# We create a relation and then flush all of its data. We can then mark
	# buffers dirty for bgwriter without requiring WAL flush and thus preventing
	# opportunistic write combining.
	$node->safe_psql(
		'postgres', qq(
	CREATE TABLE wc_bgwriter (id int, payload text);
	INSERT INTO wc_bgwriter SELECT g, repeat('y', 200) FROM generate_series(1, 1000) AS g;
	SELECT flush_rel_buffers('wc_bgwriter'::regclass);
	));

	####
	# Test one big combined write
	####

	# Mark blocks 0-5 dirty
	dirty_blocks($psql, 'wc_bgwriter', '0,1,2,3,4,5');
	assert_blocks_dirty($node, 'wc_bgwriter', '0,1,2,3,4,5', 't',
		'contiguous buffers are dirty before bgwriter cleaner');

	flush_and_reset_io_stats($node, $psql);
	run_bgwriter_cleaner($psql);
	# Should have written one big write
	assert_writes($node, 'contiguous bgwriter cleaner', 'client backend',
		'normal', 1, 6 * $block_size);
	# None of those blocks should still be dirty
	assert_any_blocks_dirty($node, 'wc_bgwriter', '0,1,2,3,4,5', 'f',
		'bgwriter cleaner wrote contiguous dirty buffers');

	####
	# Test multiple single block writes when interspersed blocks are not in
	# shared buffers
	####

	# Evict every other block and mark the other blocks dirty
	$psql->query_safe(
		"SELECT invalidate_rel_blocks('wc_bgwriter', ARRAY[1,3,5])");
	dirty_blocks($psql, 'wc_bgwriter', '0,2,4');
	flush_and_reset_io_stats($node, $psql);

	run_bgwriter_cleaner($psql);
	# The three blocks that were dirty should have been written out in three
	# writes.
	assert_writes($node, 'nonresident gaps bgwriter cleaner', 'client backend',
		'normal', 3,
		3 * $block_size);
	# And none of them should be dirty anymore
	assert_any_blocks_dirty($node, 'wc_bgwriter', '0,2,4', 'f',
		'bgwriter cleaner wrote dirty buffers separated by nonresident gaps');

	####
	# Test two combined writes split around a pinned buffer
	####

	# Make sure first six blocks are all read in and marked dirty
	dirty_blocks($psql, 'wc_bgwriter', '0,1,2,3,4,5');
	flush_and_reset_io_stats($node, $psql);

	# Do this in a transaction so that we can hold the buffer pin across
	# multiple SQL statements by transferring ownership to the top transaction
	# resource owner.
	$psql->query_safe("BEGIN");
	# Pin a block in the middle of the blocks
	my $pinned_buf = $psql->query_safe(
		"SELECT pin_rel_block('wc_bgwriter', 3)");

	run_bgwriter_cleaner($psql);
	# All the blocks should be in clean buffers except block 3 which was pinned
	# and should still be marked dirty.
	assert_any_blocks_dirty($node, 'wc_bgwriter', '0,1,2,4,5', 'f',
		'bgwriter cleaner wrote buffers around pinned gap');
	assert_blocks_dirty($node, 'wc_bgwriter', '3', 't',
		'bgwriter cleaner skipped pinned buffer');
	$psql->query_safe("SELECT release_buffer($pinned_buf)");
	$psql->query_safe("COMMIT");
	$psql->query_safe('SELECT pg_stat_force_next_flush()');
	# Should have written out blocks 0,1,2 in one write and blocks 4 and 5 in
	# another, totaling two writes.
	assert_writes($node, 'pinned gap bgwriter cleaner', 'client backend',
		'normal', 2, 5 * $block_size);

	$psql->quit();
}
