# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Tests recovery scenarios where the files are shorter than in the common
# cases, e.g. due to replaying WAL records of a relation that was subsequently
# truncated or dropped.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('n1');

$node->init();

# Disable autovacuum to guarantee VACUUM can remove rows / truncate relations
$node->append_conf(
	'postgresql.conf', qq[
wal_level = 'replica'
autovacuum = off
]);

$node->start();


# Test: Replay replay of PRUNE records for a pre-existing, then dropped,
# relation

$node->safe_psql(
	'postgres', qq[
CREATE TABLE truncme(i int) WITH (fillfactor = 50);
INSERT INTO truncme SELECT generate_series(1, 1000);
UPDATE truncme SET i = 1;
CHECKPOINT; -- ensure relation exists at start of recovery
VACUUM truncme; -- generate prune records
DROP TABLE truncme;
]);

$node->stop('immediate');

ok($node->start(),
	'replay of PRUNE records for a pre-existing, then dropped, relation');


# Test: Replay of PRUNE records for a newly created, then dropped, relation

$node->safe_psql(
	'postgres', qq[
CREATE TABLE truncme(i int) WITH (fillfactor = 50);
INSERT INTO truncme SELECT generate_series(1, 1000);
UPDATE truncme SET i = 1;
VACUUM truncme; -- generate prune records
DROP TABLE truncme;
]);

$node->stop('immediate');

ok($node->start(),
	'replay of PRUNE records for a newly created, then dropped, relation');


# Test: Replay of PRUNE records affecting truncated block. With FPIs used for
# PRUNE.

$node->safe_psql(
	'postgres', qq[
CREATE TABLE truncme(i int) WITH (fillfactor = 50);
INSERT INTO truncme SELECT generate_series(1, 1000);
UPDATE truncme SET i = 1;
CHECKPOINT; -- generate FPIs
VACUUM truncme; -- generate prune records
TRUNCATE truncme; -- make blocks non-existing
INSERT INTO truncme SELECT generate_series(1, 10);
]);

$node->stop('immediate');

ok($node->start(),
	'replay of PRUNE records affecting truncated block (FPIs)');

is($node->safe_psql('postgres', 'select count(*), sum(i) FROM truncme'),
	'10|55', 'table contents as expected after recovery');
$node->safe_psql('postgres', 'DROP TABLE truncme');


# Test replay of PRUNE records for blocks that are later truncated. Without
# FPIs used for PRUNE.

$node->safe_psql(
	'postgres', qq[
CREATE TABLE truncme(i int) WITH (fillfactor = 50);
INSERT INTO truncme SELECT generate_series(1, 1000);
UPDATE truncme SET i = 1;
VACUUM truncme; -- generate prune records
TRUNCATE truncme; -- make blocks non-existing
INSERT INTO truncme SELECT generate_series(1, 10);
]);

$node->stop('immediate');

ok($node->start(),
	'replay of PRUNE records affecting truncated block (no FPIs)');

is($node->safe_psql('postgres', 'select count(*), sum(i) FROM truncme'),
	'10|55', 'table contents as expected after recovery');
$node->safe_psql('postgres', 'DROP TABLE truncme');


# Test: Replay of partial truncation via VACUUM

$node->safe_psql(
	'postgres', qq[
CREATE TABLE truncme(i int) WITH (fillfactor = 50);
INSERT INTO truncme SELECT generate_series(1, 1000);
UPDATE truncme SET i = i + 1;
-- ensure a mix of pre/post truncation rows
DELETE FROM truncme WHERE i > 500;

VACUUM truncme; -- should truncate relation

-- rows at TIDs that previously existed
INSERT INTO truncme SELECT generate_series(1000, 1010);
]);

$node->stop('immediate');

ok($node->start(), 'replay of partial truncation via VACUUM');

is( $node->safe_psql(
		'postgres', 'select count(*), sum(i), min(i), max(i) FROM truncme'),
	'510|136304|2|1010',
	'table contents as expected after recovery');
$node->safe_psql('postgres', 'DROP TABLE truncme');


# Test WAL consistency checking when a truncated heap block is reused. The
# DELETE must have a consistency image but no image to apply during redo.
# Keep the background writer from flushing the re-inserted tuple before the
# crash, so that redo of the DELETE finds the zero page left by extension.
$node->append_conf(
	'postgresql.conf', qq[
full_page_writes = off
bgwriter_lru_maxpages = 0
]);
$node->reload();

is( $node->safe_psql(
		'postgres', q[
CREATE TABLE truncme(i int);
INSERT INTO truncme VALUES (1);
CHECKPOINT;
SET wal_consistency_checking = 'heap';
DELETE FROM truncme;
VACUUM (TRUNCATE TRUE) truncme;
SELECT pg_relation_size('truncme');
]),
	'0', 'VACUUM truncated the heap to zero blocks');

$node->safe_psql('postgres', 'INSERT INTO truncme VALUES (2)');
$node->stop('immediate');
$node->start();

is($node->safe_psql('postgres', 'TABLE truncme'),
	'2', 'WAL consistency checking skips the uninitialized heap block');

done_testing();
