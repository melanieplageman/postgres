# Copyright (c) 2024-2026, PostgreSQL Global Development Group

# Test that redo repairs and reports a visibility map that has diverged on a
# standby: a VM all-visible bit set over a heap page that is not marked
# all-visible.  Such divergence cannot arise on the primary, but it can on a
# standby (historically via CREATE DATABASE STRATEGY WAL_LOG).  Here we create
# it artificially by clearing PD_ALL_VISIBLE on the standby's copy of the heap
# page while leaving the VM bit set, then have the primary clear the VM bit, so
# redo must reconcile the standby and log the corruption.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# PD_ALL_VISIBLE is a bit in pd_flags, a two-byte field at offset 10 of the page
# header (after pd_lsn and pd_checksum).
use constant PD_FLAGS_OFFSET => 10;
use constant PD_ALL_VISIBLE => 0x0004;

my $primary = PostgreSQL::Test::Cluster->new('primary');
# We edit a heap page directly, so disable data checksums (on by default)
# rather than recomputing them.
$primary->init(allows_streaming => 1, no_data_checksums => 1);
# Keep the visibility map under our control, and disable full_page_writes so the
# delete below is a delta record (heap block BLK_NEEDS_REDO on the standby)
# rather than a full-page image that would overwrite the page and bypass the
# check.
$primary->append_conf('postgresql.conf',
	"autovacuum = off\nfull_page_writes = off");
$primary->start;

my $backup = 'my_backup';
$primary->backup($backup);
my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, $backup, has_streaming => 1);
$standby->start;

# Single-page table, marked all-visible and all-frozen on both nodes.
$primary->safe_psql(
	'postgres', qq(
	CREATE TABLE vmdiv (id int) WITH (autovacuum_enabled = off);
	INSERT INTO vmdiv SELECT g FROM generate_series(1, 50) g;
	VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) vmdiv;
));
my $relpath = $primary->safe_psql('postgres',
	q(SELECT pg_relation_filepath('vmdiv')));

# Checkpoint after the VACUUM and let the standby replay it, so the standby's
# shutdown restartpoint (which uses the last replayed checkpoint) sits after the
# VACUUM.  Otherwise the VACUUM would be replayed on restart and re-set
# PD_ALL_VISIBLE, undoing the edit below.
$primary->safe_psql('postgres', 'CHECKPOINT');
$primary->wait_for_catchup($standby);

# A clean shutdown restartpoints at the last replayed checkpoint, so on restart
# the VACUUM is not replayed and the edit below survives.
$standby->stop;

# Clear PD_ALL_VISIBLE on the standby's heap block 0, leaving the VM all-visible
# bit set: the dangerous VM-set / PD_ALL_VISIBLE-clear divergence.
my $heapfile = $standby->data_dir . '/' . $relpath;
open(my $fh, '+<', $heapfile) or die "could not open $heapfile: $!";
binmode $fh;
sysseek($fh, PD_FLAGS_OFFSET, 0) or die "seek failed: $!";
my $raw;
(sysread($fh, $raw, 2) // -1) == 2 or die "read failed: $!";
sysseek($fh, PD_FLAGS_OFFSET, 0) or die "seek failed: $!";
(syswrite($fh, pack('S', unpack('S', $raw) & ~PD_ALL_VISIBLE)) // -1) == 2
  or die "write failed: $!";
close $fh or die "close failed: $!";

$standby->start;
$primary->wait_for_catchup($standby);

my $logstart = -s $standby->logfile;

# On the primary the page is still all-visible, so this delete clears the VM
# bit.  Redoing it on the standby finds the bit set over a page that is not
# marked all-visible, so it must clear the divergent bit and report it.
$primary->safe_psql('postgres', 'DELETE FROM vmdiv WHERE id = 1');
$primary->wait_for_catchup($standby);

ok( $standby->log_contains(
		qr/is not marked all-visible but its visibility map bit is set/,
		$logstart),
	'standby reports a divergent visibility map bit during redo');

# Recovery did not halt: a further change replays cleanly.
$primary->safe_psql('postgres', 'INSERT INTO vmdiv VALUES (0)');
$primary->wait_for_catchup($standby);
is( $standby->safe_psql('postgres', 'SELECT count(*) FROM vmdiv'),
	'50', 'standby stays consistent after repairing the divergence');

$standby->stop;
$primary->stop;
done_testing();
