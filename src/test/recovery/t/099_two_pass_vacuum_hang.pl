use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use IPC::Run qw(pump);
use Time::HiRes qw(usleep);

# Set up nodes
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 'physical');

my $tablespace1 = "test_vacuum_hang_tblspc";

$node_primary->append_conf(
	'postgresql.conf', qq[
hot_standby_feedback = on
log_recovery_conflict_waits = true
log_statement='all'
log_connections=true
log_lock_waits = true
autovacuum = off
maintenance_work_mem = 1024
]);
$node_primary->start;

my $backup_name = 'my_backup';

$node_primary->backup($backup_name);
my $node_replica = PostgreSQL::Test::Cluster->new('standby');
$node_replica->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);

$node_replica->start;

my $test_db = "test_db";
$node_primary->safe_psql('postgres', "CREATE DATABASE $test_db");

my $orig_conninfo = $node_primary->connstr();

# test schema / data
my $table1 = "test_vacuum_hang_table";
my $index1 = "test_vacuum_hang_index";
my $col1 = "col1";

my $psql_timeout = IPC::Run::timer(10);

# Long-running Primary Session A
my %psql_primaryA = ('stdin' => '', 'stdout' => '');
$psql_primaryA{run} =
  $node_primary->background_psql($test_db, \$psql_primaryA{stdin},
	\$psql_primaryA{stdout},
	$psql_timeout);
$psql_primaryA{stdout} = '';
$psql_primaryA{stdin} = "set application_name=A;\n";

# Long-running Primary Session B
my %psql_primaryB = ('stdin' => '', 'stdout' => '');
$psql_primaryB{run} =
  $node_primary->background_psql($test_db, \$psql_primaryB{stdin},
	\$psql_primaryB{stdout},
	$psql_timeout);
$psql_primaryB{stdout} = '';
$psql_primaryB{stdin} = "set application_name=B;\n";

# Long-running Replica Session A
my %psql_replicaA = ('stdin' => '', 'stdout' => '');
$psql_replicaA{run} =
  $node_replica->background_psql($test_db, \$psql_replicaA{stdin},
	\$psql_replicaA{stdout},
	$psql_timeout);
$psql_replicaA{stdout} = '';

# Insert one tuple with value 1 which we can use to make sure the cursor has
# successfully pinned and locked the buffer.
#
# Then insert and update enough rows that we force at least one round of index
# vacuuming before getting to a dead tuple which was killed after the standby
# is disconnected.
#
# Multiple index vacuuming passes is required to repro because after the
# standby reconnects to the primary, our backend's GlobalVisStates will not
# have been updated with the new horizon until an update is forced.
#
# _bt_pendingfsm_finalize() calls GetOldestNonRemovableTransactionId() at the
# end of a round of index vacuuming, updating the backend's GlobalVisState
# and, in our case, moving maybe_needed backwards.
#
# Then vacuum's first pass will continue and pruning will find our later
# inserted and updated tuple HEAPTUPLE_RECENTLY_DEAD when compared to
# maybe_needed but HEAPTUPLE_DEAD when compared to OldestXmin.
$node_primary->safe_psql($test_db, qq[
	CREATE TABLE ${table1}(${col1} int) with (autovacuum_enabled=false);
	INSERT INTO $table1 VALUES (1);
	INSERT INTO $table1 SELECT generate_series(2, 30000);
	CREATE INDEX ${index1} on ${table1}(${col1});
	UPDATE $table1 SET $col1 = 0 WHERE $col1 > 1;
	UPDATE $table1 SET $col1 = 3 WHERE $col1 = 0;
	UPDATE $table1 SET $col1 = 0 WHERE $col1 = 3;
	UPDATE $table1 SET $col1 = 3 WHERE $col1 = 0;
	UPDATE $table1 SET $col1 = 0 WHERE $col1 = 3;
	UPDATE $table1 SET $col1 = 3 WHERE $col1 = 0;
]);

my $primary_lsn = $node_primary->lsn('flush');
$node_primary->wait_for_catchup($node_replica, 'replay', $primary_lsn);

my $walreceiver_pid = $node_replica->safe_psql($test_db, qq[
	select pid from pg_stat_activity where backend_type = 'walreceiver';]);

# Set primary_conninfo to something invalid on the replica and reload the
# config. This will prevent the standby from reconnecting once the connection
# is terminated. Then terminate the wal receiver. When a new WAL receiver
# process starts up, it has to use the primary_conninfo to connect to the
# primary and it will be unable to do so.
$node_replica->safe_psql($test_db, qq[
		ALTER SYSTEM SET primary_conninfo = '';
		SELECT pg_reload_conf();
		SELECT pg_terminate_backend($walreceiver_pid)]);

# Ensure the WAL receiver is no longer active on replica.
$node_replica->poll_query_until($test_db, qq[
	select exists (select * from pg_stat_activity where pid = $walreceiver_pid);] , 'f');

# DECLARE and use a cursor on standby, causing the block of the relation to be
# pinned and locked in a buffer on the replica. It is important that this is
# after termination of the WAL receiver so that the primary does not know about
# the cursor and it can't hold back the horizon on the primary.
my $replica_cursor1 = "test_vacuum_hang_cursor_replica1";
$psql_replicaA{stdin} .= qq[
        BEGIN;
			DECLARE $replica_cursor1 CURSOR FOR SELECT * FROM $table1;
			FETCH FORWARD FROM $replica_cursor1;
        ];

# FETCH FORWARD should have returned a 1. That's how we know the cursor has a
# pin and lock.
ok(pump_until($psql_replicaA{run}, $psql_timeout,
	\$psql_replicaA{stdout}, qr/^1$/m), "got first value from replica cursor");

# Now insert and update a tuple which will be visible to the vacuum on the
# primary but which will have xmax newer than the standby that was recently
# disconnected.
$psql_primaryA{stdin} .= qq[
		INSERT INTO $table1 VALUES (99);
		UPDATE $table1 SET $col1 = 100 WHERE $col1 = 99;
		SELECT 'after_update';
        ];

# Make sure the UPDATE finished
ok(pump_until($psql_primaryA{run}, $psql_timeout,
	\$psql_primaryA{stdout}, qr/^after_update$/m), "SELECT output from primary session A");

# Open a cursor on the primary whose lock will keep VACUUM from getting a
# cleanup lock on the first page of the relation. We want VACUUM to be able to
# start, calculate initial values for OldestXmin and GlobalVisState and then be
# unable to proceed with pruning our dead tuples. This will allow us to
# reconnect the standby and push the horizon back before we start actual
# pruning and vacuuming.
my $primary_cursor1 = "test_vacuum_hang_cursor_primary1";
$psql_primaryB{stdin} .= qq[
        BEGIN;
			DECLARE $primary_cursor1 CURSOR FOR SELECT * FROM $table1;
			FETCH FORWARD FROM $primary_cursor1;
        ];

# FETCH FORWARD should return a 1. That's how we know the cursor has a pin and
# lock.
ok(pump_until($psql_primaryB{run}, $psql_timeout,
	\$psql_primaryB{stdout}, qr/^1$/m), "got first value from primary cursor");

# Now start a VACUUM FREEZE on the primary. It will be unable to get a cleanup
# lock and start pruning, so it will hang. It will call
# vacuum_get_cutoffs() and establish values of OldestXmin and
# GlobalVisState which are newer than all of our dead tuples.
$psql_primaryA{stdin} .= qq[
		VACUUM FREEZE $table1;
		\\echo VACUUM
        ];

# Try and make sure our vacuum command has reached the server before we commit
# the cursor.
$psql_primaryA{run}->pump_nb();

# Make sure that the VACUUM has already called vacuum_get_cutoffs() and is just
# waiting on the cleanup lock to start vacuuming. It needs to reach the state
# of scanning heap. On 16+ we can easily ensure this by checking for
# pg_stat_progress_vacuum phase 'scanning heap' with poll_query_until(). Here
# just use a sleep. Maybe I could do something better with pump_until() but I
# gave up.
# We don't want the standby to re-establish a connection to the primary and
# push the horizon back until we've saved initial values in GlobalVisState and
# calculated OldestXmin.
usleep(300_000);

# Ensure the WAL receiver is still not active on the replica.
$node_replica->poll_query_until($test_db, qq[
	select exists (select * from pg_stat_wal_receiver);] , 'f');

# Allow the WAL receiver connection to re-establish. VACUUM is still
# waiting for the ALTER INDEX to commit.
$node_replica->safe_psql(
	$test_db, qq[
		ALTER SYSTEM SET primary_conninfo = '$orig_conninfo';
		SELECT pg_reload_conf();
	]);

$node_replica->poll_query_until($test_db, qq[
	select exists (select * from pg_stat_wal_receiver);] , 't');

# Once the WAL sender is shown on the primary, the replica should have
# connected with the primary and pushed the horizon backward. Primary Session A
# won't see that until the VACUUM FREEZE proceeds and does its first round of
# index vacuuming.
$node_primary->poll_query_until($test_db, qq[
	select exists (select * from pg_stat_replication);] , 't');

# Commit the cursor so that the VACUUM can proceed.
$psql_primaryB{stdin} .= qq[
			COMMIT;
			\\echo commit
        ];

ok(pump_until($psql_primaryB{run}, $psql_timeout,
	\$psql_primaryB{stdout}, qr/^commit$/m), "CURSOR committing");

$psql_primaryA{run}->pump_nb();

# VACUUM proceeds with pruning and does a visibility check on each tuple. It
# will find our final dead tuple non-removable (HEAPTUPLE_RECENTLY_DEAD) since
# its xmax is after the new value of maybe_needed. Without the fix, after
# pruning, in lazy_scan_prune(), vacuum does another visibility check, this
# time with HeapTupleSatisfiesVacuum() which compares dead_after to OldestXmin.
# It will find the tuple HEAPTUPLE_DEAD since its xmax precedes OldestXmin.
# This will cause the infinite loop.
pump $psql_primaryA{run} until ($psql_primaryA{stdout} =~ /VACUUM/ || $psql_timeout->is_expired);

ok(!$psql_timeout->is_expired);

# Commit the original cursor transaction on the replica so it can catch up. it
# will end up replaying the VACUUM and not removing the tuple too.
$psql_replicaA{stdin} .= qq[ COMMIT; ];

$psql_replicaA{run}->pump_nb();

$primary_lsn = $node_primary->lsn('flush');
# Make sure something causes us to flush
$node_primary->safe_psql($test_db, "insert into $table1 values (1);");
$node_primary->wait_for_catchup($node_replica, 'replay', $primary_lsn);

# Shut down psqls
$psql_primaryA{stdin} .= "\\q\n";
$psql_primaryA{run}->finish;

$psql_primaryB{stdin} .= "\\q\n";
$psql_primaryB{run}->finish;


$psql_replicaA{stdin} .= "\\q\n";
$psql_replicaA{run}->finish;

$node_replica->stop();
$node_primary->stop();

done_testing();
