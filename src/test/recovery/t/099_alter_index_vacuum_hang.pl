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
allow_in_place_tablespaces = on
hot_standby_feedback = on
log_recovery_conflict_waits = true
log_statement='all'
log_connections=true
log_lock_waits = true
autovacuum = off
]);
$node_primary->start;

my $backup_name = 'my_backup';

# Make a tablespace for use with our later ALTER INDEX ... SET TABLESPACE
$node_primary->safe_psql('postgres',
	qq[CREATE TABLESPACE $tablespace1 LOCATION '']);

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

# Long-running Primary Session C
my %psql_primaryC = ('stdin' => '', 'stdout' => '');
$psql_primaryC{run} =
  $node_primary->background_psql($test_db, \$psql_primaryC{stdin},
	\$psql_primaryC{stdout},
	$psql_timeout);
$psql_primaryC{stdout} = '';
$psql_primaryC{stdin} = "set application_name=C;\n";

# Long-running Replica Session A
my %psql_replicaA = ('stdin' => '', 'stdout' => '');
$psql_replicaA{run} =
  $node_replica->background_psql($test_db, \$psql_replicaA{stdin},
	\$psql_replicaA{stdout},
	$psql_timeout);
$psql_replicaA{stdout} = '';

# Insert one tuple with value 1 which we can use to make sure the cursor has
# successfully pinned and locked the buffer.
$node_primary->safe_psql($test_db, qq[
	CREATE TABLE ${table1}(${col1} int) with (autovacuum_enabled=false);
	CREATE INDEX ${index1} on ${table1}(${col1});
	INSERT INTO $table1 VALUES(1);
]);

my $primary_lsn = $node_primary->lsn('flush');
$node_primary->wait_for_catchup($node_replica, 'replay', $primary_lsn);

my $walreceiver_pid = $node_replica->safe_psql($test_db, qq[
	select pid from pg_stat_activity where backend_type = 'walreceiver';]);

# Set primary_conninfo to something invalid on the replica and reload the config.
# This will prevent the standby from reconnecting once the connection is terminated.
# Then terminate the wal receiver. When a new WAL receiver process starts up,
# it will have to use the primary_conninfo to connect to the primary and be
# unable to do so.
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

# Insert a new tuple and update it. Now there are two live tuples and one dead
# tuple on the page on the primary. The connection to the replica was
# terminated, so the replica will only have the single live tuple.
$psql_primaryA{stdin} .= qq[
		INSERT INTO $table1 VALUES (99);
		UPDATE $table1 SET $col1 = 100 WHERE $col1 = 99;
		SELECT 'after_update';
        ];

# Make sure the UPDATE finished
ok(pump_until($psql_primaryA{run}, $psql_timeout,
	\$psql_primaryA{stdout}, qr/^after_update$/m), "SELECT output from primary session A");

# Open a cursor on the primary whose lock will keep a subsequent ALTER INDEX
# from executing. We want VACUUM to be able to start, calculate initial values
# for OldestXmin and GlobalVisState and then pause before pruning our dead
# tuple. To accomplish this, we will run an ALTER INDEX which starts before
# VACUUM but cannot proceed (because of the cursor's lock on the index) until
# after VACUUM gets initial values for OldestXmin and GlobalVisState. When the
# cursor is closed, the ALTER INDEX will proceed and modify the index, causing
# a relcache invalidation and forcing vacuum to rebuild the catalog snapshot
# after the ALTER INDEX commits before pruning the dead tuple.
#
my $primary_cursor1 = "test_vacuum_hang_cursor_primary1";
$psql_primaryB{stdin} .= qq[
        BEGIN;
			DECLARE $primary_cursor1 CURSOR FOR SELECT * FROM $table1;
			FETCH FORWARD FROM $primary_cursor1;
        ];

# There is one live tuple on the primary with value 100 and one live tuple with
# value 1.
ok(pump_until($psql_primaryB{run}, $psql_timeout,
	\$psql_primaryB{stdout}, qr/^1$/m), "got first value from primary cursor");

# the ALTER INDEX won't proceed until the cursor has been closed because it
# needs an exclusive lock and the cursor has an access share lock.
$psql_primaryC{stdin} .= qq[
        BEGIN;
			ALTER INDEX $index1 SET TABLESPACE $tablespace1;
			SELECT 'after_alter_index';
        ];

$psql_primaryC{run}->pump_nb();

# Wait until we know the access exclusive lock has been requested but not granted
$node_primary->poll_query_until($test_db, qq[
	select granted from pg_locks
	where mode = 'AccessExclusiveLock' and relation = '$index1'::regclass;], 'f');

my $vac_count_before = $node_primary->safe_psql($test_db,
	qq[
	SELECT vacuum_count
	FROM pg_stat_all_tables WHERE relname = '${table1}';
	]);

# ALTER INDEX has a row exclusive lock on the index and share update exclusive
# lock on the table, so VACUUM will have to wait while attempting to open the
# indexes.
#
# Note that all transactions on the primary see the dead tuple on the primary
# as committed, so before the new snapshot is taken and GlobalVisState is
# updated, VACUUM's GlobalVisState->maybe_needed will be newer than the dead
# tuple's xmax, so the tuple would have been considered removable when compared
# to GlobalVisState.
$psql_primaryA{stdin} .= qq[
		VACUUM $table1;
		\\echo VACUUM
        ];


# Try and make sure our vacuum command has reached the server before we commit
# the cursor.
$psql_primaryA{run}->pump_nb();

$node_primary->poll_query_until($test_db,
	qq[ SELECT count(*) >= 1
		FROM pg_stat_progress_vacuum
		WHERE phase = 'initializing' AND
		'${table1}'::regclass::oid = relid;
		] ,
	't');

usleep(300_000);

# After committing the cursor, the ALTER INDEX should proceed. We don't commit
# its outer transaction, however, so VACUUM will still be waiting.
$psql_primaryB{stdin} .= qq[
			COMMIT;
			\\echo commit
        ];

ok(pump_until($psql_primaryB{run}, $psql_timeout,
	\$psql_primaryB{stdout}, qr/^commit$/m), "CURSOR committing");

ok(pump_until($psql_primaryC{run}, $psql_timeout,
	\$psql_primaryC{stdout}, qr/^after_alter_index$/m), "ALTER INDEX ran");

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
# won't see that until the GlobalVisUpdate() though.
$node_primary->poll_query_until($test_db, qq[
	select exists (select * from pg_stat_replication);] , 't');

# Commit the transaction surrounding the ALTER INDEX
$psql_primaryC{stdin} .= qq[
		COMMIT;
		\\echo postcommit
		];

# VACUUM will take a new snapshot since the index modification caused a
# relcache invalidation. The new value of RecentXmin should reflect the
# older horizon from the standby and it will differ with
# ComputeXidHorizonsResultLastXmin, causing us to update
# GlobalVisState->maybe_needed to an older value.
ok(pump_until($psql_primaryC{run}, $psql_timeout,
	\$psql_primaryC{stdout}, qr/^postcommit$/m), "FINISH ALTER INDEX");

$psql_primaryC{run}->pump_nb();

$psql_primaryA{run}->pump_nb();

# VACUUM proceeds to pruning and does a visibility check on each tuple. It will
# find our dead tuple non-removable (HEAPTUPLE_RECENTLY_DEAD) since its xmax is
# after the new value of maybe_needed. Once vacuum finishes the first pruning
# pass, it will, in lazy_scan_prune(), do another visibility check, this time
# with HeapTupleSatisfiesVacuum() which compares dead_after to OldestXmin. It
# will find the tuple HEAPTUPLE_DEAD since its xmax precedes OldestXmin. This
# will cause the infinite loop.
pump $psql_primaryA{run} until ($psql_primaryA{stdout} =~ /VACUUM/ || $psql_timeout->is_expired);

ok(!$psql_timeout->is_expired);

$node_primary->poll_query_until($test_db,
	qq[
	SELECT vacuum_count > 0
	FROM pg_stat_all_tables WHERE relname = '${table1}';
	], 't');

my $vac_count_after = $node_primary->safe_psql($test_db,
	qq[
	SELECT vacuum_count
	FROM pg_stat_all_tables WHERE relname = '${table1}';
	]
);

# Ensure that that the VACUUM completed successfully
cmp_ok($vac_count_after, '>', $vac_count_before);


# Commit the original cursor transaction on the replica so it can catch up. it
# will end up replaying the vacuum and not removing the tuple too.
$psql_replicaA{stdin} .= qq[
	COMMIT;
	];

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

$psql_primaryC{stdin} .= "\\q\n";
$psql_primaryC{run}->finish;

$psql_replicaA{stdin} .= "\\q\n";
$psql_replicaA{run}->finish;

$node_replica->stop();
$node_primary->stop();

done_testing();
