# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Test that connections to a hot standby are correctly canceled when a
# recovery conflict is detected Also, test that statistics in
# pg_stat_database_conflicts are populated correctly

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use IPC::Run qw(pump);

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

$node_primary->safe_psql($test_db, qq[
	CREATE TABLE ${table1}(${col1} int) with (autovacuum_enabled=false);
	CREATE INDEX ${index1} on ${table1}(${col1});
	INSERT INTO $table1 VALUES(1);
	UPDATE $table1 SET $col1 = 100 WHERE $col1 = 1;
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

$node_replica->poll_query_until($test_db, qq[
	select exists (select * from pg_stat_activity where pid = $walreceiver_pid);] , 'f');

# DECLARE and use a cursor on standby, causing buffer with the only block of
# the relation to be pinned on the standby
my $replica_cursor1 = "test_vacuum_hang_cursor_replica1";
$psql_replicaA{stdin} .= qq[
        BEGIN;
			DECLARE $replica_cursor1 CURSOR FOR SELECT * FROM $table1;
			FETCH FORWARD FROM $replica_cursor1;
        ];

# FETCH FORWARD should have returned a 100
ok(pump_until($psql_replicaA{run}, $psql_timeout,
	\$psql_replicaA{stdout}, qr/^100$/m), "got first value from replica cursor");

$psql_primaryA{stdin} .= qq[
		INSERT INTO $table1 VALUES (99);
		UPDATE $table1 SET $col1 = 100 WHERE $col1 = 99;
		SELECT 'after_update';
        ];

ok(pump_until($psql_primaryA{run}, $psql_timeout,
	\$psql_primaryA{stdout}, qr/^after_update$/m), "SELECT output from primary session A");

my $primary_cursor1 = "test_vacuum_hang_cursor_primary1";
$psql_primaryB{stdin} .= qq[
        BEGIN;
			DECLARE $primary_cursor1 CURSOR FOR SELECT * FROM $table1;
			FETCH FORWARD FROM $primary_cursor1;
        ];

ok(pump_until($psql_primaryB{run}, $psql_timeout,
	\$psql_primaryB{stdout}, qr/^100$/m), "got first value from primary cursor");

$psql_primaryC{stdin} .= qq[
        BEGIN;
			ALTER INDEX $index1 SET TABLESPACE $tablespace1;
			SELECT 'after_alter_index';
        ];

$psql_primaryC{run}->pump_nb();

$node_primary->poll_query_until($test_db, qq[
	select granted from pg_locks
	where mode = 'AccessExclusiveLock' and relation = '$index1'::regclass;], 'f');

$psql_primaryA{stdin} .= qq[
		VACUUM (FREEZE) $table1;
		\\echo VACUUM
        ];

$psql_primaryA{run}->pump_nb();

# After this, the ALTER INDEX should move forward
$psql_primaryB{stdin} .= qq[
			COMMIT;
			\\echo commit
        ];

ok(pump_until($psql_primaryB{run}, $psql_timeout,
	\$psql_primaryB{stdout}, qr/^commit$/m), "CURSOR committing");

ok(pump_until($psql_primaryC{run}, $psql_timeout,
	\$psql_primaryC{stdout}, qr/^after_alter_index$/m), "ALTER INDEX completed");

# Allow the connection to re-establish
$node_replica->safe_psql(
	$test_db, qq[
		ALTER SYSTEM SET primary_conninfo = '$orig_conninfo';
		SELECT pg_reload_conf();
	]);

$node_replica->poll_query_until($test_db, qq[
	select exists (select * from pg_stat_wal_receiver);] , 't');

$node_primary->poll_query_until($test_db, qq[
	select exists (select * from pg_stat_replication);] , 't');

# Commit the alter index
$psql_primaryC{stdin} .= qq[
		COMMIT;
		\\echo postcommit
		];

# commit the alter index that held back vacuum
ok(pump_until($psql_primaryC{run}, $psql_timeout,
	\$psql_primaryC{stdout}, qr/^postcommit$/m), "FINISH ALTER INDEX");

$psql_primaryC{run}->pump_nb();

$psql_primaryA{run}->pump_nb();

pump $psql_primaryA{run} until ($psql_primaryA{stdout} =~ /VACUUM/ || $psql_timeout->is_expired);

ok(!$psql_timeout->is_expired);

# commit the original transaction on the replica so it can catch up
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
