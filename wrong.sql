drop table if exists simple;
create table simple as
  select generate_series(1, 20000) AS id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
alter table simple set (parallel_workers = 2);
analyze simple;

drop table if exists bigger_than_it_looks;
create table bigger_than_it_looks as
  select generate_series(1, 20000) as id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
alter table bigger_than_it_looks set (autovacuum_enabled = 'false');
alter table bigger_than_it_looks set (parallel_workers = 2);
analyze bigger_than_it_looks;
update pg_class set reltuples = 1000 where relname = 'bigger_than_it_looks';

set min_parallel_table_scan_size = 0;
set parallel_setup_cost = 0;
set enable_hashjoin = on;
set max_parallel_workers_per_gather = 1;
--set work_mem = '192kB';
set work_mem = '64kB';
set enable_parallel_hash = on;
--explain (analyze, verbose, costs off)
select count(*) from simple r join bigger_than_it_looks s using (id);
