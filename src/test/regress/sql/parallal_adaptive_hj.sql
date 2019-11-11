create schema adaptive_hj;
set search_path=adaptive_hj;

-- TODO: anti-semi-join and semi-join tests

-- TODO: check if test2 and 3 are different at all

-- TODO: add test for parallel-oblivious parallel hash join

-- TODO: make this function general
create or replace function explain_parallel_multi_batch() returns setof text language plpgsql as
$$
declare ln text;
begin
    for ln in
        explain (analyze, summary off, timing off, costs off)
		select count(*) from t1 left outer join t2 on a = b
    loop
        ln := regexp_replace(ln, 'Planning Time: \S*',  'Memory: xxx');
        ln := regexp_replace(ln, 'Execution Time: \S*',  'Memory: xxx');
        return next ln;
    end loop;
end;
$$;

-- parallel setup
set enable_nestloop to off;
set enable_mergejoin to off;
set  min_parallel_table_scan_size = 0;
set  parallel_setup_cost = 0;
set  enable_parallel_hash = on;
set  enable_hashjoin = on;
set  max_parallel_workers_per_gather = 1;
set  work_mem = 64;

-- Parallel_Test_1 setup
drop table t1;
create table t1(b int);
alter table t1 set (parallel_workers = 1);
insert into t1 select i from generate_series(1,11)i;
insert into t1 select 2 from generate_series(1,18)i;
analyze t1;

drop table t2;
create table t2(a int);
alter table t2 set (parallel_workers = 1);
insert into t2 select i from generate_series(4,2500)i;
insert into t2 select 2 from generate_series(1,10)i;
analyze t2;
alter table t2 set (autovacuum_enabled = 'false');
update pg_class
  set reltuples = 10, relpages = pg_relation_size('t2') / 8192
  where relname = 't2';

-- Parallel_Test_1.1
-- spills in 4 batches
-- 1 resize of nbatches
-- no batch falls back
select * from explain_parallel_multi_batch();
-- need an aggregate to exercise the code but still want to know if we are
-- emitting the right unmatched outer tuples
select count(a) from t1 left outer join t2 on a = b;
select count(*) from t1 left outer join t2 on a = b;

-- Parallel_Test_1.1.a
-- results checking for inner join
-- doesn't fall back
select count(*) from t1, t2 where a = b;
-- Parallel_Test_1.1.b
-- results checking for right outer join
-- doesn't exercise the fallback code but just checking results
select count(*) from t1 right outer join t2 on a = b;
-- Parallel_Test_1.1.c
-- results checking for full outer join
select count(*) from t1 full outer join t2 on a = b;

-- Parallel_Test_1.2
-- spill and doesn't have to resize nbatches
analyze t2;
select * from explain_parallel_multi_batch();
select count(a) from t1 left outer join t2 on a = b;

-- Parallel_Test_1.3
-- doesn't spill
-- does resize nbuckets
set work_mem = '4MB';
select * from explain_parallel_multi_batch();
select count(a) from t1 left outer join t2 on a = b;
set work_mem = 64;


-- Parallel_Test_3
-- big example
drop table t2;
create table t2(a int);
alter table t2 set (parallel_workers = 1);
insert into t2 select i from generate_series(20,25000)i;
insert into t2 select 2 from generate_series(1,100)i;
analyze t2;
update pg_class
  set reltuples = 10, relpages = pg_relation_size('t2') / 8192
  where relname = 't2';

drop table t1;
create table t1(b int);
alter table t1 set (parallel_workers = 1);
insert into t1 select i from generate_series(1,111)i;
insert into t1 select 2 from generate_series(1,180)i;
analyze t1;

select * from explain_parallel_multi_batch();
select * from t1 left outer join t2 on a = b order by a, b;
select * from explain_parallel_multi_batch();

-- TODO: check what each of these is exercising -- chunk num, etc and write that
-- down
-- also, note that this example did reveal with ROJ that it wasn't working, so
-- maybe keep that but it is not parallel
-- make sure the plans make sense for the code we are writing
select count(*) from t1 left outer join t2 on a = b;

select count(*) from t1, t2 where a = b;
select count(*) from t1 right outer join t2 on a = b;
select count(*) from t1 full outer join t2 on a = b;

-- Parallel_Test_4
-- spill and resize nbatches 2x

drop table t2;
create table t2(a int);
alter table t2 set (parallel_workers = 1);
insert into t2 select i from generate_series(4,1000)i;
insert into t2 select 2 from generate_series(1,4000)i;
analyze t2;
alter table t2 set (autovacuum_enabled = 'false');
update pg_class
set reltuples = 10, relpages = pg_relation_size('t2') / 8192
where relname = 't2';

drop table t1;
create table t1(b int);
alter table t1 set (parallel_workers = 1);
insert into t1 select i from generate_series(1,11)i;
insert into t1 select 2 from generate_series(1,18)i;
insert into t1 values(500);
analyze t1;

select * from explain_parallel_multi_batch();
select * from t1 left outer join t2 on a = b order by a, b;

select count(a) from t1 left outer join t2 on a = b;

select * from t1, t2 where a = b order by b;

select * from t1 right outer join t2 on a = b order by b;

select * from t1 full outer join t2 on a = b order by b;
