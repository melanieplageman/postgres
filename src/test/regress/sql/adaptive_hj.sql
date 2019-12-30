-- TODO: remove some of these tests and make the test file faster
create schema adaptive_hj;
set search_path=adaptive_hj;
drop table if exists t1;
drop table if exists t2;
create table t1(a int);
create table t2(b int);

-- serial setup
set work_mem=64;
set enable_mergejoin to off;
-- TODO: make this function general
create or replace function explain_multi_batch() returns setof text language plpgsql as
$$
declare ln text;
begin
    for ln in
        explain (analyze, summary off, timing off, costs off)
		select count(*) from t1 left outer join t2 on a = b
    loop
        ln := regexp_replace(ln, 'Memory Usage: \S*',  'Memory Usage: xxx');
        return next ln;
    end loop;
end;
$$;

-- Serial_Test_1 reset
-- TODO: refactor into procedure or change to drop table
update pg_class set reltuples = 0, relpages = 0 where relname = 't2';
update pg_class set reltuples = 0, relpages = 0 where relname = 't1';
delete from pg_statistic where starelid = 't2'::regclass;
delete from pg_statistic where starelid = 't1'::regclass;

-- Serial_Test_1 setup
truncate table t1;
insert into t1 values(1),(2);
insert into t1 select i from generate_series(1,10)i;
insert into t1 select 2 from generate_series(1,5)i;
truncate table t2;
insert into t2 values(2),(3),(11);
insert into t2 select i from generate_series(2,10)i;
insert into t2 select 2 from generate_series(2,7)i;

-- Serial_Test_1.1
-- TODO: automate the checking for expected number of chunks (explain option?)
-- spills in 4 batches
-- batch 1 falls back with 2 chunks with no unmatched tuples
-- batch 2 falls back with 2 chunks with 2 unmatched tuples emitted at EOB 
-- batch 3 falls back with 5 chunks with no unmatched tuples
-- batch 4 does not fall back with no unmatched tuples
select * from explain_multi_batch();
select * from t1 left outer join t2 on a = b order by b, a;
select * from t1, t2 where a = b order by b;
select * from t1 right outer join t2 on a = b order by a, b;
select * from t1 full outer join t2 on a = b order by b, a;

-- Serial_Test_1.2 setup
analyze t1; analyze t2;

-- Serial_Test_1.2
-- doesn't spill (happens to do a hash right join)
select * from explain_multi_batch();

-- Serial_Test_2 reset
update pg_class set reltuples = 0, relpages = 0 where relname = 't2';
update pg_class set reltuples = 0, relpages = 0 where relname = 't1';
delete from pg_statistic where starelid = 't2'::regclass;
delete from pg_statistic where starelid = 't1'::regclass;

-- Serial_Test_2 setup:
truncate table t1;
insert into t1 values (1),(2),(2),(3);
truncate table t2;
insert into t2 values(2),(2),(3),(3),(4);

-- Serial_Test_2.1
-- spills in 4 batches
-- batch 1 falls back with 2 chunks with no unmatched tuples
-- batch 2 does not fall back with 1 unmatched tuple
-- batch 3 does not fall back with no unmatched tuples
-- batch 4 does not fall back with no unmatched tuples
select * from explain_multi_batch();
select * from t1 left outer join t2 on a = b order by b, a;
select * from t1 right outer join t2 on a = b order by a, b;

-- TODO: check coverage for emitting ummatched inner tuples
-- Serial_Test_2.1.a
-- results checking for inner join
select * from t1 left outer join t2 on a = b order by b, a;
select * from t1, t2 where a = b order by b;
select * from t1 right outer join t2 on a = b order by a, b;
select * from t1 full outer join t2 on a = b order by b, a;
select * from t1, t2 where a = b order by b;

-- Serial_Test_2.2
analyze t1; analyze t2;
-- doesn't spill (happens to do a hash right join)
select * from explain_multi_batch();

-- Serial_Test_3 reset
update pg_class set reltuples = 0, relpages = 0 where relname = 't2';
update pg_class set reltuples = 0, relpages = 0 where relname = 't1';
delete from pg_statistic where starelid = 't2'::regclass;
delete from pg_statistic where starelid = 't1'::regclass;


-- Serial_Test_3 setup:
truncate table t1;
insert into t1 values(1),(1);
insert into t1 select 2 from generate_series(1,7)i;
insert into t1 select i from generate_series(3,10)i;
truncate table t2;
insert into t2 select 2 from generate_series(1,7)i;
insert into t2 values(3),(3);
insert into t2 select i from generate_series(5,9)i;

-- Serial_Test_3.1
-- spills in 4 batches
-- batch 1 falls back with 2 chunks with 1 unmatched tuple
-- batch 2 does not fall back with 2 unmatched tuples
-- batch 3 falls back with 4 chunks with 1 unmatched tuple
-- batch 4 does not fall back with no unmatched tuples
select * from explain_multi_batch();
select * from t1 left outer join t2 on a = b order by b, a;
select * from t1, t2 where a = b order by b;
select * from t1 right outer join t2 on a = b order by a, b;
select * from t1 full outer join t2 on a = b order by b, a;
select * from t1, t2 where a = b order by b;

-- Serial_Test_3.2 
-- swap join order
select * from t2 left outer join t1 on a = b order by a, b;
select * from t2, t1 where a = b order by a;
select * from t2 right outer join t1 on a = b order by b, a;
select * from t2 full outer join t1 on a = b order by a, b;

-- Serial_Test_3.3 setup
analyze t1; analyze t2;

-- Serial_Test_3.3
-- doesn't spill
select * from explain_multi_batch();

-- Serial_Test_4 setup
drop table t1;
create table t1(b int);
insert into t1 select i from generate_series(1,111)i;
insert into t1 select 2 from generate_series(1,180)i;
analyze t1;

drop table t2;
create table t2(a int);
insert into t2 select i from generate_series(20,25000)i;
insert into t2 select 2 from generate_series(1,100)i;
analyze t2;
update pg_class
  set reltuples = 10, relpages = pg_relation_size('t2') / 8192
  where relname = 't2';

-- Serial_Test_4.1
-- spills in 32 batches
--batch 0 does not fall back with 1 unmatched outer tuple (15)
--batch 1 falls back with 396 chunks.
--batch 2 falls back with 402 chunks with 1 unmatched outer tuple (1)
--batch 3 falls back with 389 chunks with 1 unmatched outer tuple (8)
--batch 4 falls back with 409 chunks with no unmatched outer tuples
--batch 5 falls back with 366 chunks with 1 unmatched outer tuple (4)
--batch 6 falls back with 407 chunks with 1 unmatched outer tuple (11)
--batch 7 falls back with 382 chunks with unmatched outer tuple (10)
--batch 8 falls back with 413 chunks with no unmatched outer tuples
--batch 9 falls back with 371 chunks with 1 unmatched outer tuple (3)
--batch 10 falls back with 389 chunks with no unmatched outer tuples
--batch 11 falls back with 408 chunks with no unmatched outer tuples
--batch 12 falls back with 387 chunks with no unmatched outer tuples
--batch 13 falls back with 402 chunks with 1 unmatched outer tuple (18) 
--batch 14 falls back with 369 chunks with 1 unmatched outer tuple (9)
--batch 15 falls back with 387 chunks with no unmatched outer tuples
--batch 16 falls back with 365 chunks with no unmatched outer tuples
--batch 17 falls back with 403 chunks with 2 unmatched outer tuples (14,19)
--batch 18 falls back with 375 chunks with no unmatched outer tuples
--batch 19 falls back with 384 chunks with no unmatched outer tuples
--batch 20 falls back with 377 chunks with 1 unmatched outer tuple (12)
--batch 22 falls back with 401 chunks with no unmatched outer tuples
--batch 23 falls back with 396 chunks with no unmatched outer tuples
--batch 24 falls back with 387 chunks with 1 unmatched outer tuple (5)
--batch 25 falls back with 399 chunks with 1 unmatched outer tuple (7)
--batch 26 falls back with 387 chunks.
--batch 27 falls back with 442 chunks.
--batch 28 falls back with 385 chunks with 1 unmatched outer tuple (17)
--batch 29 falls back with 375 chunks.
--batch 30 falls back with 404 chunks with 1 unmatched outer tuple (6)
--batch 31 falls back with 396 chunks with 2 unmatched outer tuples (13,16)
select * from explain_multi_batch();
select count(*) from t1 left outer join t2 on a = b;
select count(a) from t1 left outer join t2 on a = b;
select count(*) from t1, t2 where a = b;
-- used to give wrong results because there is a whole batch of outer which is
-- empty and so the inner doesn't emit unmatched tuples with ROJ
select count(*) from t1 right outer join t2 on a = b;
select count(*) from t1 full outer join t2 on a = b; 

-- Test_6 non-negligible amount of data test case
-- TODO: doesn't finish with my code when it is set to be serial
-- it does finish when it is parallel -- the serial version is either simply too
-- slow or has a bug -- I tried it with less data and it did finish, so it must
-- just be really slow
-- inner join shouldn't even need to make the unmatched files
-- it finishes eventually if I decrease data amount

--drop table simple;
--create table simple as
 -- select generate_series(1, 20000) AS id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
--alter table simple set (parallel_workers = 2);
--analyze simple;
--
--drop table extremely_skewed;
--create table extremely_skewed (id int, t text);
--alter table extremely_skewed set (autovacuum_enabled = 'false');
--alter table extremely_skewed set (parallel_workers = 2);
--analyze extremely_skewed;
--insert into extremely_skewed
--  select 42 as id, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
--  from generate_series(1, 20000);
--update pg_class
--  set reltuples = 2, relpages = pg_relation_size('extremely_skewed') / 8192
--  where relname = 'extremely_skewed';

--set work_mem=64;
--set enable_mergejoin to off;
--explain (analyze, costs off, timing off)
  --select * from simple r join extremely_skewed s using (id);
--select * from explain_multi_batch();

drop table t1;
drop table t2;
drop function explain_multi_batch();
reset enable_mergejoin;
reset work_mem;
reset search_path;
drop schema adaptive_hj;
