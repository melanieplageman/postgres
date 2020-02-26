create table t1(a int);
insert into t1 select i from generate_series(1,111)i;
insert into t1 select 2 from generate_series(1,180)i;
analyze t1;
update pg_class
  set reltuples = 1000000, relpages = pg_relation_size('t1') / 8192
  where relname = 't1';
create table t2(b int);
insert into t2 select i from generate_series(200,15000)i;
insert into t2 select 2 from generate_series(1,1000)i;
insert into t2 select 85 from generate_series(1,10000)i;
analyze t2;
update pg_class
  set reltuples = 10, relpages = pg_relation_size('t2') / 8192
  where relname = 't2';
set enable_nestloop to off;
set enable_mergejoin to off;
set  work_mem = 64;
explain select count(*) from t2 right outer join t1 on b = a;
select count(*) from t2 right outer join t1 on b = a;
--explain select count(*) from t1 left outer join t2 on a = b;
--explain (analyze, costs off, timing off)
--select * from t1 left outer join t2 on a = b order by t2.b, t1.a;
--select * from t1 left outer join t2 on a = b order by t2.b asc, t1.a asc;--select count(a) from t1, t2 where a = b;
--select count(a) from t1 left outer join t2 on a = b;
