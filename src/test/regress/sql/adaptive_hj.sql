drop table if exists t1;
drop table if exists t2;
create table t1(a int);
create table t2(b int);

insert into t1 values(1),(2);
insert into t2 values(2),(3);
insert into t1 select i from generate_series(1,10)i;
insert into t2 select i from generate_series(2,10)i;
insert into t1 select 2 from generate_series(1,5)i;
insert into t2 select 2 from generate_series(2,7)i;
set work_mem=64;
set enable_mergejoin to off;

select * from t1 left outer join t2 on a = b order by b;
select count(*) from t1 left outer join t2 on a = b;
select * from t1, t2 where a = b;
select count(*) from t1, t2 where a = b;

truncate table t1;
insert into t1 values (1),(2),(2),(3);
truncate table t2;
insert into t2 values(2),(2),(3),(3),(4);

set work_mem=64;
set enable_mergejoin to off;

select * from t1 left outer join t2 on a = b order by b;
select count(*) from t1 left outer join t2 on a = b;
select * from t1, t2 where a = b;
select count(*) from t1, t2 where a = b;
