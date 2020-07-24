DROP TABLE IF EXISTS probeside_batch0;
CREATE TABLE probeside_batch0(a stub);
ALTER TABLE probeside_batch0 ALTER COLUMN a SET STORAGE PLAIN;
INSERT INTO probeside_batch0 SELECT '(0, "")' FROM generate_series(1, 13);
INSERT INTO probeside_batch0 SELECT '(0, "unmatched outer")' FROM generate_series(1, 1);

DROP TABLE IF EXISTS hashside_wide_batch0;
CREATE TABLE hashside_wide_batch0(a stub, id int);
ALTER TABLE hashside_wide_batch0 ALTER COLUMN a SET STORAGE PLAIN;
INSERT INTO hashside_wide_batch0 SELECT '(0, "")', 10 FROM generate_series(1, 6);
INSERT INTO hashside_wide_batch0 SELECT '(0, "")', 22 FROM generate_series(1, 3);
INSERT INTO hashside_wide_batch0 SELECT '(2, "")', 22 FROM generate_series(1, 2);
INSERT INTO hashside_wide_batch0 SELECT '(32, "")', 22 FROM generate_series(1, 10);
INSERT INTO hashside_wide_batch0 SELECT '(0, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")', 42 FROM generate_series(1, 1);
ANALYZE probeside_batch0, hashside_wide_batch0;

set min_parallel_table_scan_size = 0;
set parallel_setup_cost = 0;
set enable_hashjoin = on;

set max_parallel_workers_per_gather = 1;
set enable_parallel_hash = on;
set work_mem = '64kB';

/* set log_min_messages = error; */
/* set log_min_error_statement = error; */
/* set client_min_messages = error; */
--explain (costs off, analyze)
SELECT (probeside_batch0.a).hash, ((((probeside_batch0.a).hash << 7) >> 3) & 31) AS batchno, TRIM((probeside_batch0.a).value), hashside_wide_batch0.id, hashside_wide_batch0.ctid as innerctid, (hashside_wide_batch0.a).hash, TRIM((hashside_wide_batch0.a).value), probeside_batch0.ctid as outerctid
FROM probeside_batch0
LEFT OUTER JOIN hashside_wide_batch0 USING (a)
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8;
