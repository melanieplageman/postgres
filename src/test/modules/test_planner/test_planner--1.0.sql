/* src/test/modules/test_planner/test_planner--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_planner" to load this file. \quit

CREATE FUNCTION test_planner(query text,
  OUT result bool
 )
STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
