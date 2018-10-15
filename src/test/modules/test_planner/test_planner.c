/*--------------------------------------------------------------------------
 *
 * test_planner.c
 *		Test correctness of optimizer's predicate proof logic.
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_planner/test_planner.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"

PG_MODULE_MAGIC;

/*
 * test_planner(query text) returns record
 */
PG_FUNCTION_INFO_V1(test_planner);

Datum
test_planner(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[1];
	bool		nulls[1];

	tupdesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1,
					   "foo", BOOLOID, -1, 0);
	MemSet(nulls, 0, sizeof(nulls));
	tupdesc = BlessTupleDesc(tupdesc);
	values[0] = BoolGetDatum(true);

	List *parsetree_list;
	List *querytree_list;

	const char *query_string = "select * from pg_class;";
	parsetree_list = pg_parse_query(query_string);
	ListCell * parsetree_item = list_head(parsetree_list);
	RawStmt       *parsetree = (RawStmt *) lfirst(parsetree_item);

	querytree_list = pg_analyze_and_rewrite(parsetree, query_string, NULL, 0, NULL);

	ListCell * querytree = list_head(querytree_list);
	Query *query = (Query *) lfirst(querytree);
	PlannedStmt *plannedStmt = planner(query, 0, NULL);


	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));

}
