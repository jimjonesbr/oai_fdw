
#include "postgres.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "utils/rel.h"
#include "access/table.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "nodes/pg_list.h"

PG_MODULE_MAGIC;

Datum oai_fdw_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(oai_fdw_handler);

typedef struct oai_fdw_state {
	int current;
	int end;
} oai_fdw_state;

typedef struct oai_fdw_TableOptions {
	int start, end;
} oai_fdw_TableOptions;


void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
void oai_fdw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
ForeignScan *oai_fdw_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
void oai_fdw_BeginForeignScan(ForeignScanState *node, int eflags);
TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node);
void oai_fdw_ReScanForeignScan(ForeignScanState *node);
void oai_fdw_EndForeignScan(ForeignScanState *node);

Datum oai_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	fdwroutine->GetForeignRelSize = oai_fdw_GetForeignRelSize;
	fdwroutine->GetForeignPaths = oai_fdw_GetForeignPaths;
	fdwroutine->GetForeignPlan = oai_fdw_GetForeignPlan;
	fdwroutine->BeginForeignScan = oai_fdw_BeginForeignScan;
	fdwroutine->IterateForeignScan = oai_fdw_IterateForeignScan;
	fdwroutine->ReScanForeignScan = oai_fdw_ReScanForeignScan;
	fdwroutine->EndForeignScan = oai_fdw_EndForeignScan;
	PG_RETURN_POINTER(fdwroutine);
}


void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	Relation rel = table_open(foreigntableid, NoLock);
	if (rel->rd_att->natts != 1) {
		ereport(ERROR,
				errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
				errmsg("incorrect schema for oai_fdw table %s: table must have exactly one column", NameStr(rel->rd_rel->relname)));
	}
	Oid typid = rel->rd_att->attrs[0].atttypid;
	if (typid != INT4OID) {
		ereport(ERROR,
				errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				errmsg("incorrect schema for oai_fdw table %s: table column must have type int", NameStr(rel->rd_rel->relname)));
	}
	table_close(rel, NoLock);

	int start = 0, end = 64;

	ForeignTable *ft = GetForeignTable(foreigntableid);
	ListCell *cell;
	foreach(cell, ft->options) {
		DefElem *def = lfirst_node(DefElem, cell);

		if (strcmp("start", def->defname) == 0) {
			char *val = defGetString(def);
			if (sscanf(val, "%d", &start) != 1) {
				ereport(ERROR,
						errcode(ERRCODE_FDW_ERROR),
						errmsg("invalid value for option \"start\": \"%s\" must be a decimal integer", val));
			}
		} else if (strcmp("end", def->defname) == 0) {
			char *val = defGetString(def);
			if (sscanf(val, "%d", &end) != 1) {
				ereport(ERROR,
						errcode(ERRCODE_FDW_ERROR),
						errmsg("invalid value for option \"end\": \"%s\" must be a decimal integer", val));
			}
		} else {
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid option \"%s\"", def->defname),
							errhint("Valid table options for tutorial_fdw are \"start\" and \"end\"")));
		}
	}
	if (start < 0) {
		ereport(ERROR,
				errcode(ERRCODE_FDW_ERROR),
				errmsg("invalid value for option \"start\": must be non-negative"));
	}
	if (end < 0) {
		ereport(ERROR,
				errcode(ERRCODE_FDW_ERROR),
				errmsg("invalid value for option \"end\": must be non-negative"));
	}
	if (end < start) {
		ereport(ERROR,
				errcode(ERRCODE_FDW_ERROR),
				errmsg("invalid values for option \"start\" and \"end\": end must be >= start"));
	}

	baserel->rows = end - start;

	oai_fdw_TableOptions *opts = palloc(sizeof(oai_fdw_TableOptions));
	opts->start = start;
	opts->end = end;
	baserel->fdw_private = opts;
}

void oai_fdw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	Path *path = (Path *)create_foreignscan_path(root, baserel,
			NULL,              /* default pathtarget */
			baserel->rows,     /* rows */
			1,                 /* startup cost */
			1 + baserel->rows, /* total cost */
			NIL,               /* no pathkeys */
			NULL,              /* no required outer relids */
			NULL,              /* no fdw_outerpath */
			NIL);              /* no fdw_private */
	add_path(baserel, path);
}

ForeignScan *oai_fdw_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan) {

	oai_fdw_TableOptions *opts = baserel->fdw_private;
	List *fdw_private = list_make2(makeInteger(opts->start), makeInteger(opts->end));

	scan_clauses = extract_actual_clauses(scan_clauses, false);
	return make_foreignscan(tlist,
			scan_clauses,
			baserel->relid,
			NIL, /* no expressions we will evaluate */
			fdw_private, /* pass along our start and end */
			NIL, /* no custom tlist; our scan tuple looks like tlist */
			NIL, /* no quals we will recheck */
			outer_plan);
}


void oai_fdw_BeginForeignScan(ForeignScanState *node, int eflags) {
	oai_fdw_state *state = palloc0(sizeof(oai_fdw_state));
	ForeignScan *fs = (ForeignScan *)node->ss.ps.plan;
	state->current = intVal(linitial(fs->fdw_private));
	state->end = intVal(lsecond(fs->fdw_private));
	node->fdw_state = state;
}

TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node) {

	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExecClearTuple(slot);

	oai_fdw_state *state = node->fdw_state;
	if (state->current < state->end) {
		slot->tts_isnull[0] = false;
		slot->tts_values[0] = Int32GetDatum(state->current);
		ExecStoreVirtualTuple(slot);
		state->current++;
	}

	return slot;
}

void oai_fdw_ReScanForeignScan(ForeignScanState *node) {
	oai_fdw_state *state = node->fdw_state;
	state->current = 0;
}

void oai_fdw_EndForeignScan(ForeignScanState *node) {
}
