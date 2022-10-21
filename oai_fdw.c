/**********************************************************************
 *
 * oai_fdw - Open Archive Initiative Foreign Data Wrapper for PostgreSQL
 *
 * oai_fdw is free software: you can redistribute it and/or modify
 * it under the terms of the MIT Licence.
 *
 * Copyright (C) 2022 University of Münster (WWU), Germany
 * Written by Jim Jones <jim.jones@uni-muenster.de>
 *
 **********************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "utils/rel.h"

#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#else
#include "optimizer/optimizer.h"
#endif

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/reloptions.h"

#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif

#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "nodes/pg_list.h"

#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <commands/explain.h>
#include <libxml/tree.h>
#include <catalog/pg_collation.h>

#include <funcapi.h>

#include "lib/stringinfo.h"

#include <utils/lsyscache.h>

#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"
#include "utils/formatting.h"

#include "catalog/pg_operator.h"
#include "utils/syscache.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "access/reloptions.h"
#include "catalog/pg_namespace.h"

#define OAI_FDW_VERSION "1.3.0"
#define OAI_REQUEST_LISTRECORDS "ListRecords"
#define OAI_REQUEST_LISTIDENTIFIERS "ListIdentifiers"
#define OAI_REQUEST_IDENTIFY "Identify"
#define OAI_REQUEST_LISTSETS "ListSets"
#define OAI_REQUEST_GETRECORD "GetRecord"
#define OAI_REQUEST_LISTMETADATAFORMATS "ListMetadataFormats"
#define OAI_REQUEST_LISTSETS "ListSets"
#define OAI_REQUEST_CONNECTTIMEOUT 300
#define OAI_REQUEST_MAXRETRY 3

#define OAI_XML_ROOT_ELEMENT "OAI-PMH"
#define OAI_NODE_IDENTIFIER "identifier"
#define OAI_NODE_URL "url"
#define OAI_NODE_HTTP_PROXY "http_proxy"
#define OAI_NODE_HTTPS_PROXY "https_proxy"
#define OAI_NODE_PROXY_USER "proxy_user"
#define OAI_NODE_PROXY_USER_PASSWORD "proxy_user_password"
#define OAI_NODE_CONNECTTIMEOUT "connect_timeout"
#define OAI_NODE_CONNECTRETRY "connect_retry"
#define OAI_NODE_CONTENT "content"
#define OAI_NODE_DATESTAMP "datestamp"
#define OAI_NODE_SETSPEC "setspec"
#define OAI_NODE_METADATAPREFIX "metadataprefix"
#define OAI_NODE_FROM "from"
#define OAI_NODE_UNTIL "until"
#define OAI_NODE_STATUS "status"
#define OAI_NODE_RESUMPTIONTOKEN "resumptionToken"
#define OAI_NODE_OPTION "oai_node"

#define OAI_ERROR_ID_DOES_NOT_EXIST "idDoesNotExist"
#define OAI_ERROR_NO_RECORD_MATCH "noRecordsMatch"

#define OAI_SUCCESS 0
#define OAI_FAIL 1
#define OAI_MALFORMED_URL 3
#define OAI_UNKNOWN_REQUEST 2
#define ERROR_CODE_MISSING_PREFIX 1

PG_MODULE_MAGIC;

typedef struct OAIFdwState {

	int 	current_row;
	int 	numcols;
	int 	numfdwcols;
	int		rowcount;
	long    maxretries;
	long    connectTimeout;
	char	*completeListSize;
	char	*identifier;
	char 	*document;
	char 	*date;
	char 	*set;
	char 	*url;
	char    *proxy;
	char    *proxyType;
	char    *proxyUser;
	char    *proxyUserPassword;
	char 	*from;
	char 	*until;
	char 	*metadataPrefix;
	char 	*resumptionToken;
	char	*requestType;
	char	*current_identifier;

	Oid 		foreigntableid;
	xmlDocPtr	xmldoc;

} OAIFdwState;

typedef struct OAIRecord {

	char *identifier;
	char *content;
	char *datestamp;
	char *metadataPrefix;
	char *completeListSize;
	bool isDeleted;
	ArrayType *setsArray;

} OAIRecord;

typedef struct OAIColumn {

	int   id;
	char *oaiNode;
	char *label;
	char *function;

} OAIColumn;


typedef struct OAIFdwTableOptions {

	Oid foreigntableid;
	int numcols;
	int numfdwcols;
	long maxretries;
	long connectTimeout;
	char *metadataPrefix;
	char *from;
	char *until;
	char *url;
	char *proxy;
	char *proxyType;
	char *proxyUser;
	char *proxyUserPassword;
	char *set;
	char *identifier;
	char *requestType;

} OAIFdwTableOptions;

typedef struct OAIMetadataFormat {

	char *metadataPrefix;
	char *schema;
	char *metadataNamespace;

} OAIMetadataFormat;

typedef struct OAISet {

	char *setSpec;
	char *setName;

} OAISet;

typedef struct OAIFdwIdentityNode {

	char *name;
	char *description;

} OAIFdwIdentityNode;

struct string {
	char *ptr;
	size_t len;
};

struct OAIFdwOption
{
	const char* optname;
	Oid optcontext;     /* Oid of catalog in which option may appear */
	bool optrequired;   /* Flag mandatory options */
	bool optfound;      /* Flag whether options was specified by user */
};

struct MemoryStruct {
  char *memory;
  size_t size;
};


static struct OAIFdwOption valid_options[] =
{

	{OAI_NODE_URL, ForeignServerRelationId, true, false},
	{OAI_NODE_METADATAPREFIX, ForeignServerRelationId, false, false},
	{OAI_NODE_HTTP_PROXY, ForeignServerRelationId, false, false},
	{OAI_NODE_HTTPS_PROXY, ForeignServerRelationId, false, false},
	{OAI_NODE_PROXY_USER, ForeignServerRelationId, false, false},
	{OAI_NODE_PROXY_USER_PASSWORD, ForeignServerRelationId, false, false},
	{OAI_NODE_CONNECTTIMEOUT, ForeignServerRelationId, false, false},
	{OAI_NODE_CONNECTRETRY, ForeignServerRelationId, false, false},

	{OAI_NODE_IDENTIFIER, ForeignTableRelationId, false, false},
	{OAI_NODE_METADATAPREFIX, ForeignTableRelationId, true, false},
	{OAI_NODE_SETSPEC, ForeignTableRelationId, false, false},
	{OAI_NODE_FROM, ForeignTableRelationId, false, false},
	{OAI_NODE_UNTIL, ForeignTableRelationId, false, false},

	{OAI_NODE_OPTION, AttributeRelationId, true, false},

	/* EOList option */
	{NULL, InvalidOid, false, false}
};

#define option_count (sizeof(valid_options)/sizeof(struct OAIFdwOption))


extern Datum oai_fdw_handler(PG_FUNCTION_ARGS);
extern Datum oai_fdw_validator(PG_FUNCTION_ARGS);
extern Datum oai_fdw_version(PG_FUNCTION_ARGS);
extern Datum oai_fdw_listMetadataFormats(PG_FUNCTION_ARGS);
extern Datum oai_fdw_listSets(PG_FUNCTION_ARGS);
extern Datum oai_fdw_identity(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(oai_fdw_handler);
PG_FUNCTION_INFO_V1(oai_fdw_validator);
PG_FUNCTION_INFO_V1(oai_fdw_version);
PG_FUNCTION_INFO_V1(oai_fdw_listMetadataFormats);
PG_FUNCTION_INFO_V1(oai_fdw_listSets);
PG_FUNCTION_INFO_V1(oai_fdw_identity);

static void OAIFdwGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void OAIFdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *OAIFdwGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
static void OAIFdwBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *OAIFdwIterateForeignScan(ForeignScanState *node);
static void OAIFdwReScanForeignScan(ForeignScanState *node);
static void OAIFdwEndForeignScan(ForeignScanState *node);
static TupleTableSlot *OAIFdwExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *OAIFdwExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *OAIFdwExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static List* OAIFdwImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid serverOid);

static void appendTextArray(ArrayType **array, char* text_element) ;
static int ExecuteOAIRequest(OAIFdwState *state);
static void CreateOAITuple(TupleTableSlot *slot, OAIFdwState *state, OAIRecord *oai );
static OAIRecord *FetchNextOAIRecord(OAIFdwState *state);
static void LoadOAIRecords(OAIFdwState *state);

static void deparseExpr(Expr *expr, OAIFdwTableOptions *opts);
static char *datumToString(Datum datum, Oid type);
static char *GetOAINodeFromColumn(Oid foreigntableid, int16 attnum);
static void deparseWhereClause(List *conditions, OAIFdwTableOptions *opts);
static void deparseSelectColumns(OAIFdwTableOptions *opts, List* exprs);
static void OAIRequestPlanner(OAIFdwTableOptions *opts, ForeignTable *ft, RelOptInfo *baserel);
static char *deparseTimestamp(Datum datum);
static int CheckURL(char *url);
static OAIFdwState *GetServerInfo(const char *srvname);
static List *GetMetadataFormats(OAIFdwState *state);
static List *GetIdentity(OAIFdwState *state);
static List *GetSets(OAIFdwState *state);

Datum oai_fdw_handler(PG_FUNCTION_ARGS) {

	FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	fdwroutine->GetForeignRelSize = OAIFdwGetForeignRelSize;
	fdwroutine->GetForeignPaths = OAIFdwGetForeignPaths;
	fdwroutine->GetForeignPlan = OAIFdwGetForeignPlan;
	fdwroutine->BeginForeignScan = OAIFdwBeginForeignScan;
	fdwroutine->IterateForeignScan = OAIFdwIterateForeignScan;
	fdwroutine->ReScanForeignScan = OAIFdwReScanForeignScan;
	fdwroutine->EndForeignScan = OAIFdwEndForeignScan;

	fdwroutine->ExecForeignUpdate = OAIFdwExecForeignUpdate;
	fdwroutine->ExecForeignDelete = OAIFdwExecForeignDelete;
	fdwroutine->ExecForeignInsert = OAIFdwExecForeignInsert;
	fdwroutine->ImportForeignSchema = OAIFdwImportForeignSchema;

	PG_RETURN_POINTER(fdwroutine);

}

Datum oai_fdw_version(PG_FUNCTION_ARGS) {

	StringInfoData buffer;
	initStringInfo(&buffer);

	appendStringInfo(&buffer,"oai fdw = %s,",OAI_FDW_VERSION);
	appendStringInfo(&buffer," libxml = %s,",LIBXML_DOTTED_VERSION);
	appendStringInfo(&buffer," libcurl = %s",curl_version());

    PG_RETURN_TEXT_P(cstring_to_text(buffer.data));
}

OAIFdwState *GetServerInfo(const char *srvname) {

	OAIFdwState *state = (OAIFdwState *) palloc0(sizeof(OAIFdwState));
	ForeignServer *server = GetForeignServerByName(srvname, true);

	elog(DEBUG1,"%s called: '%s'",__func__,srvname);

	if(server) {

		ListCell   *cell;

		foreach(cell, server->options) {

			DefElem *def = lfirst_node(DefElem, cell);

			elog(DEBUG1,"  %s parsing node '%s': %s",__func__,def->defname,defGetString(def));

			if(strcmp(def->defname,OAI_NODE_URL)==0) {

				state->url = defGetString(def);

			}

			if(strcmp(def->defname,OAI_NODE_HTTP_PROXY)==0) {

				state->proxy = defGetString(def);
				state->proxyType = OAI_NODE_HTTP_PROXY;

			}

			if(strcmp(def->defname,OAI_NODE_PROXY_USER)==0) {

				state->proxyUser = defGetString(def);

			}

			if(strcmp(def->defname,OAI_NODE_PROXY_USER_PASSWORD)==0) {

				state->proxy = defGetString(def);

			}

			if(strcmp(def->defname,OAI_NODE_CONNECTTIMEOUT)==0) {

				char *tailpt;
				char *timeout_str =  defGetString(def);

				state->connectTimeout = strtol(timeout_str, &tailpt, 0);

			}

		}

	} else {

		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
						errmsg("FOREIGN SERVER does not exist: '%s'",srvname)));


	}

	return state;
}

Datum oai_fdw_identity(PG_FUNCTION_ARGS) {

	text *srvname_text = PG_GETARG_TEXT_P(0);
	const char * srvname = text_to_cstring(srvname_text);
	OAIFdwState *state = GetServerInfo(srvname);

	FuncCallContext     *funcctx;
	AttInMetadata       *attinmeta;
	TupleDesc            tupdesc;
	int                  call_cntr;
	int                  max_calls;
	MemoryContext        oldcontext;
	List                *identity;

	if (SRF_IS_FIRSTCALL())	{

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		identity = GetIdentity(state);
		funcctx->user_fctx = identity;

		if(identity) funcctx->max_calls = identity->length;
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("function returning record called in context that cannot accept type record")));


		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);

	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls) {

		char       **values;
		HeapTuple    tuple;
		Datum        result;
		int MAX_SIZE = 512;

		OAIFdwIdentityNode *set = (OAIFdwIdentityNode *) list_nth((List*) funcctx->user_fctx, call_cntr);

		values = (char **) palloc(2 * sizeof(char *));
		values[0] = (char *) palloc(MAX_SIZE * sizeof(char));
		values[1] = (char *) palloc(MAX_SIZE * sizeof(char));

		snprintf(values[0], MAX_SIZE, "%s",set->name);
		snprintf(values[1], MAX_SIZE, "%s",set->description);

		tuple = BuildTupleFromCStrings(attinmeta, values);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);

	} else {

		SRF_RETURN_DONE(funcctx);

	}

}

Datum oai_fdw_listSets(PG_FUNCTION_ARGS) {

	text *srvname_text = PG_GETARG_TEXT_P(0);
	char *srvname = text_to_cstring(srvname_text);
	OAIFdwState *state = GetServerInfo(srvname);

	MemoryContext   oldcontext;
	FuncCallContext     *funcctx;
	AttInMetadata       *attinmeta;
	TupleDesc            tupdesc;
	int                  call_cntr;
	int                  max_calls;

	if (SRF_IS_FIRSTCALL())	{

		List *sets;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		sets = GetSets(state);
		funcctx->user_fctx = sets;

		if(sets) funcctx->max_calls = sets->length;
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					       errmsg("function returning record called in context that cannot accept type record")));


		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);

	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls) {

		char       **values;
		HeapTuple    tuple;
		Datum        result;

		OAISet *set = (OAISet *) list_nth((List*) funcctx->user_fctx, call_cntr);

		int MAX_SIZE = 512;

		values = (char **) palloc(2 * sizeof(char *));
		values[0] = (char *) palloc(MAX_SIZE * sizeof(char));
		values[1] = (char *) palloc(MAX_SIZE * sizeof(char));

		snprintf(values[0], MAX_SIZE, "%s",set->setSpec);
		snprintf(values[1], MAX_SIZE, "%s",set->setName);

		tuple = BuildTupleFromCStrings(attinmeta, values);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);

	} else {

		SRF_RETURN_DONE(funcctx);

	}


}

Datum oai_fdw_listMetadataFormats(PG_FUNCTION_ARGS) {

	text *srvname_text = PG_GETARG_TEXT_P(0);
	const char * srvname = text_to_cstring(srvname_text);
	OAIFdwState *state = GetServerInfo(srvname);

	FuncCallContext     *funcctx;
	int                  call_cntr;
	int                  max_calls;
	TupleDesc            tupdesc;
	AttInMetadata       *attinmeta;
	MemoryContext  oldcontext;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL()) {

		List *formats;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		formats = GetMetadataFormats(state);
		funcctx->user_fctx = formats;

		if(formats)	funcctx->max_calls = formats->length;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					       errmsg("function returning record called in context that cannot accept type record")));

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);

	}


	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)    /* do when there is more left to send */
	    {
	        char             **values;
	        HeapTuple          tuple;
	        Datum              result;
	        OAIMetadataFormat *format = (OAIMetadataFormat *) list_nth((List*) funcctx->user_fctx, call_cntr);
	        const size_t MAX_SIZE = 512;

	        values = (char **) palloc(3 * sizeof(char *));
	        values[0] = (char *) palloc(MAX_SIZE * sizeof(char));
	        values[1] = (char *) palloc(MAX_SIZE * sizeof(char));
	        values[2] = (char *) palloc(MAX_SIZE * sizeof(char));

	        snprintf(values[0], MAX_SIZE, "%s", format->metadataPrefix);
	        snprintf(values[1], MAX_SIZE, "%s", format->schema);
	        snprintf(values[2], MAX_SIZE, "%s", format->metadataNamespace);

	        tuple = BuildTupleFromCStrings(attinmeta, values);

	        /* make the tuple into a datum */
	        result = HeapTupleGetDatum(tuple);

	        SRF_RETURN_NEXT(funcctx, result);
	    }
	    else    /* do when there is no more left */
	    {
	        SRF_RETURN_DONE(funcctx);
	    }

}

Datum oai_fdw_validator(PG_FUNCTION_ARGS) {

	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;
	struct OAIFdwOption* opt;

	/* Initialize found state to not found */
	for (opt = valid_options; opt->optname; opt++) {

		opt->optfound = false;

	}

	foreach (cell, options_list) {

		DefElem* def = (DefElem*) lfirst(cell);
		bool optfound = false;

		for (opt = valid_options; opt->optname; opt++) {

			if (catalog == opt->optcontext && strcmp(opt->optname, def->defname)==0) {

				/* Mark that this user option was found */
				opt->optfound = optfound = true;

				if(strlen(defGetString(def))==0) {

					ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						 errmsg("empty value in option '%s'",opt->optname)));

				}

				if(strcmp(opt->optname, OAI_NODE_URL)==0 || strcmp(opt->optname, OAI_NODE_HTTP_PROXY)==0 || strcmp(opt->optname, OAI_NODE_HTTPS_PROXY)==0){

					int return_code = CheckURL(defGetString(def));

					if(return_code !=OAI_SUCCESS ) {

						if(return_code == OAI_MALFORMED_URL) {

							ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("malformed URL in option '%s' (%d): '%s'",opt->optname,return_code, defGetString(def)),
								 errhint("make sure the given URL is valid and contains supported protocol (http:// or  https://).")));

						} else {

							ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: '%s'",opt->optname, defGetString(def))));

						}


					}

				}

				if(strcmp(opt->optname, OAI_NODE_CONNECTTIMEOUT)==0){

					char *endptr;
					char *timeout_str =  defGetString(def);
					long timeout_val = strtol(timeout_str, &endptr, 0);

					if (timeout_str[0] == '\0' || *endptr != '\0' || timeout_val < 0) {

						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
									errmsg("invalid %s: %s", def->defname,timeout_str),
						       		errhint("expected values are positive integers (timeout in seconds)")));

					}
				}

				if(strcmp(opt->optname, OAI_NODE_CONNECTRETRY)==0){

					char *endptr;
					char *retry_str =  defGetString(def);
					long retry_val = strtol(retry_str, &endptr, 0);

					if (retry_str[0] == '\0' || *endptr != '\0' || retry_val < 0) {

						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
									errmsg("invalid %s: %s", def->defname,retry_str),
						       		errhint("expected values are positive integers (retry attempts in case of failure)")));

					}
				}


				if (strcmp(opt->optname, OAI_NODE_OPTION) == 0) {

					if(strcmp(defGetString(def), OAI_NODE_IDENTIFIER) !=0 &&
					   strcmp(defGetString(def), OAI_NODE_METADATAPREFIX) !=0 &&
					   strcmp(defGetString(def), OAI_NODE_SETSPEC) !=0 &&
					   strcmp(defGetString(def), OAI_NODE_DATESTAMP) !=0 &&
					   strcmp(defGetString(def), OAI_NODE_CONTENT) !=0 &&
					   strcmp(defGetString(def), OAI_NODE_STATUS) !=0) {

						ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							 errmsg("invalid %s option '%s'",OAI_NODE_OPTION, defGetString(def)),
							 errhint("Valid column options for oai_fdw are:\nCREATE SERVER: '%s', '%s'\nCREATE TABLE: '%s','%s', '%s', '%s' and '%s'",
								OAI_NODE_URL,
								OAI_NODE_METADATAPREFIX,
								OAI_NODE_METADATAPREFIX,
								OAI_NODE_SETSPEC,
								OAI_NODE_FROM,
								OAI_NODE_UNTIL,
								OAI_NODE_STATUS)));

					}

				}

				break;
			}

		}

		if (!optfound) {

			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("invalid oai_fdw option \"%s\"", def->defname)));

		}
	}


	for (opt = valid_options; opt->optname; opt++) {

		/* Required option for this catalog type is missing? */
		if (catalog == opt->optcontext && opt->optrequired && !opt->optfound) {

			ereport(ERROR, (
			    errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
			    errmsg("required option '%s' is missing", opt->optname)));

		}

	}

	PG_RETURN_VOID();
}

static List *GetIdentity(OAIFdwState *state) {

	int oaiExecuteResponse;
	List *result = NIL;

	elog(DEBUG1, "%s called",__func__);

	state->requestType = OAI_REQUEST_IDENTIFY;

	oaiExecuteResponse = ExecuteOAIRequest(state);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlNodePtr oai_root;
		xmlNodePtr Identity;
		xmlNodePtr xmlroot = xmlDocGetRootElement(state->xmldoc);

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next) {

			if (oai_root->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(oai_root->name, (xmlChar*) OAI_REQUEST_IDENTIFY) != 0) continue;

			for (Identity = oai_root->children; Identity != NULL; Identity = Identity->next) {

				OAIFdwIdentityNode *node = (OAIFdwIdentityNode *)palloc0(sizeof(OAIFdwIdentityNode));

				if (Identity->type != XML_ELEMENT_NODE)	continue;

				node->name = (char*) Identity->name;
				node->description = (char*) xmlNodeGetContent(Identity);

				result = lappend(result, node);

			}

		}

		xmlFreeNode(oai_root);
		xmlFreeNode(Identity);
		xmlFreeNode(xmlroot);

	}

	elog(DEBUG1, "%s => finished",__func__);

	return result;

}


static List *GetSets(OAIFdwState *state) {

	int oaiExecuteResponse;
	List *result = NIL;

	elog(DEBUG1, "%s called",__func__);

	state->requestType = OAI_REQUEST_LISTSETS;

	oaiExecuteResponse = ExecuteOAIRequest(state);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlNodePtr oai_root;
		xmlNodePtr ListSets;
		xmlNodePtr SetElement;
		xmlNodePtr xmlroot = xmlDocGetRootElement(state->xmldoc);

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next) {

			if (oai_root->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(oai_root->name, (xmlChar*) OAI_REQUEST_LISTSETS) != 0) continue;

			for (ListSets = oai_root->children; ListSets != NULL; ListSets = ListSets->next) {

				OAISet *set = (OAISet *)palloc0(sizeof(OAISet));

				if (ListSets->type != XML_ELEMENT_NODE)	continue;
				if (xmlStrcmp(ListSets->name, (xmlChar*) "set") != 0) continue;

				for (SetElement = ListSets->children; SetElement != NULL; SetElement = SetElement->next) {

					if (SetElement->type != XML_ELEMENT_NODE)	continue;

					if (xmlStrcmp(SetElement->name, (xmlChar*) "setSpec") == 0) {

						set->setSpec = (char*) xmlNodeGetContent(SetElement);


					} else if (xmlStrcmp(SetElement->name, (xmlChar*) "setName") == 0) {

						set->setName = (char*) xmlNodeGetContent(SetElement);

					}

				}

				result = lappend(result, set);

			}

		}

	}

	elog(DEBUG1, "%s => finished",__func__);

	return result;
}

static List *GetMetadataFormats(OAIFdwState *state) {

	int oaiExecuteResponse;
	List *result = NIL;

	elog(DEBUG1, "  %s called",__func__);

	state->requestType = OAI_REQUEST_LISTMETADATAFORMATS;

	oaiExecuteResponse = ExecuteOAIRequest(state);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlNodePtr oai_root;
		xmlNodePtr ListMetadataFormats;
		xmlNodePtr MetadataElement;
		xmlNodePtr xmlroot = xmlDocGetRootElement(state->xmldoc);

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next) {

			if (oai_root->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(oai_root->name, (xmlChar*) OAI_REQUEST_LISTMETADATAFORMATS) != 0) continue;

			for (ListMetadataFormats = oai_root->children; ListMetadataFormats != NULL; ListMetadataFormats = ListMetadataFormats->next) {

				OAIMetadataFormat *format = (OAIMetadataFormat *)palloc0(sizeof(OAIMetadataFormat));

				if (ListMetadataFormats->type != XML_ELEMENT_NODE)	continue;
				if (xmlStrcmp(ListMetadataFormats->name, (xmlChar*) "metadataFormat") != 0) continue;

				for (MetadataElement = ListMetadataFormats->children; MetadataElement != NULL; MetadataElement = MetadataElement->next) {

					if (MetadataElement->type != XML_ELEMENT_NODE)	continue;

					if (xmlStrcmp(MetadataElement->name, (xmlChar*) "metadataPrefix") == 0) {

						format->metadataPrefix = (char*) xmlNodeGetContent(MetadataElement);

					} else if (xmlStrcmp(MetadataElement->name, (xmlChar*) "schema") == 0) {

						format->schema = (char*) xmlNodeGetContent(MetadataElement);

					} else if (xmlStrcmp(MetadataElement->name, (xmlChar*) "metadataNamespace") == 0) {

						format->metadataNamespace = (char*) xmlNodeGetContent(MetadataElement);

					}

				}

				result = lappend(result, format);

			}

		}

	}

	elog(DEBUG1, "  %s => finished.",__func__);

	return result;
}

static int CheckURL(char *url) {

	CURLUcode code;
	CURLU *handler = curl_url();

	elog(DEBUG1,"%s called > '%s'",__func__,url);

	code = curl_url_set(handler, CURLUPART_URL,url, 0);

	curl_url_cleanup(handler);

	elog(DEBUG1,"  %s handler return code: %u",__func__,code);

	if(code != 0) {

		elog(DEBUG1,"%s: invalid URL (%u) > '%s'",__func__,code,url);

		return code;

	}

	return OAI_SUCCESS;

}


static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
  size_t realsize = size * nmemb;
  struct MemoryStruct *mem = (struct MemoryStruct *)userp;

  char *ptr = repalloc(mem->memory, mem->size + realsize + 1);
  if(!ptr) {
    /* out of memory! */
    printf("not enough memory (realloc returned NULL)\n");
    return 0;
  }

  mem->memory = ptr;
  memcpy(&(mem->memory[mem->size]), contents, realsize);
  mem->size += realsize;
  mem->memory[mem->size] = 0;

  return realsize;
}

static int ExecuteOAIRequest(OAIFdwState *state) {

	CURL *curl;
	CURLcode res;
	StringInfoData url_buffer;
	char errbuf[CURL_ERROR_SIZE];
	struct MemoryStruct chunk;
	chunk.memory = palloc(1);
	chunk.size = 0;    /* no data at this point */

	initStringInfo(&url_buffer);

	elog(DEBUG1,"%s called: base url > '%s' ",__func__,state->url);

	appendStringInfo(&url_buffer,"verb=%s",state->requestType);

	if(strcmp(state->requestType,OAI_REQUEST_LISTRECORDS)==0) {

		if(state->set) {

			elog(DEBUG1,"  %s (%s): appending 'set' > %s",__func__,state->requestType,state->set);
			appendStringInfo(&url_buffer,"&set=%s",state->set);

		}

		if(state->from) {

			elog(DEBUG1,"  %s (%s): appending 'from' > %s",__func__,state->requestType,state->from);
			appendStringInfo(&url_buffer,"&from=%s",state->from);

		}

		if(state->until) {

			elog(DEBUG1,"  %s (%s): appending 'until' > %s",__func__,state->requestType,state->until);
			appendStringInfo(&url_buffer,"&until=%s",state->until);

		}

		if(state->metadataPrefix) {

			elog(DEBUG1,"  %s (%s): appending 'metadataPrefix' > %s",__func__,state->requestType,state->metadataPrefix);
			appendStringInfo(&url_buffer,"&metadataPrefix=%s",state->metadataPrefix);

		}

		if(state->resumptionToken) {

			elog(DEBUG1,"  %s (%s): appending 'resumptionToken' > %s",__func__,state->requestType,state->resumptionToken);
			resetStringInfo(&url_buffer);
			appendStringInfo(&url_buffer,"verb=%s&resumptionToken=%s",state->requestType,state->resumptionToken);

			state->resumptionToken = NULL;

		}

	} else if(strcmp(state->requestType,OAI_REQUEST_GETRECORD)==0) {

		if(state->identifier) {

			elog(DEBUG1,"  %s (%s): appending 'identifier' > %s",__func__,state->requestType,state->identifier);
			appendStringInfo(&url_buffer,"&identifier=%s",state->identifier);

		}

		if(state->metadataPrefix) {

			elog(DEBUG1,"  %s (%s): appending 'metadataPrefix' > %s",__func__,state->requestType,state->metadataPrefix);
			appendStringInfo(&url_buffer,"&metadataPrefix=%s",state->metadataPrefix);

		}

	} else if(strcmp(state->requestType,OAI_REQUEST_LISTIDENTIFIERS)==0) {


		if(state->set) {

			elog(DEBUG1,"  %s (%s): appending 'set' > %s",__func__,state->requestType,state->set);
			appendStringInfo(&url_buffer,"&set=%s",state->set);

		}

		if(state->from) {

			elog(DEBUG1,"  %s (%s): appending 'from' > %s",__func__,state->requestType,state->from);
			appendStringInfo(&url_buffer,"&from=%s",state->from);


		}

		if(state->until) {

			elog(DEBUG1,"  %s (%s): appending 'until' > %s",__func__,state->requestType,state->until);
			appendStringInfo(&url_buffer,"&until=%s",state->until);

		}

		if(state->metadataPrefix) {

			elog(DEBUG1,"  %s (%s): appending 'metadataPrefix' > %s",__func__,state->requestType,state->metadataPrefix);
			appendStringInfo(&url_buffer,"&metadataPrefix=%s",state->metadataPrefix);

		}


		if(state->resumptionToken) {

			elog(DEBUG1,"  %s (%s): appending 'resumptionToken' > %s",__func__,state->requestType,state->resumptionToken);
			resetStringInfo(&url_buffer);
			appendStringInfo(&url_buffer,"verb=%s&resumptionToken=%s",state->requestType,state->resumptionToken);

			state->resumptionToken = NULL;

		}

	} else {

		if(strcmp(state->requestType,OAI_REQUEST_LISTMETADATAFORMATS)!=0 &&
		   strcmp(state->requestType,OAI_REQUEST_LISTSETS)!=0 &&
		   strcmp(state->requestType,OAI_REQUEST_IDENTIFY)!=0) {

			return OAI_UNKNOWN_REQUEST;

		}

	}


	elog(DEBUG1,"  %s (%s): url build > %s?%s",__func__,state->requestType,state->url,url_buffer.data);

	curl_global_init(CURL_GLOBAL_ALL);
	curl = curl_easy_init();

	if(curl) {

		errbuf[0] = 0;

		curl_easy_setopt(curl, CURLOPT_URL, state->url);
		curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
		curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

		if(!state->connectTimeout) {

			state->connectTimeout = OAI_REQUEST_CONNECTTIMEOUT;

		}

		curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, state->connectTimeout);
		elog(DEBUG1, "  %s (%s): timeout > %ld",__func__,state->requestType,state->connectTimeout);

		if(!state->maxretries) {

			state->maxretries = OAI_REQUEST_MAXRETRY;

		}

		elog(DEBUG1, "  %s (%s): max retry > %ld",__func__,state->requestType,state->maxretries);

		/* Proxy support: added in version 1.1.0 */
		if(state->proxy) {

			elog(DEBUG1, "  %s (%s): proxy URL > '%s'",__func__,state->requestType,state->proxy);

			curl_easy_setopt(curl, CURLOPT_PROXY, state->proxy);

			if(strcmp(state->proxyType,OAI_NODE_HTTP_PROXY)==0) {

				elog(DEBUG1, "  %s (%s): proxy protocol > 'HTTP'",__func__,state->requestType);
				curl_easy_setopt(curl, CURLOPT_PROXYTYPE, CURLPROXY_HTTP);

			} else if(strcmp(state->proxyType,OAI_NODE_HTTPS_PROXY)==0) {

				elog(DEBUG1, "  %s (%s): proxy protocol > 'HTTPS'",__func__,state->requestType);
				curl_easy_setopt(curl, CURLOPT_PROXYTYPE, CURLPROXY_HTTPS);

			}

			if(state->proxyUser) {

				elog(DEBUG1, "  %s (%s): entering proxy user ('%s').",__func__,state->requestType,state->proxyUser);
				curl_easy_setopt(curl, CURLOPT_PROXYUSERNAME, state->proxyUser);

			}

			if(state->proxyUserPassword) {

				elog(DEBUG1, "  %s (%s): entering proxy user's password.",__func__,state->requestType);
				curl_easy_setopt(curl, CURLOPT_PROXYUSERPWD, state->proxyUserPassword);

			}

		}

		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, url_buffer.data);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
		curl_easy_setopt(curl, CURLOPT_FAILONERROR, true);

		elog(DEBUG2, "  %s (%s): performing cURL request ... ",__func__,state->requestType);

		res = curl_easy_perform(curl);

		if (res != CURLE_OK) {

			for (long i = 1; i <= state->maxretries && (res = curl_easy_perform(curl)) != CURLE_OK; i++) {

				elog(WARNING, "%s (%s): connection to '%s' failed (%ld)",__func__,state->requestType, state->url,i);

			}

		}

		if (res != CURLE_OK) {

			size_t len = strlen(errbuf);
			fprintf(stderr, "\nlibcurl: (%d) ", res);

			state->xmldoc = NULL;
			curl_easy_cleanup(curl);

			if(len) {

				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						 errmsg("%s => (%u) %s%s",__func__, res, errbuf,
										((errbuf[len - 1] != '\n') ? "\n" : ""))));
			}  else {

				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						 errmsg("%s => (%u) '%s'\n",__func__, res, curl_easy_strerror(res))));
			}


		} else {

			long response_code;
			curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
			state->xmldoc = xmlReadMemory(chunk.memory, chunk.size, NULL, NULL, XML_PARSE_NOBLANKS);

			elog(DEBUG1, "%s (%s) => http response code: %ld",__func__,state->requestType,response_code);
			elog(DEBUG1, "%s (%s) => http response size: %ld",__func__,state->requestType,chunk.size);
		}

	}

	pfree(chunk.memory);
	curl_easy_cleanup(curl);
	curl_global_cleanup();

	return OAI_SUCCESS;


}

static void OAIRequestPlanner(OAIFdwTableOptions *opts, ForeignTable *ft, RelOptInfo *baserel) {

	List *conditions = baserel->baserestrictinfo;
	bool hasContentForeignColumn = false;

#if PG_VERSION_NUM < 130000
	Relation rel = heap_open(ft->relid, NoLock);
#else
	Relation rel = table_open(ft->relid, NoLock);;
#endif

	elog(DEBUG1, "%s called.",__func__);


	/* The default request type is OAI_REQUEST_LISTRECORDS.
	 * This can be altered depending on the columns used
	 * in the WHERE and SELECT clauses */
	opts->requestType = OAI_REQUEST_LISTRECORDS;
	opts->numcols = rel->rd_att->natts;
	opts->foreigntableid = ft->relid;

	for (int i = 0; i < rel->rd_att->natts ; i++) {

		List *options = GetForeignColumnOptions(ft->relid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			DefElem *def = (DefElem*) lfirst(lc);
			opts->numfdwcols++;

			if (strcmp(def->defname, OAI_NODE_OPTION)==0) {

				char *option_value = defGetString(def);

				if (strcmp(option_value,OAI_NODE_STATUS)==0){

					if (rel->rd_att->attrs[i].atttypid != BOOLOID) {

						ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("invalid data type for '%s.%s': %d",
									NameStr(rel->rd_rel->relname),
									NameStr(rel->rd_att->attrs[i].attname),
									rel->rd_att->attrs[i].atttypid),
							errhint("OAI %s must be of type 'boolean'.",
									OAI_NODE_STATUS)));
					}

				} else if (strcmp(option_value,OAI_NODE_IDENTIFIER)==0 || strcmp(option_value,OAI_NODE_METADATAPREFIX)==0){

					if (rel->rd_att->attrs[i].atttypid != TEXTOID &&
						rel->rd_att->attrs[i].atttypid != VARCHAROID) {

						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s must be of type 'text' or 'varchar'.",
										option_value)));

					}


				} else if (strcmp(option_value,OAI_NODE_CONTENT)==0){

					hasContentForeignColumn = true;

					if (rel->rd_att->attrs[i].atttypid != TEXTOID &&
						rel->rd_att->attrs[i].atttypid != VARCHAROID &&
						rel->rd_att->attrs[i].atttypid != XMLOID) {

						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s expects one of the following types: 'xml', 'text' or 'varchar'.",
										OAI_NODE_CONTENT)));
					}


				} else if (strcmp(option_value,OAI_NODE_SETSPEC)==0){

					if (rel->rd_att->attrs[i].atttypid != TEXTARRAYOID &&
					    rel->rd_att->attrs[i].atttypid != VARCHARARRAYOID) {

						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s expects one of the following types: 'text[]', 'varchar[]'.",
										OAI_NODE_SETSPEC)));

					}


				}  else if (strcmp(option_value,OAI_NODE_DATESTAMP)==0){

					if (rel->rd_att->attrs[i].atttypid != TIMESTAMPOID) {

						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s expects a 'timestamp'.",
										OAI_NODE_DATESTAMP)));

					}

				}

			}

		}

	}


	/* If the foreign table has no "oai_attribute = 'content'" there is no need
	 * to retrieve the document itself. The ListIdentifiers request lists the
	 * whole OAI header */
	if(!hasContentForeignColumn) {

		opts->requestType = OAI_REQUEST_LISTIDENTIFIERS;
		elog(DEBUG1,"  %s: the foreign table '%s' has no 'content' OAI node. Request type set to '%s'",
				__func__,NameStr(rel->rd_rel->relname),OAI_REQUEST_LISTIDENTIFIERS);

	}



	if(opts->numfdwcols!=0) deparseSelectColumns(opts,baserel->reltarget->exprs);

	deparseWhereClause(conditions,opts);

#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	elog(DEBUG1, "%s => finished.",__func__);
}

static TupleTableSlot *OAIFdwExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot) {

	ereport(
	  ERROR,(
	    errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
		errmsg("Operation not supported."),
  		errhint("The OAI Foreign Data Wrapper does not support UPDATE queries.")
	  )
	);

	return NULL;
}

static TupleTableSlot *OAIFdwExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot) {

	ereport(
	  ERROR,(
	    errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
		errmsg("Operation not supported."),
  		errhint("The OAI Foreign Data Wrapper does not support INSERT queries.")
	  )
	);

	return NULL;
}

static TupleTableSlot *OAIFdwExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot) {

	ereport(
	  ERROR,(
	    errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
		errmsg("Operation not supported."),
  		errhint("The OAI Foreign Data Wrapper does not support DELETE queries.")
	  )
	);

	return NULL;
}

static char *GetOAINodeFromColumn(Oid foreigntableid, int16 attnum) {

	List *options;
	char *optionValue = NULL;

#if PG_VERSION_NUM < 130000
	Relation rel = heap_open(foreigntableid, NoLock);
#else
	Relation rel = table_open(foreigntableid, NoLock);;
#endif

	elog(DEBUG1, "  %s called",__func__);


	for (int i = 0; i < rel->rd_att->natts ; i++) {

		ListCell *lc;
		options = GetForeignColumnOptions(foreigntableid, i+1);

		if(rel->rd_att->attrs[i].attnum == attnum){

			foreach (lc, options) {

				DefElem *def = (DefElem*) lfirst(lc);

				if (strcmp(def->defname, OAI_NODE_OPTION)==0) {

					optionValue = defGetString(def);

					break;

				}

			}

		}

	}

	//table_close(rel, NoLock);
#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	return optionValue;

}

static char *datumToString(Datum datum, Oid type) {

	regproc typoutput;
	HeapTuple tuple;
	char *result;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));

	if (!HeapTupleIsValid(tuple)) elog(ERROR, "%s: cache lookup failed for type %u",__func__, type);

	typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;

	ReleaseSysCache(tuple);

	elog(DEBUG1,"%s: type > %u",__func__,type);

	switch (type) {

	case TEXTOID:
	case VARCHAROID:
		result =  DatumGetCString(OidFunctionCall1(typoutput, datum));
		break;
	default:
		return NULL;
	}

	return result;

}

static void deparseExpr(Expr *expr, OAIFdwTableOptions *opts){

	OpExpr *oper;
	Var *var;
	HeapTuple tuple;
	char *operName;
	char* oaiNode;

	elog(DEBUG2,"%s called for expr->type %u",__func__,expr->type);

	switch (expr->type)	{

	case T_OpExpr:

		elog(DEBUG2,"  %s: case T_OpExpr",__func__);
		oper = (OpExpr *)expr;

		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper->opno));

		if (!HeapTupleIsValid(tuple)) elog(ERROR, "%s: cache lookup failed for operator %u",__func__, oper->opno);

		operName = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);

		ReleaseSysCache(tuple);


		elog(DEBUG2,"  %s: opername > %s",__func__,operName);

		var  = (Var *) linitial(oper->args);
		oaiNode = GetOAINodeFromColumn(opts->foreigntableid,var->varattno);

		if (strcmp(operName, "=") == 0){

			if (strcmp(oaiNode,OAI_NODE_IDENTIFIER) ==0 && (var->vartype == TEXTOID || var->vartype == VARCHAROID)) {

				Const *constant = (Const*) lsecond(oper->args);

				opts->requestType = OAI_REQUEST_GETRECORD;
				opts->identifier = datumToString(constant->constvalue,constant->consttype);

				elog(DEBUG2,"  %s: request type set to '%s' with identifier '%s'",__func__,OAI_REQUEST_GETRECORD,opts->identifier);

			}


			if (strcmp(oaiNode,OAI_NODE_DATESTAMP) ==0 && (var->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->from = deparseTimestamp(constant->constvalue);
				opts->until = deparseTimestamp(constant->constvalue);

			}

			if (strcmp(oaiNode,OAI_NODE_METADATAPREFIX) ==0 && (var->vartype == TEXTOID || var->vartype == VARCHAROID)) {

				Const *constant = (Const*) lsecond(oper->args);

				opts->metadataPrefix = datumToString(constant->constvalue,constant->consttype);

				elog(DEBUG2,"  %s: metadataPrefix set to '%s'",__func__,opts->metadataPrefix);

			}


		}

		if (strcmp(operName, ">=") == 0 || strcmp(operName, ">") == 0){

			if (strcmp(oaiNode,OAI_NODE_DATESTAMP) ==0 && (var->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->from = deparseTimestamp(constant->constvalue);

			}

		}

		if (strcmp(operName, "<=") == 0 || strcmp(operName, "<") == 0){

			if (strcmp(oaiNode,OAI_NODE_DATESTAMP) ==0 && (var->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->until = deparseTimestamp(constant->constvalue);

			}


		}

		if (strcmp(operName, "<@") == 0 || strcmp(operName, "@>") == 0 || strcmp(operName, "&&") == 0 ){

			if (strcmp(oaiNode,OAI_NODE_SETSPEC) ==0 && (var->vartype == TEXTARRAYOID || var->vartype == VARCHARARRAYOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				ArrayType *array = (ArrayType*) constant->constvalue;

				int numitems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

				if(numitems > 1) {

					elog(WARNING,"The OAI standard requests do not support multiple '%s' attributes. This filter will be applied AFTER the OAI request.",OAI_NODE_SETSPEC);
					elog(DEBUG2,"  %s: clearing '%s' attribute.",__func__,OAI_NODE_SETSPEC);
					opts->set = NULL;

				} else if (numitems == 1) {


					bool isnull;
					Datum value;

					ArrayIterator iterator = array_create_iterator(array,0,NULL);


					while (array_iterate(iterator, &value, &isnull)) {

						elog(DEBUG2,"  %s: setSpec set to '%s'",__func__,datumToString(value,TEXTOID));
						opts->set = datumToString(value,TEXTOID);

					}

					array_free_iterator(iterator);

				}

			}

		}

		break;

	default:

		break;

	}

}

static void deparseSelectColumns(OAIFdwTableOptions *opts, List *exprs){

	ListCell *cell;

	elog(DEBUG2,"%s called",__func__);

	foreach(cell, exprs) {

		Expr *expr = (Expr *) lfirst(cell);

		elog(DEBUG2,"  %s: evaluating expr->type = %u",__func__,expr->type);

		deparseExpr(expr, opts);

	}

}

static char *deparseTimestamp(Datum datum) {

	struct pg_tm datetime_tm;
	fsec_t datetime_fsec;
	StringInfoData s;

	(void)timestamp2tm(DatumGetTimestampTz(datum),
				NULL,
				&datetime_tm,
				&datetime_fsec,
				NULL,
				NULL);

	initStringInfo(&s);
	appendStringInfo(&s, "%04d-%02d-%02dT%02d:%02d:%02dZ",
		datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
		datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
		datetime_tm.tm_min, datetime_tm.tm_sec);

	return s.data;

}

static void deparseWhereClause(List *conditions, OAIFdwTableOptions *opts){

	ListCell *cell;

	foreach(cell, conditions) {

		Expr  *expr = (Expr *) lfirst(cell);

		/* extract WHERE clause from RestrictInfo */
		if (IsA(expr, RestrictInfo)) {

			RestrictInfo *ri = (RestrictInfo *) expr;
			expr = ri->clause;

		}

		deparseExpr(expr,opts);

	}


}

static void OAIFdwGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	ForeignTable *ft = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(ft->serverid);
	OAIFdwTableOptions *opts = (OAIFdwTableOptions *)palloc0(sizeof(OAIFdwTableOptions));
	ListCell *cell;

	elog(DEBUG1,"%s called",__func__);

	foreach(cell, server->options) {

		DefElem *def = lfirst_node(DefElem, cell);

		if (strcmp(OAI_NODE_URL, def->defname) == 0) {

			opts->url = defGetString(def);

		} else if (strcmp(OAI_NODE_METADATAPREFIX, def->defname) == 0) {

			opts->metadataPrefix = defGetString(def);

		} else if (strcmp(OAI_NODE_HTTP_PROXY, def->defname) == 0) {

			opts->proxy = defGetString(def);
			opts->proxyType = OAI_NODE_HTTP_PROXY;

		} else if (strcmp(OAI_NODE_HTTPS_PROXY, def->defname) == 0) {

			opts->proxy = defGetString(def);
			opts->proxyType = OAI_NODE_HTTPS_PROXY;

		} else if (strcmp(OAI_NODE_PROXY_USER, def->defname) == 0) {

			opts->proxyUser = defGetString(def);

		} else if (strcmp(OAI_NODE_PROXY_USER_PASSWORD, def->defname) == 0) {

			opts->proxyUserPassword = defGetString(def);

		} else if (strcmp(OAI_NODE_CONNECTTIMEOUT, def->defname) == 0) {

			char *tailpt;
			char *timeout_str =  defGetString(def);

			opts->connectTimeout = strtol(timeout_str, &tailpt, 0);

			//elog(DEBUG1,"  %s parsing node '%s': %ld",__func__,def->defname,opts->connectTimeout);

		} else if (strcmp(OAI_NODE_CONNECTRETRY, def->defname) == 0) {

			char *tailpt;
			char *maxretry_str =  defGetString(def);
			//elog(DEBUG2,"  %s parsing node '%s': %d",__func__,def->defname,opts->maxretries);
			opts->maxretries = strtol(maxretry_str, &tailpt, 0);

		} else {

			elog(WARNING,"Invalid SERVER OPTION > '%s'",def->defname);
		}

	}

	foreach(cell, ft->options) {

		DefElem *def = lfirst_node(DefElem, cell);

		if (strcmp(OAI_NODE_METADATAPREFIX, def->defname) == 0) {

			opts->metadataPrefix = defGetString(def);

		} else if (strcmp(OAI_NODE_SETSPEC, def->defname) == 0) {

			opts->set = defGetString(def);

		} else if (strcmp(OAI_NODE_FROM, def->defname) == 0) {

			opts->from= defGetString(def);

		} else if (strcmp(OAI_NODE_UNTIL, def->defname) == 0) {

			opts->until= defGetString(def);

		}

	}

	OAIRequestPlanner(opts, ft, baserel);
	baserel->fdw_private = opts;

}

static void OAIFdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

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

static ForeignScan *OAIFdwGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan) {

	List *fdw_private;
	OAIFdwTableOptions *opts = baserel->fdw_private;
	OAIFdwState *state = (OAIFdwState *) palloc0(sizeof(OAIFdwState));

	state->url = opts->url;
	state->proxy = opts->proxy;
	state->proxyType = opts->proxyType;
	state->proxyUser = opts->proxyUser;
	state->proxyUserPassword = opts->proxyUserPassword;
	state->connectTimeout = opts->connectTimeout;
	state->maxretries = opts->maxretries;
	state->set = opts->set;
	state->from = opts->from;
	state->until = opts->until;
	state->metadataPrefix = opts->metadataPrefix;
	state->foreigntableid = opts->foreigntableid;
	state->numcols = opts->numcols;
	state->requestType = opts->requestType;
	state->identifier = opts->identifier;
	state->numfdwcols = opts->numfdwcols;

	fdw_private = list_make1(state);

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
			scan_clauses,
			baserel->relid,
			NIL, 		 /* no expressions we will evaluate */
			fdw_private, /* pass along our start and end */
			NIL, 		 /* no custom tlist; our scan tuple looks like tlist */
			NIL, 		 /* no quals we will recheck */
			outer_plan);

}

static void OAIFdwBeginForeignScan(ForeignScanState *node, int eflags) {

	ForeignScan *fs = (ForeignScan *) node->ss.ps.plan;
	OAIFdwState *state = (OAIFdwState *) linitial(fs->fdw_private);

	if(eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

	state->current_row = 0;

	node->fdw_state = state;

	xmlInitParser();

}

static OAIRecord *FetchNextOAIRecord(OAIFdwState *state) {

	xmlNodePtr oaipmh;
	xmlNodePtr headerElements;
	xmlNodePtr ListRecordsRequest;
	xmlNodePtr record;
	xmlNodePtr xmlroot = xmlDocGetRootElement(state->xmldoc);
	bool getNext = false;
	char *buffer_identifier = NULL;

	OAIRecord *oai = (OAIRecord*) palloc(sizeof(OAIRecord));

	if(state->xmldoc == NULL ) return NULL;

	LIBXML_TEST_VERSION;

	if(state->current_identifier != NULL) {

		buffer_identifier = palloc0(sizeof(char)*strlen(state->current_identifier)+1);
		snprintf(buffer_identifier,strlen(state->current_identifier)+1,"%s",state->current_identifier);

	}

	elog(DEBUG2,"%s (%s) called: current_identifier > '%s'",__func__,state->requestType,state->current_identifier);


	if(strcmp(state->requestType,OAI_REQUEST_LISTIDENTIFIERS)==0) {

		bool ret = false;

		for (oaipmh = xmlroot->children; oaipmh!= NULL; oaipmh = oaipmh->next) {

			oai->metadataPrefix = state->metadataPrefix;
			oai->isDeleted = false;

			if (xmlStrcmp(oaipmh->name, (xmlChar*)state->requestType)!=0) continue;

			for (ListRecordsRequest = oaipmh->children; ListRecordsRequest != NULL; ListRecordsRequest = ListRecordsRequest->next) {

				if(ListRecordsRequest->next) {

					if (xmlStrcmp(ListRecordsRequest->next->name, (xmlChar*)OAI_NODE_RESUMPTIONTOKEN)==0) {

						xmlBufferPtr bufferToken = xmlBufferCreate();
						size_t content_size = (size_t) xmlNodeDump(bufferToken, state->xmldoc, ListRecordsRequest->next->children, 0, 0);

						if(strlen((char*)bufferToken->content) != 0) {

							state->resumptionToken = (char*) palloc0(sizeof(char)*content_size+1);
							snprintf(state->resumptionToken,content_size+1,"%s",(char*)bufferToken->content);

							elog(DEBUG2,"  %s: (%s): Token detected in current page > %s",__func__,state->requestType,(char*)bufferToken->content);

						}

						xmlBufferFree(bufferToken);
					}

				}


				if (xmlStrcmp(ListRecordsRequest->name, (xmlChar*) "header") == 0) {

					oai->setsArray = NULL;

					if( xmlGetProp(ListRecordsRequest, (xmlChar*) OAI_NODE_STATUS)){

						char *status = (char*) xmlGetProp(ListRecordsRequest, (xmlChar*) OAI_NODE_STATUS);

						if (strcmp(status,"deleted")) oai->isDeleted = true;

					}

					for (headerElements = ListRecordsRequest->children; headerElements!= NULL; headerElements = headerElements->next) {

						xmlBufferPtr buffer = xmlBufferCreate();
						size_t content_size = (size_t) xmlNodeDump(buffer, state->xmldoc, headerElements->children, 0, 0);

						if (xmlStrcmp(headerElements->name, (xmlChar*) "identifier")==0) {

							elog(DEBUG2,"  %s (%s): setting identifier to OAI object > '%s'",__func__,state->requestType,(char*)buffer->content);
							oai->identifier = (char*) palloc0(sizeof(char)*content_size+1);
							snprintf(oai->identifier,content_size+1,"%s",(char*)buffer->content);

						} else if (xmlStrcmp(headerElements->name, (xmlChar*) "setSpec")==0) {

							char *array_element = (char*) palloc0(sizeof(char)*content_size+1);
							elog(DEBUG2,"  %s (%s): setting setspec to OAI object > '%s'",__func__,state->requestType,(char*)buffer->content);
							snprintf(array_element,content_size+1,"%s",(char*)buffer->content);

							appendTextArray(&oai->setsArray,array_element);

							elog(DEBUG2,"  %s (%s): array appended",__func__,state->requestType);

						} else if (xmlStrcmp(headerElements->name, (xmlChar*) "datestamp")==0)	{

							elog(DEBUG2,"  %s (%s): setting datestamp to OAI object > '%s'",__func__,state->requestType,(char*)buffer->content);
							oai->datestamp = (char*) palloc0(sizeof(char)*content_size+1);
							snprintf(oai->datestamp,content_size+1,"%s",(char*)buffer->content);

						}

						elog(DEBUG2,"  %s (%s): freeing header buffer.",__func__,state->requestType);
						xmlBufferFree(buffer);

					}

				}



				elog(DEBUG2,"  %s (%s): starting evaluation > [buffer_identifier = '%s' | oai->identifier = '%s']",__func__,state->requestType,buffer_identifier,oai->identifier);

				if(buffer_identifier == NULL) {

					state->current_identifier = palloc0(sizeof(char)*strlen(oai->identifier)+1);
					snprintf(state->current_identifier,strlen(oai->identifier)+1,"%s",oai->identifier);
					elog(DEBUG2,"  %s (%s): setting first current identifier > %s",__func__,state->requestType,state->current_identifier );

					ret = true;

				} else if (strcmp(buffer_identifier,oai->identifier)==0){

					elog(DEBUG2,"  %s (%s): ##########  match > %s",__func__,state->requestType,oai->identifier);
					getNext = true;

				} else if(getNext == true) {

					state->current_identifier = palloc0(sizeof(char)*strlen(oai->identifier)+1);
					snprintf(state->current_identifier,strlen(oai->identifier)+1,"%s",oai->identifier);
					elog(DEBUG2,"  %s: (%s) setting new current identifier > %s",__func__,state->requestType,state->current_identifier);

					ret=true;

				} else if(!ListRecordsRequest->next && !state->resumptionToken) {

					ret = true;
					oai = NULL;

				}

				if(ret) {

					elog(DEBUG2,"%s: (%s) => returning %s",__func__,state->requestType,oai->identifier);
					return oai;
				}


			}

		}


	}



	/* Fetches next record from OAI ListRecord or GetRecord requests. */
	if(strcmp(state->requestType,OAI_REQUEST_LISTRECORDS)==0 || strcmp(state->requestType,OAI_REQUEST_GETRECORD)==0) {

		bool ret = false;

		for (oaipmh = xmlroot->children; oaipmh!= NULL; oaipmh = oaipmh->next) {

			oai->metadataPrefix = (char*)palloc0(sizeof(char)*strlen(state->metadataPrefix)+1);
			snprintf(oai->metadataPrefix,strlen(state->metadataPrefix)+1,"%s",state->metadataPrefix);
			oai->isDeleted = false;

			if (xmlStrcmp(oaipmh->name, (xmlChar*)state->requestType)!=0) continue;

			for (ListRecordsRequest = oaipmh->children; ListRecordsRequest != NULL; ListRecordsRequest = ListRecordsRequest->next) {

				if(ListRecordsRequest->next) {

					if (xmlStrcmp(ListRecordsRequest->next->name, (xmlChar*)OAI_NODE_RESUMPTIONTOKEN)==0) {

						xmlBufferPtr bufferToken = xmlBufferCreate();
						size_t content_size = (size_t) xmlNodeDump(bufferToken, state->xmldoc, ListRecordsRequest->next->children, 0, 0);

						if(strlen((char*)bufferToken->content) != 0) {

							state->resumptionToken = (char*) palloc0(sizeof(char)*content_size+1);
							snprintf(state->resumptionToken,content_size+1,"%s",(char*)bufferToken->content);

							elog(DEBUG2,"  %s (%s): Token detected in current page > %s",__func__,state->requestType,(char*)bufferToken->content);

						}

						xmlBufferFree(bufferToken);
					}

				}

				if (xmlStrcmp(ListRecordsRequest->name, (xmlChar*) "record") == 0) {

					oai->setsArray = NULL;

					for (record = ListRecordsRequest->children; record != NULL; record = record->next) {

						if (xmlStrcmp(record->name, (xmlChar*) "metadata") == 0) {

							/*copy necessary to get include the namespaces in the buffer output*/
							xmlNodePtr copy = xmlCopyNode(record->children, 1);

							xmlBufferPtr buffer = xmlBufferCreate();
							size_t content_size = (size_t) xmlNodeDump(buffer, state->xmldoc, copy, 0, 1);

							elog(DEBUG2,"  %s (%s): XML Buffer size: %ld",__func__,state->requestType,content_size);

							oai->content = (char*) palloc0(sizeof(char)*content_size+1);
							snprintf(oai->content,content_size+1,"%s",(char*)buffer->content);


							elog(DEBUG2,"  %s (%s): freeing node copy.",__func__,state->requestType);
							xmlFreeNode(copy);
							elog(DEBUG2,"  %s (%s): freeing xml content buffer.",__func__,state->requestType);
							xmlBufferFree(buffer);

						}


						if (xmlStrcmp(record->name, (xmlChar*) "header") == 0) {

							if( xmlGetProp(record, (xmlChar*) OAI_NODE_STATUS)){

								char *status = (char*) xmlGetProp(record, (xmlChar*) OAI_NODE_STATUS);

								if (strcmp(status,"deleted")) oai->isDeleted = true;

							}

							for (headerElements = record->children; headerElements!= NULL; headerElements = headerElements->next) {

								xmlBufferPtr buffer = xmlBufferCreate();
								size_t content_size = (size_t) xmlNodeDump(buffer, state->xmldoc, headerElements->children, 0, 0);

								if (xmlStrcmp(headerElements->name, (xmlChar*) "identifier")==0) {

									oai->identifier = (char*) palloc0(sizeof(char)*content_size+1);
									snprintf(oai->identifier,content_size+1,"%s",(char*)buffer->content);

									elog(DEBUG2,"  %s (%s): setting identifier to OAI object > '%s'",__func__,state->requestType,oai->identifier);

								} else if (xmlStrcmp(headerElements->name, (xmlChar*) "setSpec")==0) {

									char *array_element = (char*) palloc0(sizeof(char)*content_size+1);
									elog(DEBUG2,"  %s (%s): setting setspec to OAI object > '%s'",__func__,state->requestType,(char*)buffer->content);
									snprintf(array_element,content_size+1,"%s",(char*)buffer->content);

									appendTextArray(&oai->setsArray,array_element);

								} else if (xmlStrcmp(headerElements->name, (xmlChar*) "datestamp")==0)	{

									oai->datestamp = (char*) palloc0(sizeof(char)*content_size+1);
									snprintf(oai->datestamp,content_size+1,"%s",(char*)buffer->content);

									elog(DEBUG2,"  %s (%s): setting datestamp to OAI object > '%s'",__func__,state->requestType,oai->datestamp);

								}

								elog(DEBUG3,"  %s (%s): freeing header buffer.",__func__,state->requestType);
								xmlBufferFree(buffer);

							}

						}

					}

				}


				elog(DEBUG2,"  %s (%s): starting evaluation > [buffer_identifier = '%s' | oai->identifier = '%s']",__func__,state->requestType,buffer_identifier,oai->identifier);

				if(buffer_identifier == NULL) {

					state->current_identifier = palloc0(sizeof(char)*strlen(oai->identifier)+1);
					snprintf(state->current_identifier,strlen(oai->identifier)+1,"%s",oai->identifier);
					elog(DEBUG2,"  %s (%s): setting first current identifier > %s",__func__,state->requestType,state->current_identifier );

					ret = true;

				} else if (strcmp(buffer_identifier,oai->identifier)==0){

					elog(DEBUG2,"  %s (%s): match (next record will be returned) > %s",__func__,state->requestType,oai->identifier);
					getNext = true;


				} else if(getNext == true) {

					state->current_identifier = palloc0(sizeof(char)*strlen(oai->identifier)+1);
					snprintf(state->current_identifier,strlen(oai->identifier)+1,"%s",oai->identifier);
					elog(DEBUG2,"  %s: (%s) setting new current identifier > %s",__func__,state->requestType,state->current_identifier);

					ret=true;

				} else if(!ListRecordsRequest->next && !state->resumptionToken) {

					ret = true;
					oai = NULL;

				}

				if(ret) {

					elog(DEBUG2,"%s (%s): => returning %s",__func__,state->requestType,oai->identifier);
					return oai;
				}


			}

		}


	}

	return NULL;

}

static void CreateOAITuple(TupleTableSlot *slot, OAIFdwState *state, OAIRecord *oai) {

	elog(DEBUG2,"%s called",__func__);

	for (int i = 0; i < state->numcols; i++) {

		List * options = GetForeignColumnOptions(state->foreigntableid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			DefElem *def = (DefElem*) lfirst(lc);

			if (strcmp(def->defname, OAI_NODE_OPTION)==0) {

				char *option_value = defGetString(def);

				if (strcmp(option_value,OAI_NODE_STATUS)==0){

					elog(DEBUG2,"  %s: setting column %d option '%s'",__func__,i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = oai->isDeleted;


				} else if (strcmp(option_value,OAI_NODE_IDENTIFIER)==0 && oai->identifier){

					elog(DEBUG2,"  %s: setting column %d option '%s'",__func__,i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->identifier);


				} else if (strcmp(option_value,OAI_NODE_METADATAPREFIX)==0 && oai->metadataPrefix){

					elog(DEBUG2,"  %s: setting column %d option '%s'",__func__,i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->metadataPrefix);


				} else if (strcmp(option_value,OAI_NODE_CONTENT)==0 && oai->content){

					elog(DEBUG2,"  %s: setting column %d option '%s'",__func__,i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum((char*)oai->content);

				} else if (strcmp(option_value,OAI_NODE_SETSPEC)==0 && oai->setsArray){

					elog(DEBUG2,"  %s: setting column %d option '%s'",__func__,i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = (Datum) DatumGetArrayTypeP(oai->setsArray);

				}  else if (strcmp(option_value,OAI_NODE_DATESTAMP)==0 && oai->datestamp) {

					char lowstr[MAXDATELEN + 1];
					char *field[MAXDATEFIELDS];
					int ftype[MAXDATEFIELDS];
					int nf;
					int parseError = ParseDateTime(oai->datestamp, lowstr, MAXDATELEN, field, ftype, MAXDATEFIELDS, &nf);

					elog(DEBUG2,"  %s: setting column %d option '%s'",__func__,i,option_value);


					if (parseError == 0) {

						int dtype;
						struct pg_tm date;
						fsec_t fsec = 0;
						int tz = 0;

						int decodeError = DecodeDateTime(field, ftype, nf, &dtype, &date, &fsec, &tz);

						Timestamp tmsp;
						tm2timestamp(&date, fsec, &tz, &tmsp);

						if (decodeError == 0) {

							slot->tts_isnull[i] = false;
							slot->tts_values[i] = (Datum) TimestampTzGetDatum(tmsp);

							elog(DEBUG2,"  %s: datestamp (\"%s\") parsed and decoded.",__func__,oai->datestamp);


						} else {

							slot->tts_isnull[i] = true;
							slot->tts_values[i] = PointerGetDatum(NULL);;
							elog(WARNING,"  %s: could not decode datestamp: %s",__func__, oai->datestamp);

						}


					} else {


						slot->tts_isnull[i] = true;
						slot->tts_values[i] = PointerGetDatum(NULL);;

						elog(WARNING,"  %s: could not parse datestamp: %s",__func__, oai->datestamp);

					}

				} else {

					slot->tts_isnull[i] = true;
					slot->tts_values[i] = PointerGetDatum(NULL);
				}

			}

		}

	}


	state->rowcount++;
	elog(DEBUG2,"%s => finished (rowcount: %d)",__func__,state->rowcount);

}

static TupleTableSlot *OAIFdwIterateForeignScan(ForeignScanState *node) {

	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	OAIFdwState *state = node->fdw_state;
	OAIRecord *record = (OAIRecord *)palloc(sizeof(OAIRecord));

	elog(DEBUG2,"%s called.",__func__);

	ExecClearTuple(slot);

	/* Returns empty tuple in case there is no mapping for OAI nodes and columns*/
	if(state->numfdwcols == 0) return slot;

	if(state->rowcount == 0 || state->resumptionToken) {

		elog(DEBUG2,"  %s: loading OAI records (token: \"%s\")",__func__,state->resumptionToken);

		if(state->xmldoc != NULL) {

			elog(DEBUG2,"  %s: clearing xml document.",__func__);
			state->xmldoc = NULL;

		}

		state->current_identifier = NULL;
		LoadOAIRecords(state);

		elog(DEBUG1,"%s: records retrieved %d/%s (rowcount/completeListSize)",__func__,state->rowcount,state->completeListSize);

	}

	record = FetchNextOAIRecord(state);

	if(record) {

		CreateOAITuple(slot, state, record);
		elog(DEBUG2,"  %s: OAI tuple created",__func__);

		ExecStoreVirtualTuple(slot);
		elog(DEBUG2,"  %s: virtual tuple stored",__func__);

	} else {

		elog(DEBUG1,"%s: records retrieved %d/%s (rowcount/completeListSize)",__func__,state->rowcount,state->completeListSize);

		xmlFreeDoc(state->xmldoc);
		xmlCleanupParser();
		elog(DEBUG2,"  %s: (EOF) xmldoc and xml parser cleared",__func__);

	}

	elog(DEBUG2,"%s => returning tuple (rowcount: %d)",__func__,state->rowcount);

	return slot;

}

static void LoadOAIRecords(OAIFdwState *state) {

	xmlNodePtr recordsList;
	xmlNodePtr record;
	xmlNodePtr xmlroot;

	int oaiExecuteResponse;

	elog(DEBUG2,"%s called.",__func__);

	oaiExecuteResponse = ExecuteOAIRequest(state);

	elog(DEBUG2,"  %s: ExecuteOAIRequest response > %d",__func__,oaiExecuteResponse);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlroot = xmlDocGetRootElement(state->xmldoc);

		for (recordsList = xmlroot->children; recordsList != NULL; recordsList = recordsList->next) {

			elog(DEBUG2,"  %s: ExecuteOAIRequest parsing xmlroot's children",__func__);

			if (recordsList->type != XML_ELEMENT_NODE) continue;

			elog(DEBUG3,"  %s: XML root parsed and it has valid children (XML_ELEMENT_NODE) -> '%s'",__func__,recordsList->name);

			if (xmlStrcmp(recordsList->name, (xmlChar*)state->requestType)==0) {

				for (record = recordsList->children; record!= NULL; record = record->next) {

					if (record->type != XML_ELEMENT_NODE) continue;

					if (xmlStrcmp(record->name, (xmlChar*)"resumptionToken")==0){

						if(xmlGetProp(record, (xmlChar*) "completeListSize")) {

							state->completeListSize = (char*) xmlGetProp(record, (xmlChar*) "completeListSize");

						}

					}

				}

			} else if (xmlStrcmp(recordsList->name, (xmlChar*)"error")==0) {

				char *errorCode = (char*) xmlGetProp(recordsList, (xmlChar*) "code");

				state->xmldoc = NULL;

				if(strcmp(errorCode,OAI_ERROR_ID_DOES_NOT_EXIST)==0 || strcmp(errorCode,OAI_ERROR_NO_RECORD_MATCH)==0) {

					ereport(WARNING,
							(errcode(ERRCODE_NO_DATA_FOUND),
									errmsg("OAI %s: %s",errorCode,(char*)xmlNodeGetContent(recordsList))));

				} else {

					xmlFreeDoc(state->xmldoc);
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
									errmsg("OAI %s: %s",errorCode,(char*)xmlNodeGetContent(recordsList))));

				}


			}

		}

	}

	elog(DEBUG1,"%s => finished: current_row > %d",__func__,state->current_row);

}

static void appendTextArray(ArrayType **array, char* text_element) {

	/* Array build variables */
	size_t arr_nelems = 0;
	size_t arr_elems_size = 1;
	Oid elem_type = TEXTOID;
	int16 elem_len;
	bool elem_byval;
	char elem_align;
	Datum *arr_elems = palloc0(arr_elems_size*sizeof(Datum));

	elog(DEBUG2,"  %s called with element > %s",__func__,text_element);

	get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

	if(*array == NULL) {

		/*Array has no elements */
		arr_elems[arr_nelems] = CStringGetTextDatum(text_element);
		elog(DEBUG3,"    %s: array empty! adding value at arr_elems[%ld]",__func__,arr_nelems);
		arr_nelems++;

	} else {

		bool isnull;
		Datum value;
		ArrayIterator iterator = array_create_iterator(*array,0,NULL);
		elog(DEBUG3,"    %s: current array size: %d",__func__,ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array)));

		arr_elems_size *= ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array))+1;
		arr_elems = repalloc(arr_elems, arr_elems_size*sizeof(Datum));

		while (array_iterate(iterator, &value, &isnull)) {

			if (isnull) continue;

			elog(DEBUG3,"    %s: re-adding element: arr_elems[%ld]. arr_elems_size > %ld",__func__,arr_nelems,arr_elems_size);

			arr_elems[arr_nelems] = value;
			arr_nelems++;

		}
		array_free_iterator(iterator);

		elog(DEBUG3,"   %s: adding new element: arr_elems[%ld]. arr_elems_size > %ld",__func__,arr_nelems,arr_elems_size);
		arr_elems[arr_nelems++] = CStringGetTextDatum(text_element);

	}

	elog(DEBUG2,"  %s => construct_array called: arr_nelems > %ld arr_elems_size %ld",__func__,arr_nelems,arr_elems_size);

	*array = construct_array(arr_elems, arr_nelems, elem_type, elem_len, elem_byval, elem_align);

}

static void OAIFdwReScanForeignScan(ForeignScanState *node) {

	OAIFdwState *state = node->fdw_state;
	state->current_row = 0;

}

static void OAIFdwEndForeignScan(ForeignScanState *node) {

}

static List* OAIFdwImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid serverOid) {

	ListCell   *cell;
	List *sql_commands = NIL;
	List *all_sets = NIL;
	char *format = "oai_dc";
	bool format_set = false;
	OAIFdwState *state ;
	ForeignServer *server = GetForeignServer(serverOid);

	elog(DEBUG1,"%s called: '%s'",__func__,server->servername);
	state = GetServerInfo(server->servername);

	all_sets = GetSets(state);

	elog(DEBUG1,"  %s: parsing statements",__func__);

	foreach(cell, stmt->options) {

		DefElem *def = lfirst_node(DefElem, cell);
		if(strcmp(def->defname,OAI_NODE_METADATAPREFIX)==0) {

			ListCell *cell_formats;
			List *formats = NIL;
			bool found = false;
			formats = GetMetadataFormats(state);

			foreach(cell_formats, formats) {

				OAIMetadataFormat *format = (OAIMetadataFormat *) lfirst(cell_formats);

				if(strcmp(format->metadataPrefix,defGetString(def))==0){
					found = true;
				}
			}

			if(!found) {

				ereport(ERROR,
					   (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
						errmsg("invalid 'metadataprefix': '%s'",defGetString(def))));

			}

			format = defGetString(def);
			format_set = true;

		} else {

			ereport(ERROR,
				   (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					errmsg("invalid FOREIGN SCHEMA OPTION: '%s'",def->defname)));

		}

	}

	if(!format_set) {

		ereport(ERROR,
			   (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
			    errmsg("missing 'metadataprefix' OPTION."),
				errhint("A OAI Foreign Table must have a fixed 'metadataprefix'. Execute 'SELECT * FROM OAI_ListMetadataFormats('%s')' to see which formats are offered in the OAI Repository.",server->servername)));

	}

	if(strcmp(stmt->remote_schema,"oai_sets") == 0) {

		List *tables = NIL;

		if(stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO) {

			ListCell *cell_limit_to;

			foreach (cell_limit_to, stmt->table_list) {

				RangeVar* rv = (RangeVar*) lfirst(cell_limit_to);
				OAISet *set = (OAISet *)palloc0(sizeof(OAISet));
				set->setSpec = rv->relname;

				tables = lappend(tables, set);

			}

		} else if(stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT) {

			ListCell *cell_sets;

			foreach (cell_sets, all_sets) {

				ListCell *cell_except;
				bool found = false;
				OAISet *set = (OAISet *)palloc0(sizeof(OAISet));
				set = (OAISet *) lfirst(cell_sets);

				foreach (cell_except, stmt->table_list) {

					RangeVar* rv = (RangeVar*) lfirst(cell_except);
					if(strcmp(rv->relname,set->setSpec)==0) {

						found = true;
						break;
					}

				}

				if(!found) tables = lappend(tables,set);

			}


		} else if(stmt->list_type == FDW_IMPORT_SCHEMA_ALL) {

			tables = all_sets;

		}

		foreach(cell, tables) {

			StringInfoData buffer;
			OAISet *set = (OAISet *) lfirst(cell);
			initStringInfo(&buffer);

			appendStringInfo(&buffer,"\nCREATE FOREIGN TABLE %s (\n",quote_identifier(set->setSpec));
			appendStringInfo(&buffer,"  id text                OPTIONS (oai_node 'identifier'),\n");
			appendStringInfo(&buffer,"  xmldoc xml             OPTIONS (oai_node 'content'),\n");
			appendStringInfo(&buffer,"  sets text[]            OPTIONS (oai_node 'setspec'),\n");
			appendStringInfo(&buffer,"  updatedate timestamp   OPTIONS (oai_node 'datestamp'),\n");
			appendStringInfo(&buffer,"  format text            OPTIONS (oai_node 'metadataprefix'),\n");
			appendStringInfo(&buffer,"  status boolean         OPTIONS (oai_node 'status')\n");
			appendStringInfo(&buffer,") SERVER %s OPTIONS (metadataPrefix '%s', setspec '%s');\n",server->servername,format,set->setSpec);

			sql_commands = lappend(sql_commands, pstrdup(buffer.data));

			elog(DEBUG1,"%s: IMPORT FOREIGN SCHEMA (%s): \n%s",__func__,stmt->remote_schema,buffer.data);

		}

		elog(NOTICE, "Foreign tables to be created in schema '%s': %d", stmt->local_schema, list_length(sql_commands));

	} else if(strcmp(stmt->remote_schema,"oai_repository") == 0){

		StringInfoData buffer;
		initStringInfo(&buffer);

		appendStringInfo(&buffer,"\nCREATE FOREIGN TABLE %s_repository (\n",server->servername);
		appendStringInfo(&buffer,"  id text                OPTIONS (oai_node 'identifier'),\n");
		appendStringInfo(&buffer,"  xmldoc xml             OPTIONS (oai_node 'content'),\n");
		appendStringInfo(&buffer,"  sets text[]            OPTIONS (oai_node 'setspec'),\n");
		appendStringInfo(&buffer,"  updatedate timestamp   OPTIONS (oai_node 'datestamp'),\n");
		appendStringInfo(&buffer,"  format text            OPTIONS (oai_node 'metadataprefix'),\n");
		appendStringInfo(&buffer,"  status boolean         OPTIONS (oai_node 'status')\n");
		appendStringInfo(&buffer,") SERVER %s OPTIONS (metadataPrefix '%s');\n",server->servername,format);

		sql_commands = lappend(sql_commands, pstrdup(buffer.data));

		elog(DEBUG1,"%s: IMPORT FOREIGN SCHEMA: \n%s",__func__,buffer.data);

	} else {

		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_NAME),
				 errmsg("invalid FOREIGN SCHEMA: '%s'",stmt->remote_schema)));

	}

	return sql_commands;

}
