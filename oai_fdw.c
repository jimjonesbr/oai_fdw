/**********************************************************************
 *
 * oai_fdw -  PostgreSQL Foreign Data Wrapper for OAI-PMH Repositories
 *
 * oai_fdw is free software: you can redistribute it and/or modify
 * it under the terms of the MIT Licence.
 *
 * Copyright (C) 2021-2026 University of Münster, Germany
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
#include "miscadmin.h"

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

#define OAI_FDW_VERSION "1.12"
#define OAI_REQUEST_LISTRECORDS "ListRecords"
#define OAI_REQUEST_LISTIDENTIFIERS "ListIdentifiers"
#define OAI_REQUEST_IDENTIFY "Identify"
#define OAI_REQUEST_LISTSETS "ListSets"
#define OAI_REQUEST_GETRECORD "GetRecord"
#define OAI_REQUEST_LISTMETADATAFORMATS "ListMetadataFormats"
#define OAI_REQUEST_LISTSETS "ListSets"
#define OAI_REQUEST_CONNECTTIMEOUT 300
#define OAI_REQUEST_MAXRETRY 3

#define OAI_USERMAPPING_OPTION_USER "user"
#define OAI_USERMAPPING_OPTION_PASSWORD "password"

#define OAI_RESPONSE_ELEMENT_RECORD "record"
#define OAI_RESPONSE_ELEMENT_METADATA "metadata"
#define OAI_RESPONSE_ELEMENT_HEADER "header"
#define OAI_RESPONSE_ELEMENT_IDENTIFIER "identifier"
#define OAI_RESPONSE_ELEMENT_DATESTAMP "datestamp"
#define OAI_RESPONSE_ELEMENT_SETSPEC "setSpec"
#define OAI_RESPONSE_ELEMENT_SETNAME "setName"
#define OAI_RESPONSE_ELEMENT_METADATANAMESPACE "metadataNamespace"
#define OAI_RESPONSE_ELEMENT_METADATAPREFIX "metadataPrefix"
#define OAI_RESPONSE_ELEMENT_METADATAFORMAT "metadataFormat"
#define OAI_RESPONSE_ELEMENT_SCHEMA "schema"

#define OAI_RESPONSE_ELEMENT_DELETED "deleted"
#define OAI_RESPONSE_ELEMENT_RESUMPTIONTOKEN "resumptionToken"
#define OAI_RESPONSE_ELEMENT_COMPLETELISTSIZE "completeListSize"

#define OAI_XML_ROOT_ELEMENT "OAI-PMH"
#define OAI_NODE_IDENTIFIER "identifier"
#define OAI_NODE_URL "url"
#define OAI_NODE_HTTP_PROXY "http_proxy"
#define OAI_NODE_HTTPS_PROXY "https_proxy"
#define OAI_NODE_PROXY_USER "proxy_user"
#define OAI_NODE_PROXY_USER_PASSWORD "proxy_user_password"
#define OAI_NODE_CONNECTTIMEOUT "connect_timeout"
#define OAI_NODE_CONNECTRETRY "connect_retry"
#define OAI_NODE_REQUEST_REDIRECT "request_redirect"
#define OAI_NODE_REQUEST_MAX_REDIRECT "request_max_redirect"
#define OAI_NODE_CONTENT "content"
#define OAI_NODE_DATESTAMP "datestamp"
#define OAI_NODE_SETSPEC "setspec"
#define OAI_NODE_METADATAPREFIX "metadataprefix"
#define OAI_NODE_FROM "from"
#define OAI_NODE_UNTIL "until"
#define OAI_NODE_STATUS "status"
#define OAI_NODE_OPTION "oai_node"
#define OAI_ERROR_ID_DOES_NOT_EXIST "idDoesNotExist"
#define OAI_ERROR_NO_RECORD_MATCH "noRecordsMatch"

#define OAI_SUCCESS 0
#define OAI_FAIL 1
#define OAI_MALFORMED_URL 3
#define OAI_UNKNOWN_REQUEST 2
#define ERROR_CODE_MISSING_PREFIX 1

PG_MODULE_MAGIC;

typedef struct OAIFdwState
{
	int numcols;			 /* Total number of columns in a foreign table. */
	int numfdwcols;			 /* Number of oai_fdw columns in a foregign table. */
	int rowcount;			 /* Total number of OAI records retrieved. */
	bool requestRedirect;	 /* Enables or disables URL redirecting. */
	long requestMaxRedirect; /* Limit of how many times the URL redirection (jump) may occur. */
	long maxretries;		 /* Max number of retries in case a request returns an error message. */
	long connectTimeout;	 /* Connection timeout for OAI requests in seconds. */
	char *identifier;		 /* The unique identifier of an item in a repository. */
	char *set;				 /* The set membership of the item for the purpose of selective harvesting. */
	char *url;				 /* Concatenated URL with the OAI request. */
	char *metadataPrefix;	 /* Metadata format in OAI requests issued to the repository. */
	char *proxy;			 /* Proxy for HTTP requests, if necessary. */
	char *proxyType;		 /* Proxy protocol (HTTPS, HTTP). */
	char *proxyUser;		 /* User name for proxy authentication. */
	char *proxyUserPassword; /* Password for proxy authentication. */
	char *from;				 /* Beginning of am interval to filter an OAI request. */
	char *until;			 /* End of an interval to filter an OAI request. */
	char *resumptionToken;	 /* Token to retrieve the next page of a result set. */
	char *requestVerb;		 /* Type of OAI request (GetRecord, ListRecords, ListIdentifiers,
								Identify, ListSets, ListMetadataFormats. */
	Oid foreigntableid;
	xmlDocPtr xmldoc;	  /* Result of an OAI request. */
	MemoryContext oaicxt; /* Memory Context for data manipulation. */
	List *records;		  /* List of OAI records retrieved. */
	int pageindex;		  /* Index of a record within a retrieved page (list). */
	int pagesize;		  /* Number of OAI records retrieved. */
	ForeignTable *foreign_table;
	ForeignServer *foreign_server;
	char *user;
	char *password;
} OAIFdwState;

typedef struct OAIRecord
{
	char *identifier;
	char *content;
	char *datestamp;
	char *metadataPrefix;
	bool isDeleted;
	ArrayType *setsArray;
} OAIRecord;

typedef struct OAIColumn
{
	int id;
	char *oaiNode;
	char *label;
	char *function;
} OAIColumn;

typedef struct OAIMetadataFormat
{
	char *metadataPrefix;
	char *schema;
	char *metadataNamespace;
} OAIMetadataFormat;

typedef struct OAISet
{
	char *setSpec;
	char *setName;
} OAISet;

typedef struct OAIFdwIdentityNode
{
	char *name;
	char *description;
} OAIFdwIdentityNode;

struct string
{
	char *ptr;
	size_t len;
};

struct OAIFdwOption
{
	const char *optname;
	Oid optcontext;	  /* Oid of catalog in which option may appear */
	bool optrequired; /* Flag mandatory options */
	bool optfound;	  /* Flag whether options was specified by user */
};

struct MemoryStruct
{
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
		{OAI_NODE_REQUEST_REDIRECT, ForeignServerRelationId, false, false},
		{OAI_NODE_REQUEST_MAX_REDIRECT, ForeignServerRelationId, false, false},

		{OAI_NODE_IDENTIFIER, ForeignTableRelationId, false, false},
		{OAI_NODE_METADATAPREFIX, ForeignTableRelationId, true, false},
		{OAI_NODE_SETSPEC, ForeignTableRelationId, false, false},
		{OAI_NODE_FROM, ForeignTableRelationId, false, false},
		{OAI_NODE_UNTIL, ForeignTableRelationId, false, false},

		{OAI_NODE_OPTION, AttributeRelationId, true, false},
		/* User Mapping */
		{OAI_USERMAPPING_OPTION_USER, UserMappingRelationId, false, false},
		{OAI_USERMAPPING_OPTION_PASSWORD, UserMappingRelationId, false, false},
		/* EOList option */
		{NULL, InvalidOid, false, false}};

#define option_count (sizeof(valid_options) / sizeof(struct OAIFdwOption))

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
static List *OAIFdwImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);

static void appendTextArray(ArrayType **array, char *text_element);
static int ExecuteOAIRequest(OAIFdwState *state);
static void CreateOAITuple(TupleTableSlot *slot, OAIFdwState *state, OAIRecord *oai);
static OAIRecord *FetchNextOAIRecord(OAIFdwState **state);
static void LoadOAIRecords(struct OAIFdwState **state);
static void deparseExpr(Expr *expr, OAIFdwState *state);
static char *datumToString(Datum datum, Oid type);
static char *GetOAINodeFromColumn(Oid foreigntableid, int16 attnum);
static void deparseWhereClause(OAIFdwState *state, List *conditions);
static void deparseSelectColumns(OAIFdwState *state, List *exprs);
static void OAIRequestPlanner(OAIFdwState *state, RelOptInfo *baserel);
static char *deparseTimestamp(Datum datum);
static int CheckURL(char *url);
static OAIFdwState *GetServerInfo(const char *srvname);
static List *GetMetadataFormats(OAIFdwState *state);
static List *GetIdentity(OAIFdwState *state);
static List *GetSets(OAIFdwState *state);
static void RaiseOAIException(xmlNodePtr error);
static Datum CreateDatum(HeapTuple tuple, int pgtype, int pgtypmod, char *value);
static void LoadOAIUserMapping(OAIFdwState *state);
static void InitSession(OAIFdwState *state, RelOptInfo *baserel);

Datum oai_fdw_handler(PG_FUNCTION_ARGS)
{
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

Datum oai_fdw_version(PG_FUNCTION_ARGS)
{

	StringInfoData buffer;
	initStringInfo(&buffer);

	appendStringInfo(&buffer, "oai fdw = %s,", OAI_FDW_VERSION);
	appendStringInfo(&buffer, " libxml = %s,", LIBXML_DOTTED_VERSION);
	appendStringInfo(&buffer, " libcurl = %s", curl_version());

	PG_RETURN_TEXT_P(cstring_to_text(buffer.data));
}

/**
 * Retrieves all information configured in the CREATE SERVER statement.
 */
OAIFdwState *GetServerInfo(const char *srvname)
{
	OAIFdwState *state = (OAIFdwState *)palloc0(sizeof(OAIFdwState));
	ForeignServer *server = GetForeignServerByName(srvname, true);

	state->requestRedirect = false;
	state->requestMaxRedirect = 0L;

	elog(DEBUG1, "%s called: '%s'", __func__, srvname);

	if (server)
	{
		ListCell *cell;

		foreach (cell, server->options)
		{
			DefElem *def = lfirst_node(DefElem, cell);

			elog(DEBUG1, "  %s parsing node '%s': %s", __func__, def->defname, defGetString(def));

			if (strcmp(def->defname, OAI_NODE_URL) == 0)
			{
				state->url = defGetString(def);
			}

			if (strcmp(def->defname, OAI_NODE_HTTP_PROXY) == 0)
			{
				state->proxy = defGetString(def);
				state->proxyType = OAI_NODE_HTTP_PROXY;
			}

			if (strcmp(def->defname, OAI_NODE_PROXY_USER) == 0)
			{
				state->proxyUser = defGetString(def);
			}

			if (strcmp(def->defname, OAI_NODE_PROXY_USER_PASSWORD) == 0)
			{
				state->proxyUserPassword = defGetString(def);
			}

			if (strcmp(def->defname, OAI_NODE_CONNECTTIMEOUT) == 0)
			{
				char *tailpt;
				char *timeout_str = defGetString(def);

				state->connectTimeout = strtol(timeout_str, &tailpt, 0);
			}

			if (strcmp(def->defname, OAI_NODE_REQUEST_REDIRECT) == 0)
			{
				state->requestRedirect = defGetBoolean(def);

				elog(DEBUG1, "  %s: setting \"%s\": %d", __func__, OAI_NODE_REQUEST_REDIRECT, state->requestRedirect);
			}

			if (strcmp(def->defname, OAI_NODE_REQUEST_MAX_REDIRECT) == 0)
			{
				char *tailpt;
				char *maxredirect_str = defGetString(def);

				state->requestMaxRedirect = strtol(maxredirect_str, &tailpt, 10);

				elog(DEBUG1, "  %s: setting \"%s\": %ld", __func__, OAI_NODE_REQUEST_MAX_REDIRECT, state->requestMaxRedirect);

				if (strcmp(defGetString(def), "0") != 0 && state->requestMaxRedirect == 0)
				{
					elog(ERROR, "invalid value for \"%s\"", OAI_NODE_REQUEST_MAX_REDIRECT);
				}
			}
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
				 errmsg("FOREIGN SERVER does not exist: '%s'", srvname)));
	}

	return state;
}

Datum oai_fdw_identity(PG_FUNCTION_ARGS)
{
	text *srvname_text = PG_GETARG_TEXT_P(0);
	const char *srvname = text_to_cstring(srvname_text);
	OAIFdwState *state = GetServerInfo(srvname);

	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	TupleDesc tupdesc;
	int call_cntr;
	int max_calls;
	MemoryContext oldcontext;
	List *identity;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		identity = GetIdentity(state);
		funcctx->user_fctx = identity;

		if (identity)
			funcctx->max_calls = identity->length;
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("function returning record called in context that cannot accept type record")));

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)
	{
		Datum values[23];
        bool nulls[23];
        HeapTuple tuple;
        Datum result;
        OAIFdwIdentityNode *identity_node = (OAIFdwIdentityNode *)list_nth((List *)funcctx->user_fctx, call_cntr);

		memset(nulls, 0, sizeof(nulls));

		for (size_t i = 0; i < funcctx->attinmeta->tupdesc->natts; i++)
        {
			Form_pg_attribute att = TupleDescAttr(funcctx->attinmeta->tupdesc, i);
            
			if(strcmp(NameStr(att->attname),"name")==0)
				values[i] = CreateDatum(tuple, att->atttypid, att->atttypmod, identity_node->name);
			else if(strcmp(NameStr(att->attname),"description")==0)
				values[i] = CreateDatum(tuple, att->atttypid, att->atttypmod, identity_node->description);
			else
				nulls[i] = true;
        }

		elog(DEBUG2, "  %s: creating heap tuple", __func__);

        tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

Datum oai_fdw_listSets(PG_FUNCTION_ARGS)
{

	text *srvname_text = PG_GETARG_TEXT_P(0);
	char *srvname = text_to_cstring(srvname_text);
	OAIFdwState *state = GetServerInfo(srvname);

	MemoryContext oldcontext;
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	TupleDesc tupdesc;
	int call_cntr;
	int max_calls;

	if (SRF_IS_FIRSTCALL())
	{
		List *sets;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		sets = GetSets(state);
		funcctx->user_fctx = sets;

		if (sets)
			funcctx->max_calls = sets->length;
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("function returning record called in context that cannot accept type record")));

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)
	{
		Datum values[23];
        bool nulls[23];
        HeapTuple tuple;
        Datum result;
		OAISet *set_node = (OAISet *)list_nth((List *)funcctx->user_fctx, call_cntr);

		memset(nulls, 0, sizeof(nulls));

		for (size_t i = 0; i < funcctx->attinmeta->tupdesc->natts; i++)
        {
			Form_pg_attribute att = TupleDescAttr(funcctx->attinmeta->tupdesc, i);
            
			if(strcmp(NameStr(att->attname),"setname")==0)
				values[i] = CreateDatum(tuple, att->atttypid, att->atttypmod, set_node->setName);
			else if(strcmp(NameStr(att->attname),"setspec")==0)
				values[i] = CreateDatum(tuple, att->atttypid, att->atttypmod, set_node->setSpec);
			else
				nulls[i] = true;
        }

		elog(DEBUG2, "  %s: creating heap tuple", __func__);

        tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
	}
	else
	{

		SRF_RETURN_DONE(funcctx);
	}
}

Datum oai_fdw_listMetadataFormats(PG_FUNCTION_ARGS)
{
	text *srvname_text = PG_GETARG_TEXT_P(0);
	const char *srvname = text_to_cstring(srvname_text);
	OAIFdwState *state = GetServerInfo(srvname);

	FuncCallContext *funcctx;
	int call_cntr;
	int max_calls;
	TupleDesc tupdesc;
	AttInMetadata *attinmeta;
	MemoryContext oldcontext;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		List *formats;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		formats = GetMetadataFormats(state);
		funcctx->user_fctx = formats;

		if (formats)
			funcctx->max_calls = formats->length;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("function returning record called in context that cannot accept type record")));

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every function call */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	/* do when there is more left to send */
	if (call_cntr < max_calls)
	{
		Datum values[23];
        bool nulls[23];
        HeapTuple tuple;
        Datum result;
		OAIMetadataFormat *format = (OAIMetadataFormat *)list_nth((List *)funcctx->user_fctx, call_cntr);
		
		memset(nulls, 0, sizeof(nulls));

		for (size_t i = 0; i < funcctx->attinmeta->tupdesc->natts; i++)
        {
			Form_pg_attribute att = TupleDescAttr(funcctx->attinmeta->tupdesc, i);
            
			if(strcmp(NameStr(att->attname),"metadataprefix")==0)
				values[i] = CreateDatum(tuple, att->atttypid, att->atttypmod, format->metadataPrefix);
			else if(strcmp(NameStr(att->attname),"schema")==0)
				values[i] = CreateDatum(tuple, att->atttypid, att->atttypmod, format->schema);
			else if(strcmp(NameStr(att->attname),"metadatanamespace")==0)
				values[i] = CreateDatum(tuple, att->atttypid, att->atttypmod, format->metadataNamespace);
			else
				nulls[i] = true;
        }

		elog(DEBUG2, "  %s: creating heap tuple", __func__);

        tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
	}
	else /* do when there is no more left */
	{
		SRF_RETURN_DONE(funcctx);
	}
}

Datum oai_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	ListCell *cell;
	struct OAIFdwOption *opt;

	/* Initialize found state to not found */
	for (opt = valid_options; opt->optname; opt++)
	{
		opt->optfound = false;
	}

	foreach (cell, options_list)
	{
		DefElem *def = (DefElem *)lfirst(cell);
		bool optfound = false;

		for (opt = valid_options; opt->optname; opt++)
		{
			if (catalog == opt->optcontext && strcmp(opt->optname, def->defname) == 0)
			{
				/* Mark that this user option was found */
				opt->optfound = optfound = true;

				if (strlen(defGetString(def)) == 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							 errmsg("empty value in option '%s'", opt->optname)));
				}

				if (strcmp(opt->optname, OAI_NODE_URL) == 0 || strcmp(opt->optname, OAI_NODE_HTTP_PROXY) == 0 || strcmp(opt->optname, OAI_NODE_HTTPS_PROXY) == 0)
				{

					int return_code = CheckURL(defGetString(def));

					if (return_code != OAI_SUCCESS)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: '%s'", opt->optname, defGetString(def))));
					}
				}

				if (strcmp(opt->optname, OAI_NODE_CONNECTTIMEOUT) == 0)
				{
					char *endptr;
					char *timeout_str = defGetString(def);
					long timeout_val = strtol(timeout_str, &endptr, 0);

					if (timeout_str[0] == '\0' || *endptr != '\0' || timeout_val < 0)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: %s", def->defname, timeout_str),
								 errhint("expected values are positive integers (timeout in seconds)")));
					}
				}

				if (strcmp(opt->optname, OAI_NODE_CONNECTRETRY) == 0)
				{
					char *endptr;
					char *retry_str = defGetString(def);
					long retry_val = strtol(retry_str, &endptr, 0);

					if (retry_str[0] == '\0' || *endptr != '\0' || retry_val < 0)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: %s", def->defname, retry_str),
								 errhint("expected values are positive integers (retry attempts in case of failure)")));
					}
				}

				if (strcmp(opt->optname, OAI_NODE_OPTION) == 0)
				{
					if (strcmp(defGetString(def), OAI_NODE_IDENTIFIER) != 0 &&
						strcmp(defGetString(def), OAI_NODE_METADATAPREFIX) != 0 &&
						strcmp(defGetString(def), OAI_NODE_SETSPEC) != 0 &&
						strcmp(defGetString(def), OAI_NODE_DATESTAMP) != 0 &&
						strcmp(defGetString(def), OAI_NODE_CONTENT) != 0 &&
						strcmp(defGetString(def), OAI_NODE_STATUS) != 0)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
								 errmsg("invalid %s option '%s'", OAI_NODE_OPTION, defGetString(def)),
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

		if (!optfound)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid oai_fdw option \"%s\"", def->defname)));
		}
	}

	for (opt = valid_options; opt->optname; opt++)
	{
		/* Required option for this catalog type is missing? */
		if (catalog == opt->optcontext && opt->optrequired && !opt->optfound)
		{
			ereport(ERROR, (
							   errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
							   errmsg("required option '%s' is missing", opt->optname)));
		}
	}

	PG_RETURN_VOID();
}

/*
 * Parses information from the OAI Identify request.
 * https://www.openarchives.org/OAI/openarchivesprotocol.html#Identify
 */
static List *GetIdentity(OAIFdwState *state)
{
	int oaiExecuteResponse;
	List *result = NIL;

	elog(DEBUG1, "%s called", __func__);

	state->requestVerb = OAI_REQUEST_IDENTIFY;

	oaiExecuteResponse = ExecuteOAIRequest(state);

	if (!state->xmldoc)
		elog(ERROR, "invalid %s response from '%s'", state->requestVerb, state->url);

	xmlInitParser();

	if (oaiExecuteResponse == OAI_SUCCESS)
	{
		xmlNodePtr oai_root;
		xmlNodePtr Identity;
		xmlNodePtr xmlroot = xmlDocGetRootElement(state->xmldoc);

		if (xmlroot == NULL)
			elog(ERROR, "invalid root element for %s response", state->requestVerb);

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next)
		{
			if (oai_root->type != XML_ELEMENT_NODE)
				continue;

			if (xmlStrcmp(oai_root->name, (xmlChar *)OAI_REQUEST_IDENTIFY) != 0)
				continue;

			for (Identity = oai_root->children; Identity != NULL; Identity = Identity->next)
			{
				OAIFdwIdentityNode *node = (OAIFdwIdentityNode *)palloc0(sizeof(OAIFdwIdentityNode));

				if (Identity->type != XML_ELEMENT_NODE)
					continue;

				node->name = pstrdup((char *)Identity->name);
				node->description = pstrdup((char *)xmlNodeGetContent(Identity));
				result = lappend(result, node);
			}
		}
	}

	xmlFreeDoc(state->xmldoc);
	xmlCleanupParser();

	elog(DEBUG1, "%s => finished", __func__);

	return result;
}

/*
 * Parses information from the OAI ListSets request.
 * https://www.openarchives.org/OAI/openarchivesprotocol.html#ListSets
 */
static List *GetSets(OAIFdwState *state)
{
	int oaiExecuteResponse;
	List *result = NIL;

	elog(DEBUG1, "%s called", __func__);

	state->requestVerb = OAI_REQUEST_LISTSETS;

	oaiExecuteResponse = ExecuteOAIRequest(state);

	if (!state->xmldoc)
		elog(ERROR, "invalid %s response from '%s'", state->requestVerb, state->url);

	xmlInitParser();

	if (oaiExecuteResponse == OAI_SUCCESS)
	{
		xmlNodePtr oai_root;
		xmlNodePtr ListSets;
		xmlNodePtr SetElement;
		xmlNodePtr xmlroot = xmlDocGetRootElement(state->xmldoc);

		if (xmlroot == NULL)
			elog(ERROR, "invalid root element for %s response", state->requestVerb);

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next)
		{
			if (oai_root->type != XML_ELEMENT_NODE)
				continue;

			if (xmlStrcmp(oai_root->name, (xmlChar *)OAI_REQUEST_LISTSETS) != 0)
				continue;

			for (ListSets = oai_root->children; ListSets != NULL; ListSets = ListSets->next)
			{
				OAISet *set = (OAISet *)palloc0(sizeof(OAISet));

				if (ListSets->type != XML_ELEMENT_NODE)
					continue;
				if (xmlStrcmp(ListSets->name, (xmlChar *)"set") != 0)
					continue;

				for (SetElement = ListSets->children; SetElement != NULL; SetElement = SetElement->next)
				{
					if (SetElement->type != XML_ELEMENT_NODE)
						continue;

					if (xmlStrcmp(SetElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SETSPEC) == 0)
					{
						set->setSpec = pstrdup((char *)xmlNodeGetContent(SetElement));
					}
					else if (xmlStrcmp(SetElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SETNAME) == 0)
					{
						set->setName = pstrdup((char *)xmlNodeGetContent(SetElement));
					}
				}

				result = lappend(result, set);
			}
		}
	}

	xmlFreeDoc(state->xmldoc);
	xmlCleanupParser();

	elog(DEBUG1, "%s => finished", __func__);

	return result;
}

/*
 * Parses information from the OAI ListMetadataFormats request.
 * https://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats
 */
static List *GetMetadataFormats(OAIFdwState *state)
{

	int oaiExecuteResponse;
	List *result = NIL;

	elog(DEBUG1, "  %s called", __func__);

	state->requestVerb = OAI_REQUEST_LISTMETADATAFORMATS;

	oaiExecuteResponse = ExecuteOAIRequest(state);

	if (!state->xmldoc)
		elog(ERROR, "invalid %s response from '%s'", state->requestVerb, state->url);

	xmlInitParser();

	if (oaiExecuteResponse == OAI_SUCCESS)
	{
		xmlNodePtr oai_root;
		xmlNodePtr ListMetadataFormats;
		xmlNodePtr MetadataElement;
		xmlNodePtr xmlroot = xmlDocGetRootElement(state->xmldoc);

		if (xmlroot == NULL)
			elog(ERROR, "invalid root element for %s response", state->requestVerb);

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next)
		{
			if (oai_root->type != XML_ELEMENT_NODE)
				continue;

			if (xmlStrcmp(oai_root->name, (xmlChar *)OAI_REQUEST_LISTMETADATAFORMATS) != 0)
				continue;

			for (ListMetadataFormats = oai_root->children; ListMetadataFormats != NULL; ListMetadataFormats = ListMetadataFormats->next)
			{
				OAIMetadataFormat *format = (OAIMetadataFormat *)palloc0(sizeof(OAIMetadataFormat));

				if (ListMetadataFormats->type != XML_ELEMENT_NODE)
					continue;
				if (xmlStrcmp(ListMetadataFormats->name, (xmlChar *)OAI_RESPONSE_ELEMENT_METADATAFORMAT) != 0)
					continue;

				for (MetadataElement = ListMetadataFormats->children; MetadataElement != NULL; MetadataElement = MetadataElement->next)
				{
					if (MetadataElement->type != XML_ELEMENT_NODE)
						continue;

					if (xmlStrcmp(MetadataElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_METADATAPREFIX) == 0)
					{
						format->metadataPrefix = pstrdup((char *)xmlNodeGetContent(MetadataElement));
					}
					else if (xmlStrcmp(MetadataElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SCHEMA) == 0)
					{
						format->schema = pstrdup((char *)xmlNodeGetContent(MetadataElement));
					}
					else if (xmlStrcmp(MetadataElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_METADATANAMESPACE) == 0)
					{
						format->metadataNamespace = pstrdup((char *)xmlNodeGetContent(MetadataElement));
					}
				}

				result = lappend(result, format);
			}
		}
	}

	xmlFreeDoc(state->xmldoc);
	xmlCleanupParser();

	elog(DEBUG1, "  %s => finished.", __func__);

	return result;
}

/**
 * Checks if a given URL is valid.
 */
static int CheckURL(char *url)
{

	CURLUcode code;
	CURLU *handler = curl_url();

	elog(DEBUG1, "%s called > '%s'", __func__, url);

	code = curl_url_set(handler, CURLUPART_URL, url, 0);

	curl_url_cleanup(handler);

	elog(DEBUG1, "  %s handler return code: %u", __func__, code);

	if (code != 0)
	{
		elog(DEBUG1, "%s: invalid URL (%u) > '%s'", __func__, code, url);

		return code;
	}

	return OAI_SUCCESS;
}

static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t realsize = size * nmemb;
	struct MemoryStruct *mem = (struct MemoryStruct *)userp;
	char *ptr = repalloc(mem->memory, mem->size + realsize + 1);

	if (!ptr)
	{

		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("out of memory (repalloc returned NULL)")));
	}

	mem->memory = ptr;
	memcpy(&(mem->memory[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->memory[mem->size] = 0;

	return realsize;
}

static size_t HeaderCallbackFunction(char *contents, size_t size, size_t nmemb, void *userp)
{

	size_t realsize = size * nmemb;
	struct MemoryStruct *mem = (struct MemoryStruct *)userp;
	char *ptr;
	char *textxml = "content-type: text/xml";
	char *applicationxml = "content-type: application/xml";

	/* is it a "content-type" entry? "*/
	if (strncasecmp(contents, textxml, 13) == 0)
	{
		/*
		 * if the content-type isn't "text/xml" or "application/xml"
		 * return 0 (fail), as other content-types aren't supported
		 * in the OAI 2.0 Standard.
		 */
		if (strncasecmp(contents, textxml, strlen(textxml)) != 0 &&
			strncasecmp(contents, applicationxml, strlen(applicationxml)) != 0)
		{
			/* remove crlf */
			contents[strlen(contents) - 2] = '\0';
			elog(WARNING, "%s: unsupported header entry: \"%s\"", __func__, contents);
			return 0;
		}
	}

	ptr = repalloc(mem->memory, mem->size + realsize + 1);

	if (!ptr)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("[%s] out of memory (repalloc returned NULL)", __func__)));
	}

	mem->memory = ptr;
	memcpy(&(mem->memory[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->memory[mem->size] = 0;

	return realsize;
}

/*
 * CURLProgressCallback
 * --------------------
 * Progress callback function for cURL requests. This allows us to
 * check for interruptions to immediatelly cancel the request.
 *
 * dltotal: Total bytes to download
 * dlnow: Bytes downloaded so far
 * ultotal: Total bytes to upload
 * ulnow: Bytes uploaded so far
 */
static int CURLProgressCallback(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow)
{
	CHECK_FOR_INTERRUPTS();

	return 0;
}

/**
 * Executes the HTTP request to the OAI repository using the
 * libcurl library.
 */
static int ExecuteOAIRequest(OAIFdwState *state)
{

	CURL *curl;
	CURLcode res;
	StringInfoData url_buffer;
	StringInfoData user_agent;
	char errbuf[CURL_ERROR_SIZE];
	struct MemoryStruct chunk;
	struct MemoryStruct chunk_header;
	long maxretries = OAI_REQUEST_MAXRETRY;
	long connectTimeout = OAI_REQUEST_CONNECTTIMEOUT;
	struct curl_slist *headers = NULL;

	if (state->maxretries)
		maxretries = state->maxretries;

	if (state->connectTimeout)
		connectTimeout = state->connectTimeout;

	chunk.memory = palloc(1);
	chunk.size = 0; /* no data at this point */
	chunk_header.memory = palloc(1);
	chunk_header.size = 0; /* no data at this point */

	initStringInfo(&url_buffer);

	elog(DEBUG1, "%s called: base url > '%s' ", __func__, state->url);

	/* Initialize curl early so it's available for URL encoding */
	curl_global_init(CURL_GLOBAL_ALL);
	curl = curl_easy_init();

	if (!curl)
	{
		curl_global_cleanup();
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("%s: failed to initialize curl", __func__)));
	}

	appendStringInfo(&url_buffer, "verb=%s", state->requestVerb);

	if (strcmp(state->requestVerb, OAI_REQUEST_LISTRECORDS) == 0)
	{

		if (state->set)
		{
			elog(DEBUG1, "  %s (%s): appending 'set' > %s", __func__, state->requestVerb, state->set);
			appendStringInfo(&url_buffer, "&set=%s", state->set);
		}

		if (state->from)
		{
			elog(DEBUG1, "  %s (%s): appending 'from' > %s", __func__, state->requestVerb, state->from);
			appendStringInfo(&url_buffer, "&from=%s", state->from);
		}

		if (state->until)
		{
			elog(DEBUG1, "  %s (%s): appending 'until' > %s", __func__, state->requestVerb, state->until);
			appendStringInfo(&url_buffer, "&until=%s", state->until);
		}

		if (state->metadataPrefix)
		{
			elog(DEBUG1, "  %s (%s): appending 'metadataPrefix' > %s", __func__, state->requestVerb, state->metadataPrefix);
			appendStringInfo(&url_buffer, "&metadataPrefix=%s", state->metadataPrefix);
		}

		if (state->resumptionToken)
		{
			char *encoded_token;

			elog(DEBUG1, "  %s (%s): appending 'resumptionToken' > %s", __func__, state->requestVerb, state->resumptionToken);
			resetStringInfo(&url_buffer);
			
			/* URL-encode the resumption token to handle special characters like & */
			encoded_token = curl_easy_escape(curl, state->resumptionToken, 0);
			if (encoded_token)
			{
				elog(DEBUG1, "  %s (%s): encoded resumptionToken > %s", __func__, state->requestVerb, encoded_token);
				appendStringInfo(&url_buffer, "verb=%s&resumptionToken=%s", state->requestVerb, encoded_token);
				curl_free(encoded_token);
			}
			else
			{
				/* Fallback to unencoded if encoding fails */
				elog(DEBUG1, "  %s (%s): encoding failed, using raw token", __func__, state->requestVerb);
				appendStringInfo(&url_buffer, "verb=%s&resumptionToken=%s", state->requestVerb, state->resumptionToken);
			}
		}
	}
	else if (strcmp(state->requestVerb, OAI_REQUEST_GETRECORD) == 0)
	{

		if (state->identifier)
		{
			elog(DEBUG1, "  %s (%s): appending 'identifier' > %s", __func__, state->requestVerb, state->identifier);
			appendStringInfo(&url_buffer, "&identifier=%s", state->identifier);
		}

		if (state->metadataPrefix)
		{
			elog(DEBUG1, "  %s (%s): appending 'metadataPrefix' > %s", __func__, state->requestVerb, state->metadataPrefix);
			appendStringInfo(&url_buffer, "&metadataPrefix=%s", state->metadataPrefix);
		}
	}
	else if (strcmp(state->requestVerb, OAI_REQUEST_LISTIDENTIFIERS) == 0)
	{

		if (state->set)
		{
			elog(DEBUG1, "  %s (%s): appending 'set' > %s", __func__, state->requestVerb, state->set);
			appendStringInfo(&url_buffer, "&set=%s", state->set);
		}

		if (state->from)
		{
			elog(DEBUG1, "  %s (%s): appending 'from' > %s", __func__, state->requestVerb, state->from);
			appendStringInfo(&url_buffer, "&from=%s", state->from);
		}

		if (state->until)
		{
			elog(DEBUG1, "  %s (%s): appending 'until' > %s", __func__, state->requestVerb, state->until);
			appendStringInfo(&url_buffer, "&until=%s", state->until);
		}

		if (state->metadataPrefix)
		{
			elog(DEBUG1, "  %s (%s): appending 'metadataPrefix' > %s", __func__, state->requestVerb, state->metadataPrefix);
			appendStringInfo(&url_buffer, "&metadataPrefix=%s", state->metadataPrefix);
		}

		if (state->resumptionToken)
		{
			char *encoded_token;

			elog(DEBUG1, "  %s (%s): appending 'resumptionToken' > %s", __func__, state->requestVerb, state->resumptionToken);
			resetStringInfo(&url_buffer);
			
			/* URL-encode the resumption token to handle special characters like & */
			encoded_token = curl_easy_escape(curl, state->resumptionToken, 0);
			if (encoded_token)
			{
				elog(DEBUG1, "  %s (%s): encoded resumptionToken > %s", __func__, state->requestVerb, encoded_token);
				appendStringInfo(&url_buffer, "verb=%s&resumptionToken=%s", state->requestVerb, encoded_token);
				curl_free(encoded_token);
			}
			else
			{
				/* Fallback to unencoded if encoding fails */
				elog(DEBUG1, "  %s (%s): encoding failed, using raw token", __func__, state->requestVerb);
				appendStringInfo(&url_buffer, "verb=%s&resumptionToken=%s", state->requestVerb, state->resumptionToken);
			}
		}
	}
	else
	{
		if (strcmp(state->requestVerb, OAI_REQUEST_LISTMETADATAFORMATS) != 0 &&
			strcmp(state->requestVerb, OAI_REQUEST_LISTSETS) != 0 &&
			strcmp(state->requestVerb, OAI_REQUEST_IDENTIFY) != 0)
		{
			return OAI_UNKNOWN_REQUEST;
		}
	}

	elog(DEBUG1, "  %s (%s): url build > %s?%s", __func__, state->requestVerb, state->url, url_buffer.data);

	if (curl)
	{
		errbuf[0] = 0;

		curl_easy_setopt(curl, CURLOPT_URL, state->url);

#if ((LIBCURL_VERSION_MAJOR == 7 && LIBCURL_VERSION_MINOR < 85) || LIBCURL_VERSION_MAJOR < 7)
		curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
#else
		curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
#endif

		curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

		curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connectTimeout);
		elog(DEBUG1, "  %s (%s): timeout > %ld", __func__, state->requestVerb, connectTimeout);
		elog(DEBUG1, "  %s (%s): max retry > %ld", __func__, state->requestVerb, maxretries);

		/* Proxy support: added in version 1.1.0 */
		if (state->proxy)
		{

			elog(DEBUG1, "  %s (%s): proxy URL > '%s'", __func__, state->requestVerb, state->proxy);

			curl_easy_setopt(curl, CURLOPT_PROXY, state->proxy);

			if (strcmp(state->proxyType, OAI_NODE_HTTP_PROXY) == 0)
			{
				elog(DEBUG1, "  %s (%s): proxy protocol > 'HTTP'", __func__, state->requestVerb);
				curl_easy_setopt(curl, CURLOPT_PROXYTYPE, CURLPROXY_HTTP);
			}
			else if (strcmp(state->proxyType, OAI_NODE_HTTPS_PROXY) == 0)
			{
				elog(DEBUG1, "  %s (%s): proxy protocol > 'HTTPS'", __func__, state->requestVerb);
				curl_easy_setopt(curl, CURLOPT_PROXYTYPE, CURLPROXY_HTTPS);
			}

			if (state->proxyUser)
			{
				elog(DEBUG1, "  %s (%s): entering proxy user ('%s').", __func__, state->requestVerb, state->proxyUser);
				curl_easy_setopt(curl, CURLOPT_PROXYUSERNAME, state->proxyUser);
			}

			if (state->proxyUserPassword)
			{
				elog(DEBUG1, "  %s (%s): entering proxy user's password.", __func__, state->requestVerb);
				curl_easy_setopt(curl, CURLOPT_PROXYUSERPWD, state->proxyUserPassword);
			}
		}

		if (state->requestRedirect == true)
		{

			elog(DEBUG1, "  %s (%s): setting request redirect: %d", __func__, state->requestVerb, state->requestRedirect);
			curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

			if (state->requestMaxRedirect)
			{
				elog(DEBUG1, "  %s (%s): setting maxredirs: %ld", __func__, state->requestVerb, state->requestMaxRedirect);
				curl_easy_setopt(curl, CURLOPT_MAXREDIRS, state->requestMaxRedirect);
			}
		}

		/* Set the progress callback function */
		curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, CURLProgressCallback);

		curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, url_buffer.data);
		curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCallbackFunction);
		curl_easy_setopt(curl, CURLOPT_HEADERDATA, (void *)&chunk_header);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
		curl_easy_setopt(curl, CURLOPT_FAILONERROR, true);

		if(state->user && state->password)
		{
			curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
			curl_easy_setopt(curl, CURLOPT_USERNAME, state->user);
			curl_easy_setopt(curl, CURLOPT_PASSWORD, state->password);
		}
		else if(state->user && !state->password)
		{
			curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
			curl_easy_setopt(curl, CURLOPT_USERNAME, state->user);
		}

		initStringInfo(&user_agent);
		appendStringInfo(&user_agent, "PostgreSQL/%s oai_fdw/%s libxml2/%s %s", PG_VERSION, OAI_FDW_VERSION, LIBXML_DOTTED_VERSION, curl_version());
		curl_easy_setopt(curl, CURLOPT_USERAGENT, user_agent.data);

		headers = curl_slist_append(headers, "Accept: application/xml");
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

		elog(DEBUG2, "  %s (%s): performing cURL request ... ", __func__, state->requestVerb);

		res = curl_easy_perform(curl);

		if (res != CURLE_OK)
		{
			for (long i = 1; i <= maxretries && (res = curl_easy_perform(curl)) != CURLE_OK; i++)
			{
				elog(WARNING, "  %s (%s): request to '%s' failed (%ld)", __func__, state->requestVerb, state->url, i);
			}
		}

		if (res != CURLE_OK)
		{
			size_t len = strlen(errbuf);
			fprintf(stderr, "\nlibcurl: (%d) ", res);

			if (chunk.memory)
				pfree(chunk.memory);
			if (chunk_header.memory)
				pfree(chunk_header.memory);
			curl_slist_free_all(headers);
			curl_easy_cleanup(curl);
			curl_global_cleanup();

			if (len)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						 errmsg("unable to connect to the OAI repository"),
						 errdetail("%s => (%u) %s%s", __func__, res, errbuf,
								((errbuf[len - 1] != '\n') ? "\n" : ""))));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						errmsg("unable to connect to the OAI repository"),
						errdetail("%s => (%u) '%s'\n", __func__, res, curl_easy_strerror(res))));
			}
		}
		else
		{
			long response_code;
			curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
			state->xmldoc = xmlReadMemory(chunk.memory, chunk.size, NULL, NULL, XML_PARSE_NOBLANKS);

			elog(DEBUG1, "  %s (%s): http response code = %ld", __func__, state->requestVerb, response_code);
			elog(DEBUG1, "  %s (%s): http response size = %ld", __func__, state->requestVerb, chunk.size);
			elog(DEBUG1, "  %s (%s): http response header = \n%s", __func__, state->requestVerb, chunk_header.memory);
		}
	}

	if (chunk.memory)
		pfree(chunk.memory);
	if (chunk_header.memory)
		pfree(chunk_header.memory);
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	curl_global_cleanup();

	return OAI_SUCCESS;
}

/**
 * This function validates the oai_nodes in the OPTION clause of each column
 * and its data types. Additionally it chooses which OAI Request will be
 * executed bases on the oai_nodes set in the foreign table (ListRecords or
 * ListIdentifiers).
 *
 * ListRecords:     https://www.openarchives.org/OAI/openarchivesprotocol.html#ListRecords
 * ListIdentifiers: https://www.openarchives.org/OAI/openarchivesprotocol.html#ListIdentifiers
 */
static void OAIRequestPlanner(OAIFdwState *state, RelOptInfo *baserel)
{
	List *conditions = baserel->baserestrictinfo;
	bool hasContentForeignColumn = false;	
	TupleDesc tupdesc;

#if PG_VERSION_NUM < 130000
	Relation rel = heap_open(state->foreign_table->relid, NoLock);
#else
	Relation rel = table_open(state->foreign_table->relid, NoLock);
#endif

	char *relname = NameStr(rel->rd_rel->relname);	
	elog(DEBUG1, "%s called.", __func__);

	tupdesc = rel->rd_att;

	/* The default request type is OAI_REQUEST_LISTRECORDS.
	 * This can be altered depending on the columns used
	 * in the WHERE and SELECT clauses */
	state->requestVerb = OAI_REQUEST_LISTRECORDS;
	state->numcols = rel->rd_att->natts;
	state->foreigntableid = state->foreign_table->relid;

	for (int i = 0; i < rel->rd_att->natts; i++)
	{
		List *options = GetForeignColumnOptions(state->foreign_table->relid, i + 1);
		ListCell *lc;
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		foreach (lc, options)
		{
			DefElem *def = (DefElem *)lfirst(lc);
			state->numfdwcols++;

			if (strcmp(def->defname, OAI_NODE_OPTION) == 0)
			{
				char *option_value = defGetString(def);
				char *attname = NameStr(attr->attname);
				//int atttypid = rel->rd_att->attrs[i].atttypid;

				if (strcmp(option_value, OAI_NODE_STATUS) == 0)
				{

					if (attr->atttypid != BOOLOID)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
								 errhint("OAI %s must be of type 'boolean'.",
										 OAI_NODE_STATUS)));
					}
				}
				else if (strcmp(option_value, OAI_NODE_IDENTIFIER) == 0 || strcmp(option_value, OAI_NODE_METADATAPREFIX) == 0)
				{
					if (attr->atttypid != TEXTOID &&
						attr->atttypid != VARCHAROID)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
								 errhint("OAI %s must be of type 'text' or 'varchar'.",
										 option_value)));
					}
				}
				else if (strcmp(option_value, OAI_NODE_CONTENT) == 0)
				{
					hasContentForeignColumn = true;

					if (attr->atttypid != TEXTOID &&
						attr->atttypid != VARCHAROID &&
						attr->atttypid != XMLOID)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
								 errhint("OAI %s expects one of the following types: 'xml', 'text' or 'varchar'.",
										 OAI_NODE_CONTENT)));
					}
				}
				else if (strcmp(option_value, OAI_NODE_SETSPEC) == 0)
				{
					if (attr->atttypid != TEXTARRAYOID &&
						attr->atttypid != VARCHARARRAYOID)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
								 errhint("OAI %s expects one of the following types: 'text[]', 'varchar[]'.",
										 OAI_NODE_SETSPEC)));
					}
				}
				else if (strcmp(option_value, OAI_NODE_DATESTAMP) == 0)
				{
					if (attr->atttypid != TIMESTAMPOID)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
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
	if (!hasContentForeignColumn)
	{
		state->requestVerb = OAI_REQUEST_LISTIDENTIFIERS;
		elog(DEBUG1, "  %s: the foreign table '%s' has no 'content' OAI node. Request type set to '%s'",
			 __func__, relname, OAI_REQUEST_LISTIDENTIFIERS);
	}

	if (state->numfdwcols != 0)
		deparseSelectColumns(state, baserel->reltarget->exprs);

	deparseWhereClause(state, conditions);

#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	elog(DEBUG1, "%s => finished.", __func__);
}

static TupleTableSlot *OAIFdwExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{

	ereport(
		ERROR, (
				   errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				   errmsg("Operation not supported."),
				   errhint("The OAI Foreign Data Wrapper does not support UPDATE queries.")));

	return NULL;
}

static TupleTableSlot *OAIFdwExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{

	ereport(
		ERROR, (
				   errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				   errmsg("Operation not supported."),
				   errhint("The OAI Foreign Data Wrapper does not support INSERT queries.")));

	return NULL;
}

static TupleTableSlot *OAIFdwExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{

	ereport(
		ERROR, (
				   errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				   errmsg("Operation not supported."),
				   errhint("The OAI Foreign Data Wrapper does not support DELETE queries.")));

	return NULL;
}

static char *GetOAINodeFromColumn(Oid foreigntableid, int16 attnum)
{

	List *options;
	TupleDesc tupdesc;
	char *optionValue = NULL;

#if PG_VERSION_NUM < 130000
	Relation rel = heap_open(foreigntableid, NoLock);
#else
	Relation rel = table_open(foreigntableid, NoLock);
#endif

	elog(DEBUG1, "  %s called", __func__);
	tupdesc = rel->rd_att;

	for (int i = 0; i < rel->rd_att->natts; i++)
	{

		ListCell *lc;
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		options = GetForeignColumnOptions(foreigntableid, i + 1);

		if (attr->attnum == attnum)
		{

			foreach (lc, options)
			{

				DefElem *def = (DefElem *)lfirst(lc);

				if (strcmp(def->defname, OAI_NODE_OPTION) == 0)
				{

					optionValue = defGetString(def);

					break;
				}
			}
		}
	}

#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	return optionValue;
}

static char *datumToString(Datum datum, Oid type)
{

	regproc typoutput;
	HeapTuple tuple;
	char *result;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "%s: cache lookup failed for type %u", __func__, type);

	typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;

	ReleaseSysCache(tuple);

	elog(DEBUG1, "%s: type > %u", __func__, type);

	switch (type)
	{

	case TEXTOID:
	case VARCHAROID:
		result = DatumGetCString(OidFunctionCall1(typoutput, datum));
		break;
	default:
		return NULL;
	}

	return result;
}

static void deparseExpr(Expr *expr, OAIFdwState *state)
{
	OpExpr *oper;
	Var *var;
	HeapTuple tuple;
	char *operName;
	char *oaiNode;

	elog(DEBUG2, "%s called for expr->type %u", __func__, expr->type);

	switch (expr->type)
	{

	case T_OpExpr:

		elog(DEBUG2, "  %s: case T_OpExpr", __func__);
		oper = (OpExpr *)expr;

		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper->opno));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "%s: cache lookup failed for operator %u", __func__, oper->opno);

		operName = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);

		ReleaseSysCache(tuple);

		elog(DEBUG2, "  %s: opername > %s", __func__, operName);

		var = (Var *)linitial(oper->args);
		oaiNode = GetOAINodeFromColumn(state->foreign_table->relid, var->varattno);

		if (strcmp(operName, "=") == 0)
		{
			if (strcmp(oaiNode, OAI_NODE_IDENTIFIER) == 0 && (var->vartype == TEXTOID || var->vartype == VARCHAROID))
			{
				Const *constant = (Const *)lsecond(oper->args);

				state->requestVerb = OAI_REQUEST_GETRECORD;
				state->identifier = datumToString(constant->constvalue, constant->consttype);

				elog(DEBUG2, "  %s: request type set to '%s' with identifier '%s'", __func__, OAI_REQUEST_GETRECORD, state->identifier);
			}

			if (strcmp(oaiNode, OAI_NODE_DATESTAMP) == 0 && (var->vartype == TIMESTAMPOID))
			{
				Const *constant = (Const *)lsecond(oper->args);
				state->from = deparseTimestamp(constant->constvalue);
				state->until = deparseTimestamp(constant->constvalue);
			}

			if (strcmp(oaiNode, OAI_NODE_METADATAPREFIX) == 0 && (var->vartype == TEXTOID || var->vartype == VARCHAROID))
			{
				Const *constant = (Const *)lsecond(oper->args);

				state->metadataPrefix = datumToString(constant->constvalue, constant->consttype);

				elog(DEBUG2, "  %s: metadataPrefix set to '%s'", __func__, state->metadataPrefix);
			}
		}

		if (strcmp(operName, ">=") == 0 || strcmp(operName, ">") == 0)
		{
			if (strcmp(oaiNode, OAI_NODE_DATESTAMP) == 0 && (var->vartype == TIMESTAMPOID))
			{
				Const *constant = (Const *)lsecond(oper->args);
				state->from = deparseTimestamp(constant->constvalue);
			}
		}

		if (strcmp(operName, "<=") == 0 || strcmp(operName, "<") == 0)
		{
			if (strcmp(oaiNode, OAI_NODE_DATESTAMP) == 0 && (var->vartype == TIMESTAMPOID))
			{
				Const *constant = (Const *)lsecond(oper->args);
				state->until = deparseTimestamp(constant->constvalue);
			}
		}

		if (strcmp(operName, "<@") == 0 || strcmp(operName, "@>") == 0 || strcmp(operName, "&&") == 0)
		{
			if (strcmp(oaiNode, OAI_NODE_SETSPEC) == 0 && (var->vartype == TEXTARRAYOID || var->vartype == VARCHARARRAYOID))
			{
				Const *constant = (Const *)lsecond(oper->args);
				ArrayType *array = (ArrayType *)constant->constvalue;

				int numitems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

				if (numitems > 1)
				{
					elog(WARNING, "The OAI standard requests do not support multiple '%s' attributes. This filter will be applied AFTER the OAI request.", OAI_NODE_SETSPEC);
					elog(DEBUG2, "  %s: clearing '%s' attribute.", __func__, OAI_NODE_SETSPEC);
					state->set = NULL;
				}
				else if (numitems == 1)
				{
					bool isnull;
					Datum value;

					ArrayIterator iterator = array_create_iterator(array, 0, NULL);

					while (array_iterate(iterator, &value, &isnull))
					{
						elog(DEBUG2, "  %s: setSpec set to '%s'", __func__, datumToString(value, TEXTOID));
						state->set = datumToString(value, TEXTOID);
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

static void deparseSelectColumns(OAIFdwState *state, List *exprs)
{
	ListCell *cell;

	elog(DEBUG2, "%s called", __func__);

	foreach (cell, exprs)
	{
		Expr *expr = (Expr *)lfirst(cell);

		elog(DEBUG2, "  %s: evaluating expr->type = %u", __func__, expr->type);

		deparseExpr(expr, state);
	}
}

static char *deparseTimestamp(Datum datum)
{

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

static void deparseWhereClause(OAIFdwState *state, List *conditions)
{
	ListCell *cell;

	foreach (cell, conditions)
	{
		Expr *expr = (Expr *)lfirst(cell);

		/* extract WHERE clause from RestrictInfo */
		if (IsA(expr, RestrictInfo))
		{
			RestrictInfo *ri = (RestrictInfo *)expr;
			expr = ri->clause;
		}

		deparseExpr(expr, state);
	}
}

static void OAIFdwGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	OAIFdwState *state = (OAIFdwState *)palloc0(sizeof(OAIFdwState));
	state->foreign_table = GetForeignTable(foreigntableid);
	state->foreign_server = GetForeignServer(state->foreign_table->serverid);

	InitSession(state, baserel);
	baserel->fdw_private = state;
}

static void OAIFdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{

	Path *path = (Path *)create_foreignscan_path(root, baserel,
												 NULL,			/* default pathtarget */
												 baserel->rows, /* rows */
#if PG_VERSION_NUM >= 180000
												 0, /* no parallel pathflags */
#endif
												 1,					/* startup cost */
												 1 + baserel->rows, /* total cost */
												 NIL,				/* no pathkeys */
												 NULL,				/* no required outer relids */
												 NULL,				/* no fdw_outerpath */
#if PG_VERSION_NUM >= 170000
												 NIL,	/* no fdw_restrictinfo */
#endif													/* PG_VERSION_NUM */
												 NULL); /* no fdw_private */
	add_path(baserel, path);
}

static ForeignScan *OAIFdwGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
	OAIFdwState *state = baserel->fdw_private;
	List *fdw_private;

	fdw_private = list_make1(state);

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
							scan_clauses,
							baserel->relid,
							NIL,		 /* no expressions we will evaluate */
							fdw_private, /* pass along our start and end */
							NIL,		 /* no custom tlist; our scan tuple looks like tlist */
							NIL,		 /* no quals we will recheck */
							outer_plan);
}

static void OAIFdwBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fs = (ForeignScan *)node->ss.ps.plan;
	OAIFdwState *state = (OAIFdwState *)linitial(fs->fdw_private);
	//EState *estate = node->ss.ps.state;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	node->fdw_state = (void *)state;

	xmlInitParser();

	state->oaicxt = AllocSetContextCreate(CurrentMemoryContext,
										  "oai_fdw_ctx",
										  ALLOCSET_DEFAULT_SIZES);
}

static OAIRecord *FetchNextOAIRecord(OAIFdwState **state)
{
	if ((*state)->pageindex == (*state)->pagesize)
	{
		elog(DEBUG1, "%s: EOF > %d/%d", __func__, (*state)->pageindex, (*state)->pagesize);
		return NULL;
	}
	else
	{
		OAIRecord *record;
		ListCell *cell;

		cell = list_nth_cell((*state)->records, (*state)->pageindex);
		record = (OAIRecord *)lfirst(cell);

		(*state)->rowcount++;
		(*state)->pageindex++;

		return record;
	}
}

static void CreateOAITuple(TupleTableSlot *slot, OAIFdwState *state, OAIRecord *oai)
{

	MemoryContext old_cxt, tmp_cxt;

	elog(DEBUG2, "%s called", __func__);

	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
									"oai_fdw temporary data",
									ALLOCSET_SMALL_SIZES);

	old_cxt = MemoryContextSwitchTo(tmp_cxt);

	for (int i = 0; i < state->numcols; i++)
	{

		List *options = GetForeignColumnOptions(state->foreigntableid, i + 1);
		ListCell *lc;

		foreach (lc, options)
		{

			DefElem *def = (DefElem *)lfirst(lc);

			if (strcmp(def->defname, OAI_NODE_OPTION) == 0)
			{

				char *option_value = defGetString(def);

				if (strcmp(option_value, OAI_NODE_STATUS) == 0)
				{

					elog(DEBUG2, "  %s: setting column %d option '%s'", __func__, i, option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = oai->isDeleted;
				}
				else if (strcmp(option_value, OAI_NODE_IDENTIFIER) == 0 && oai->identifier)
				{

					elog(DEBUG2, "  %s: setting column %d option '%s'", __func__, i, option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->identifier);
				}
				else if (strcmp(option_value, OAI_NODE_METADATAPREFIX) == 0 && oai->metadataPrefix)
				{

					elog(DEBUG2, "  %s: setting column %d option '%s'", __func__, i, option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->metadataPrefix);
				}
				else if (strcmp(option_value, OAI_NODE_CONTENT) == 0 && oai->content)
				{

					elog(DEBUG2, "  %s: setting column %d option '%s'", __func__, i, option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum((char *)oai->content);
				}
				else if (strcmp(option_value, OAI_NODE_SETSPEC) == 0 && oai->setsArray)
				{

					elog(DEBUG2, "  %s: setting column %d option '%s'", __func__, i, option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = (Datum)DatumGetArrayTypeP((Datum)oai->setsArray);
				}
				else if (strcmp(option_value, OAI_NODE_DATESTAMP) == 0 && oai->datestamp)
				{

					char lowstr[MAXDATELEN + 1];
					char *field[MAXDATEFIELDS];
					int ftype[MAXDATEFIELDS];
					int nf;
					int parseError = ParseDateTime(oai->datestamp, lowstr, MAXDATELEN, field, ftype, MAXDATEFIELDS, &nf);

					elog(DEBUG2, "  %s: setting column %d option '%s'", __func__, i, option_value);

					if (parseError == 0)
					{

						int dtype;
						struct pg_tm date;
						fsec_t fsec = 0;
						int tz = 0;

#if PG_VERSION_NUM < 160000
						int decodeError = DecodeDateTime(field, ftype, nf, &dtype, &date, &fsec, &tz);
#else
						DateTimeErrorExtra extra;
						int decodeError = DecodeDateTime(field, ftype, nf, &dtype, &date, &fsec, &tz, &extra);
#endif

						Timestamp tmsp;
						tm2timestamp(&date, fsec, &tz, &tmsp);

						if (decodeError == 0)
						{

							slot->tts_isnull[i] = false;
							slot->tts_values[i] = (Datum)TimestampTzGetDatum(tmsp);

							elog(DEBUG2, "  %s: datestamp (\"%s\") parsed and decoded.", __func__, oai->datestamp);
						}
						else
						{

							slot->tts_isnull[i] = true;
							slot->tts_values[i] = PointerGetDatum(NULL);
							;
							elog(WARNING, "  %s: could not decode datestamp: %s", __func__, oai->datestamp);
						}
					}
					else
					{

						slot->tts_isnull[i] = true;
						slot->tts_values[i] = PointerGetDatum(NULL);
						;

						elog(WARNING, "  %s: could not parse datestamp: %s", __func__, oai->datestamp);
					}
				}
				else
				{

					slot->tts_isnull[i] = true;
					slot->tts_values[i] = PointerGetDatum(NULL);
				}
			}
		}
	}

	MemoryContextSwitchTo(old_cxt);
}

static TupleTableSlot *OAIFdwIterateForeignScan(ForeignScanState *node)
{

	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	struct OAIFdwState *state = (struct OAIFdwState *)node->fdw_state;
	OAIRecord *record;
	MemoryContext old_cxt;

	elog(DEBUG2, "%s called.", __func__);

	ExecClearTuple(slot);

	old_cxt = MemoryContextSwitchTo(state->oaicxt);

	/* Returns an empty tuple in case there is no mapping for OAI nodes and columns */
	if (state->numfdwcols == 0)
		return slot;
	/*
	 * Load OAI records in case that this function is called for the first time
	 * or a page contains a resumption token and the index reached the end of
	 * the page.
	 */
	if (state->rowcount == 0 || (state->resumptionToken && state->pageindex == state->pagesize))
		LoadOAIRecords(&state);

	record = FetchNextOAIRecord(&state);

	MemoryContextSwitchTo(old_cxt);

	if (record != NULL)
	{
		elog(DEBUG2, "  %s: creating OAI tuple", __func__);
		CreateOAITuple(slot, state, record);
		elog(DEBUG2, "  %s: storing virtual tuple", __func__);
		ExecStoreVirtualTuple(slot);
		pfree(record);
	}
	else
	{
		/*
		 * No further records to be retrieved. Let's clean up the XML parser 
		 * before ending the query.
		 */
		xmlCleanupParser();
		MemoryContextDelete(state->oaicxt);
	}

	elog(DEBUG1, "%s => returning tuple (rowcount: %d)", __func__, state->rowcount);

	return slot;
}

static void RaiseOAIException(xmlNodePtr error)
{

	if (strcmp((char *)xmlGetProp(error, (xmlChar *)"code"), OAI_ERROR_ID_DOES_NOT_EXIST) == 0 ||
		strcmp((char *)xmlGetProp(error, (xmlChar *)"code"), OAI_ERROR_NO_RECORD_MATCH) == 0)
	{
		ereport(WARNING,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("OAI %s: %s", (char *)xmlGetProp(error, (xmlChar *)"code"), (char *)xmlNodeGetContent(error))));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("OAI %s: %s", (char *)xmlGetProp(error, (xmlChar *)"code"), (char *)xmlNodeGetContent(error))));
	}
}

static void LoadOAIRecords(struct OAIFdwState **state)
{
	xmlNodePtr xmlroot;
	xmlNodePtr oaipmh;
	xmlNodePtr headerElements;
	xmlNodePtr ListRecordsRequest;

	elog(DEBUG1, "%s called.", __func__);

	/* Sets the page size and index to zero.*/
	(*state)->pagesize = 0;
	(*state)->pageindex = 0;
	/* Removes all retrieved records, if any.*/
	(*state)->records = NIL;

	if (ExecuteOAIRequest(*state) == OAI_SUCCESS)
	{
		/*
		 * After executing an OAI request the resumption token is no longer
		 * needed. A new resumption token will be loaded in case there are
		 * still records left to be retrieved.
		 */
		(*state)->resumptionToken = NULL;

		xmlroot = xmlDocGetRootElement((*state)->xmldoc);

		if (strcmp((*state)->requestVerb, OAI_REQUEST_LISTIDENTIFIERS) == 0)
		{
			for (oaipmh = xmlroot->children; oaipmh != NULL; oaipmh = oaipmh->next)
			{
				if (xmlStrcmp(oaipmh->name, (xmlChar *)"error") == 0)
					RaiseOAIException(oaipmh);
				else if (xmlStrcmp(oaipmh->name, (xmlChar *)(*state)->requestVerb) != 0)
					continue;

				for (ListRecordsRequest = oaipmh->children; ListRecordsRequest != NULL; ListRecordsRequest = ListRecordsRequest->next)
				{

					if (xmlStrcmp(ListRecordsRequest->name, (xmlChar *)OAI_RESPONSE_ELEMENT_RESUMPTIONTOKEN) == 0)
					{

						xmlChar *tokenContent = xmlNodeGetContent(ListRecordsRequest);
						if (tokenContent && strlen((char *)tokenContent) != 0)
						{
							(*state)->resumptionToken = pstrdup((char *)tokenContent);
							elog(DEBUG2, "  %s: (%s): Token detected in current page > %s", __func__, (*state)->requestVerb, (char *)tokenContent);
						}
						xmlFree(tokenContent);
					}
					else if (xmlStrcmp(ListRecordsRequest->name, (xmlChar *)OAI_RESPONSE_ELEMENT_HEADER) == 0)
					{

						OAIRecord *oai = (OAIRecord *)palloc(sizeof(OAIRecord));

						oai->setsArray = NULL;
						oai->isDeleted = false;
						oai->metadataPrefix = pstrdup((*state)->metadataPrefix);

						if (xmlGetProp(ListRecordsRequest, (xmlChar *)OAI_NODE_STATUS))
						{

							char *status = (char *)xmlGetProp(ListRecordsRequest, (xmlChar *)OAI_NODE_STATUS);

							if (strcmp(status, OAI_RESPONSE_ELEMENT_DELETED))
								oai->isDeleted = true;
						}

						for (headerElements = ListRecordsRequest->children; headerElements != NULL; headerElements = headerElements->next)
						{

							xmlBufferPtr buffer = xmlBufferCreate();
							xmlNodeDump(buffer, (*state)->xmldoc, headerElements->children, 0, 0);

							if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_IDENTIFIER) == 0)
							{
								oai->identifier = pstrdup((char *)buffer->content);
							}
							else if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SETSPEC) == 0)
							{
								char *array_element = pstrdup((char *)buffer->content);
								appendTextArray(&oai->setsArray, array_element);
							}
							else if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_DATESTAMP) == 0)
							{
								oai->datestamp = pstrdup((char *)buffer->content);
							}

							xmlBufferFree(buffer);
						}

						elog(DEBUG1, "  %s (%s): APPENDING RECORDS LIST -> %s", __func__, (*state)->requestVerb, oai->identifier);

						(*state)->records = lappend((*state)->records, oai);
						(*state)->pagesize++;
					}
				}
			}
		}
		else if (strcmp((*state)->requestVerb, OAI_REQUEST_LISTRECORDS) == 0 || strcmp((*state)->requestVerb, OAI_REQUEST_GETRECORD) == 0)
		{

			for (oaipmh = xmlroot->children; oaipmh != NULL; oaipmh = oaipmh->next)
			{

				if (xmlStrcmp(oaipmh->name, (xmlChar *)"error") == 0)
					RaiseOAIException(oaipmh);
				else if (xmlStrcmp(oaipmh->name, (xmlChar *)(*state)->requestVerb) != 0)
					continue;

				for (ListRecordsRequest = oaipmh->children; ListRecordsRequest != NULL; ListRecordsRequest = ListRecordsRequest->next)
				{

					if (xmlStrcmp(ListRecordsRequest->name, (xmlChar *)OAI_RESPONSE_ELEMENT_RESUMPTIONTOKEN) == 0)
					{
						xmlChar *tokenContent = xmlNodeGetContent(ListRecordsRequest);
						if (tokenContent && strlen((char *)tokenContent) != 0)
							(*state)->resumptionToken = pstrdup((char *)tokenContent);
						xmlFree(tokenContent);
					}
					else if (xmlStrcmp(ListRecordsRequest->name, (xmlChar *)OAI_RESPONSE_ELEMENT_RECORD) == 0)
					{
						OAIRecord *oai = (OAIRecord *)palloc(sizeof(OAIRecord));
						xmlNodePtr record;

						oai->metadataPrefix = pstrdup((*state)->metadataPrefix);
						oai->isDeleted = false;
						oai->setsArray = NULL;

						for (record = ListRecordsRequest->children; record != NULL; record = record->next)
						{

							if (xmlStrcmp(record->name, (xmlChar *)OAI_RESPONSE_ELEMENT_METADATA) == 0)
							{

								/* Copy necessary to include the namespaces in the buffer output */
								xmlNodePtr copy = xmlCopyNode(record->children, 1);

								xmlBufferPtr buffer = xmlBufferCreate();
								xmlNodeDump(buffer, (*state)->xmldoc, copy, 0, 1);

								elog(DEBUG2, "  %s (%s): XML Buffer size: %d", __func__, (*state)->requestVerb, buffer->size);

								oai->content = pstrdup((char *)buffer->content);

								elog(DEBUG2, "  %s (%s): freeing node copy.", __func__, (*state)->requestVerb);
								xmlFreeNode(copy);
								elog(DEBUG2, "  %s (%s): freeing xml content buffer.", __func__, (*state)->requestVerb);
								xmlBufferFree(buffer);
							}

							if (xmlStrcmp(record->name, (xmlChar *)OAI_RESPONSE_ELEMENT_HEADER) == 0)
							{

								if (xmlGetProp(record, (xmlChar *)OAI_NODE_STATUS))
								{
									char *status = (char *)xmlGetProp(record, (xmlChar *)OAI_NODE_STATUS);

									if (strcmp(status, OAI_RESPONSE_ELEMENT_DELETED))
										oai->isDeleted = true;
								}

								for (headerElements = record->children; headerElements != NULL; headerElements = headerElements->next)
								{

									xmlBufferPtr buffer = xmlBufferCreate();
									xmlNodeDump(buffer, (*state)->xmldoc, headerElements->children, 0, 0);

									if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_IDENTIFIER) == 0)
									{

										oai->identifier = pstrdup((char *)buffer->content);

										elog(DEBUG2, "  %s (%s): setting identifier to OAI object > '%s'", __func__, (*state)->requestVerb, oai->identifier);
									}
									else if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SETSPEC) == 0)
									{

										char *array_element = pstrdup((char *)buffer->content);
										elog(DEBUG2, "  %s (%s): setting setspec to OAI object > '%s'", __func__, (*state)->requestVerb, array_element);

										appendTextArray(&oai->setsArray, array_element);
									}
									else if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_DATESTAMP) == 0)
									{

										oai->datestamp = pstrdup((char *)buffer->content);
										elog(DEBUG2, "  %s (%s): setting datestamp to OAI object > '%s'", __func__, (*state)->requestVerb, oai->datestamp);
									}

									elog(DEBUG3, "  %s (%s): freeing header buffer.", __func__, (*state)->requestVerb);
									xmlBufferFree(buffer);
								}
							}
						}

						(*state)->records = lappend((*state)->records, oai);
						(*state)->pagesize++;
					}
				}
			}
		}
	}

	if((*state)->xmldoc)
		xmlFreeDoc((*state)->xmldoc);
}

static void appendTextArray(ArrayType **array, char *text_element)
{

	/* Array build variables */
	size_t arr_nelems = 0;
	size_t arr_elems_size = 1;
	Oid elem_type = TEXTOID;
	int16 elem_len;
	bool elem_byval;
	char elem_align;
	Datum *arr_elems = palloc0(arr_elems_size * sizeof(Datum));

	elog(DEBUG2, "  %s called with element > %s", __func__, text_element);

	get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

	if (*array == NULL)
	{
		/*Array has no elements */
		arr_elems[arr_nelems] = CStringGetTextDatum(text_element);
		elog(DEBUG3, "    %s: array empty! adding value at arr_elems[%ld]", __func__, arr_nelems);
		arr_nelems++;
	}
	else
	{
		bool isnull;
		Datum value;
		ArrayIterator iterator = array_create_iterator(*array, 0, NULL);
		elog(DEBUG3, "    %s: current array size: %d", __func__, ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array)));

		arr_elems_size *= ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array)) + 1;
		arr_elems = repalloc(arr_elems, arr_elems_size * sizeof(Datum));

		while (array_iterate(iterator, &value, &isnull))
		{
			if (isnull)
				continue;

			elog(DEBUG3, "    %s: re-adding element: arr_elems[%ld]. arr_elems_size > %ld", __func__, arr_nelems, arr_elems_size);

			arr_elems[arr_nelems] = value;
			arr_nelems++;
		}
		array_free_iterator(iterator);

		elog(DEBUG3, "   %s: adding new element: arr_elems[%ld]. arr_elems_size > %ld", __func__, arr_nelems, arr_elems_size);
		arr_elems[arr_nelems++] = CStringGetTextDatum(text_element);
	}

	elog(DEBUG2, "  %s => construct_array called: arr_nelems > %ld arr_elems_size %ld", __func__, arr_nelems, arr_elems_size);

	*array = construct_array(arr_elems, arr_nelems, elem_type, elem_len, elem_byval, elem_align);
}

static void OAIFdwReScanForeignScan(ForeignScanState *node)
{
}

static void OAIFdwEndForeignScan(ForeignScanState *node)
{
}

static List *OAIFdwImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	ListCell *cell;
	List *sql_commands = NIL;
	List *all_sets = NIL;
	char *format = "oai_dc";
	bool format_set = false;
	OAIFdwState *state;
	ForeignServer *server = GetForeignServer(serverOid);

	elog(DEBUG1, "%s called: '%s'", __func__, server->servername);
	state = GetServerInfo(server->servername);

	all_sets = GetSets(state);

	elog(DEBUG1, "  %s: parsing statements", __func__);

	foreach (cell, stmt->options)
	{
		DefElem *def = lfirst_node(DefElem, cell);
		if (strcmp(def->defname, OAI_NODE_METADATAPREFIX) == 0)
		{
			ListCell *cell_formats;
			List *formats = NIL;
			bool found = false;
			formats = GetMetadataFormats(state);

			foreach (cell_formats, formats)
			{
				OAIMetadataFormat *format = (OAIMetadataFormat *)lfirst(cell_formats);

				if (strcmp(format->metadataPrefix, defGetString(def)) == 0)
				{
					found = true;
				}
			}

			if (!found)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
						 errmsg("invalid 'metadataprefix': '%s'", defGetString(def))));
			}

			format = defGetString(def);
			format_set = true;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("invalid FOREIGN SCHEMA OPTION: '%s'", def->defname)));
		}
	}

	if (!format_set)
	{

		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("missing 'metadataprefix' OPTION."),
				 errhint("A OAI Foreign Table must have a fixed 'metadataprefix'. Execute 'SELECT * FROM OAI_ListMetadataFormats('%s')' to see which formats are offered in the OAI Repository.", server->servername)));
	}

	if (strcmp(stmt->remote_schema, "oai_sets") == 0)
	{
		List *tables = NIL;

		if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO)
		{
			ListCell *cell_limit_to;

			foreach (cell_limit_to, stmt->table_list)
			{
				RangeVar *rv = (RangeVar *)lfirst(cell_limit_to);
				OAISet *set = (OAISet *)palloc0(sizeof(OAISet));
				set->setSpec = rv->relname;

				tables = lappend(tables, set);
			}
		}
		else if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
		{
			ListCell *cell_sets;

			foreach (cell_sets, all_sets)
			{
				ListCell *cell_except;
				bool found = false;
				OAISet *set = (OAISet *)palloc0(sizeof(OAISet));
				set = (OAISet *)lfirst(cell_sets);

				foreach (cell_except, stmt->table_list)
				{
					RangeVar *rv = (RangeVar *)lfirst(cell_except);
					if (strcmp(rv->relname, set->setSpec) == 0)
					{
						found = true;
						break;
					}
				}

				if (!found)
					tables = lappend(tables, set);
			}
		}
		else if (stmt->list_type == FDW_IMPORT_SCHEMA_ALL)
		{
			tables = all_sets;
		}

		foreach (cell, tables)
		{
			StringInfoData buffer;
			OAISet *set = (OAISet *)lfirst(cell);
			initStringInfo(&buffer);

			appendStringInfo(&buffer, "\nCREATE FOREIGN TABLE %s (\n", quote_identifier(set->setSpec));
			appendStringInfo(&buffer, "  id text                OPTIONS (oai_node 'identifier'),\n");
			appendStringInfo(&buffer, "  xmldoc xml             OPTIONS (oai_node 'content'),\n");
			appendStringInfo(&buffer, "  sets text[]            OPTIONS (oai_node 'setspec'),\n");
			appendStringInfo(&buffer, "  updatedate timestamp   OPTIONS (oai_node 'datestamp'),\n");
			appendStringInfo(&buffer, "  format text            OPTIONS (oai_node 'metadataprefix'),\n");
			appendStringInfo(&buffer, "  status boolean         OPTIONS (oai_node 'status')\n");
			appendStringInfo(&buffer, ") SERVER %s OPTIONS (metadataPrefix '%s', setspec '%s');\n", server->servername, format, set->setSpec);

			sql_commands = lappend(sql_commands, pstrdup(buffer.data));

			elog(DEBUG1, "%s: IMPORT FOREIGN SCHEMA (%s): \n%s", __func__, stmt->remote_schema, buffer.data);
		}

		elog(NOTICE, "Foreign tables to be created in schema '%s': %d", stmt->local_schema, list_length(sql_commands));
	}
	else if (strcmp(stmt->remote_schema, "oai_repository") == 0)
	{
		StringInfoData buffer;
		initStringInfo(&buffer);

		appendStringInfo(&buffer, "\nCREATE FOREIGN TABLE %s_repository (\n", server->servername);
		appendStringInfo(&buffer, "  id text                OPTIONS (oai_node 'identifier'),\n");
		appendStringInfo(&buffer, "  xmldoc xml             OPTIONS (oai_node 'content'),\n");
		appendStringInfo(&buffer, "  sets text[]            OPTIONS (oai_node 'setspec'),\n");
		appendStringInfo(&buffer, "  updatedate timestamp   OPTIONS (oai_node 'datestamp'),\n");
		appendStringInfo(&buffer, "  format text            OPTIONS (oai_node 'metadataprefix'),\n");
		appendStringInfo(&buffer, "  status boolean         OPTIONS (oai_node 'status')\n");
		appendStringInfo(&buffer, ") SERVER %s OPTIONS (metadataPrefix '%s');\n", server->servername, format);

		sql_commands = lappend(sql_commands, pstrdup(buffer.data));

		elog(DEBUG1, "%s: IMPORT FOREIGN SCHEMA: \n%s", __func__, buffer.data);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_NAME),
				 errmsg("invalid FOREIGN SCHEMA: '%s'", stmt->remote_schema)));
	}

	return sql_commands;
}

/*
 * CreateDatum
 * ----------
 * 
 * Creates a Datum from a given value based on the postgres types and modifiers.
 * 
 * tuple: a Heaptuple 
 * pgtype: postgres type
 * pgtypemod: postgres type modifier
 * value: value to be converted
 *
 * returns Datum
 */
static Datum CreateDatum(HeapTuple tuple, int pgtype, int pgtypmod, char *value)
{
    regproc typinput;

    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtype));

    if (!HeapTupleIsValid(tuple))
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                 errmsg("cache lookup failed for type %u (osm_id)", pgtype)));
    }

    typinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;
    ReleaseSysCache(tuple);

    if (pgtype == FLOAT4OID ||
        pgtype == FLOAT8OID ||
        pgtype == NUMERICOID ||
        pgtype == TIMESTAMPOID ||
        pgtype == TIMESTAMPTZOID ||
        pgtype == VARCHAROID)
        return OidFunctionCall3(
            typinput,
            CStringGetDatum(value),
            ObjectIdGetDatum(InvalidOid),
            Int32GetDatum(pgtypmod));
    else
        return OidFunctionCall1(typinput, CStringGetDatum(value));
}

static void LoadOAIUserMapping(OAIFdwState *state)
{
	Datum		datum;
	HeapTuple	tp;
	bool		isnull;
	UserMapping *um;
	List *options = NIL;
	ListCell *cell;
	bool usermatch = true;

	elog(DEBUG1, "%s called", __func__);

	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(GetUserId()),
						 ObjectIdGetDatum(state->foreign_server->serverid));

	if (!HeapTupleIsValid(tp))
	{
		elog(DEBUG2, "%s: not found for the specific user -- try PUBLIC",__func__);
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(state->foreign_server->serverid));
	}

	if (!HeapTupleIsValid(tp))
	{
		elog(DEBUG2, "%s: user mapping not found for user \"%s\", server \"%s\"",
			 __func__, MappingUserName(GetUserId()), state->foreign_server->servername);

		usermatch = false;
	}

	if (usermatch)
	{
		elog(DEBUG2, "%s: setting UserMapping*", __func__);
		um = (UserMapping *)palloc(sizeof(UserMapping));
#if PG_VERSION_NUM < 120000
		um->umid = HeapTupleGetOid(tp);
#else
		um->umid = ((Form_pg_user_mapping)GETSTRUCT(tp))->oid;
#endif		
		um->userid = GetUserId();
		um->serverid = state->foreign_server->serverid;

		elog(DEBUG2, "%s: extract the umoptions", __func__);
		datum = SysCacheGetAttr(USERMAPPINGUSERSERVER,
								tp,
								Anum_pg_user_mapping_umoptions,
								&isnull);
		if (isnull)
			um->options = NIL;
		else
			um->options = untransformRelOptions(datum);

		if (um->options != NIL)
		{
			options = list_concat(options, um->options);

			foreach (cell, options)
			{
				DefElem *def = (DefElem *)lfirst(cell);

				if (strcmp(def->defname, OAI_USERMAPPING_OPTION_USER) == 0)
				{
					state->user = pstrdup(strVal(def->arg));
					elog(DEBUG1, "%s: %s '%s'", __func__, def->defname, state->user);
				}

				if (strcmp(def->defname, OAI_USERMAPPING_OPTION_PASSWORD) == 0)
				{					
					state->password = pstrdup(strVal(def->arg));
					elog(DEBUG1, "%s: %s '*******'", __func__, def->defname);
				}
			}
		}

		ReleaseSysCache(tp);
	}

}

static void InitSession(OAIFdwState *state, RelOptInfo *baserel)
{
	ListCell *cell;

	state->requestRedirect = false;
	state->requestMaxRedirect = 0;

	elog(DEBUG1, "%s called", __func__);

	foreach (cell, state->foreign_server->options)
	{
		DefElem *def = lfirst_node(DefElem, cell);

		if (strcmp(OAI_NODE_URL, def->defname) == 0)
		{
			state->url = defGetString(def);
		}
		else if (strcmp(OAI_NODE_METADATAPREFIX, def->defname) == 0)
		{
			state->metadataPrefix = defGetString(def);
		}
		else if (strcmp(OAI_NODE_HTTP_PROXY, def->defname) == 0)
		{
			state->proxy = defGetString(def);
			state->proxyType = OAI_NODE_HTTP_PROXY;
		}
		else if (strcmp(OAI_NODE_HTTPS_PROXY, def->defname) == 0)
		{
			state->proxy = defGetString(def);
			state->proxyType = OAI_NODE_HTTPS_PROXY;
		}
		else if (strcmp(OAI_NODE_PROXY_USER, def->defname) == 0)
		{
			state->proxyUser = defGetString(def);
		}
		else if (strcmp(OAI_NODE_PROXY_USER_PASSWORD, def->defname) == 0)
		{
			state->proxyUserPassword = defGetString(def);
		}
		else if (strcmp(OAI_NODE_CONNECTTIMEOUT, def->defname) == 0)
		{
			char *tailpt;
			char *timeout_str = defGetString(def);
			state->connectTimeout = strtol(timeout_str, &tailpt, 0);
		}
		else if (strcmp(OAI_NODE_CONNECTRETRY, def->defname) == 0)
		{
			char *tailpt;
			char *maxretry_str = defGetString(def);
			state->maxretries = strtol(maxretry_str, &tailpt, 0);
		}
		else if (strcmp(OAI_NODE_REQUEST_REDIRECT, def->defname) == 0)
		{
			state->requestRedirect = defGetBoolean(def);
		}
		else if (strcmp(OAI_NODE_REQUEST_MAX_REDIRECT, def->defname) == 0)
		{
			char *tailpt;
			char *maxredirect_str = defGetString(def);
			state->requestMaxRedirect = strtol(maxredirect_str, &tailpt, 0);
		}
		else
		{
			elog(WARNING, "Invalid SERVER OPTION > '%s'", def->defname);
		}
	}

	foreach (cell, state->foreign_table->options)
	{
		DefElem *def = lfirst_node(DefElem, cell);

		if (strcmp(OAI_NODE_METADATAPREFIX, def->defname) == 0)
		{
			state->metadataPrefix = defGetString(def);
		}
		else if (strcmp(OAI_NODE_SETSPEC, def->defname) == 0)
		{
			state->set = defGetString(def);
		}
		else if (strcmp(OAI_NODE_FROM, def->defname) == 0)
		{
			state->from = defGetString(def);
		}
		else if (strcmp(OAI_NODE_UNTIL, def->defname) == 0)
		{
			state->until = defGetString(def);
		}
	}

	LoadOAIUserMapping(state);

	OAIRequestPlanner(state, baserel);
}