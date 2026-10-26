/**********************************************************************
 *
 * oai_fdw -  PostgreSQL Foreign Data Wrapper for OAI-PMH Repositories
 *
 * oai_fdw is free software: you can redistribute it and/or modify
 * it under the terms of the MIT Licence.
 *
 * Copyright (C) 2021-2026 Jim Jones <jim.jones@uni-muenster.de>
 *
 **********************************************************************/

#include "postgres.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "optimizer/pathnode.h"
#include "optimizer/clauses.h"
#include "executor/executor.h"
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

#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <commands/explain.h>
#include <libxml/tree.h>
#include <libxml/parser.h>
#include <libxml/xmlerror.h>
#include <catalog/pg_collation.h>
#include <funcapi.h>
#include "lib/stringinfo.h"
#include "storage/latch.h"
#if PG_VERSION_NUM >= 160000
#include "utils/wait_event.h"
#else
#include "pgstat.h"
#endif
#include <utils/lsyscache.h>
#include "nodes/pg_list.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#if PG_VERSION_NUM < 180000
#include "nodes/bitmapset.h" /* Needed for bms_is_empty in versions where it's inline */
#endif
#include "utils/datetime.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "utils/formatting.h"
#include "catalog/pg_operator.h"
#include "utils/syscache.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "access/reloptions.h"
#include "catalog/pg_namespace.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "mb/pg_wchar.h"

#define OAI_FDW_VERSION "1.15-dev"
#define OAI_REQUEST_LISTRECORDS "ListRecords"
#define OAI_REQUEST_LISTIDENTIFIERS "ListIdentifiers"
#define OAI_REQUEST_IDENTIFY "Identify"
#define OAI_REQUEST_GETRECORD "GetRecord"
#define OAI_REQUEST_LISTMETADATAFORMATS "ListMetadataFormats"
#define OAI_REQUEST_LISTSETS "ListSets"

#define OAI_DEFAULT_REQUEST_TIMEOUT 0
#define OAI_DEFAULT_CONNECT_TIMEOUT 300
#define OAI_DEFAULT_MAX_RETRY 3
/*
 * A transfer that receives less than one byte per second for this many
 * seconds is aborted, and retried like any other failure.
 */
#define OAI_FDW_STALL_TIMEOUT 300
/*
 * Maximum number of bytes from an HTTP error response body to include in
 * error messages and server logs.  Prevents huge HTML error pages (e.g.
 * from misconfigured proxies) from flooding PostgreSQL logs.
 */
#define OAI_FDW_MAX_ERROR_BODY 512

#define OAI_USERMAPPING_OPTION_USER "user"
#define OAI_USERMAPPING_OPTION_PASSWORD "password"
#define OAI_USERMAPPING_OPTION_PROXY_USER "proxy_user"
#define OAI_USERMAPPING_OPTION_PROXY_PASSWORD "proxy_password"

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
#define OAI_NODE_URL "url"
#define OAI_SERVER_OPTION_HTTP_PROXY "http_proxy"
#define OAI_SERVER_OPTION_CONNECT_TIMEOUT "connect_timeout"
#define OAI_SERVER_OPTION_REQUEST_TIMEOUT "request_timeout"
#define OAI_SERVER_OPTION_CONNECTRETRY "connect_retry"
#define OAI_SERVER_OPTION_REQUEST_REDIRECT "request_redirect"
#define OAI_SERVER_OPTION_REQUEST_MAX_REDIRECT "request_max_redirect"
#define OAI_NODE_IDENTIFIER "identifier"
#define OAI_NODE_CONTENT "content"
#define OAI_NODE_DATESTAMP "datestamp"
#define OAI_NODE_SETSPEC "setspec"
#define OAI_NODE_METADATAPREFIX "metadataprefix"
#define OAI_NODE_FROM "from"
#define OAI_NODE_UNTIL "until"
#define OAI_NODE_STATUS "status"
#define OAI_NODE_COLUMN_OPTION "oai_node"
#define OAI_ERROR_ID_DOES_NOT_EXIST "idDoesNotExist"
#define OAI_ERROR_NO_RECORD_MATCH "noRecordsMatch"
#define OAI_ERROR_NO_SET_HIERARCHY "noSetHierarchy"
#define OAI_ERROR_NO_METADATA_FORMATS "noMetadataFormats"

#define OAI_SUCCESS 0
#define OAI_FAIL 1
#define OAI_UNKNOWN_REQUEST 2

/*
 * OAI-PMH flow control (protocol spec, section 3.4): a repository may answer
 * "503 Service Unavailable" together with a "Retry-After" header to throttle a
 * harvester. Retrying such a response immediately is both useless and abusive,
 * so the wait is honoured before the next attempt.
 *
 * OAI_FDW_MAX_RETRY_AFTER caps how long a single wait may last, so that a
 * repository asking for an implausible delay cannot pin a backend down
 * indefinitely. Without a usable Retry-After header, the wait starts at
 * OAI_FDW_DEFAULT_RETRY_AFTER and doubles with every attempt.
 */
/*
 * WL_EXIT_ON_PM_DEATH only exists from PostgreSQL 12 on; on 11 the caller has
 * to watch for postmaster death explicitly.
 */
#if PG_VERSION_NUM >= 120000
#define OAI_WAIT_LATCH_FLAGS (WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH)
#else
#define OAI_WAIT_LATCH_FLAGS (WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH)
#endif

#define OAI_HTTP_SERVICE_UNAVAILABLE 503
#define OAI_HTTP_REQUEST_TIMEOUT 408
#define OAI_HTTP_TOO_MANY_REQUESTS 429
#define OAI_FDW_MAX_RETRY_AFTER 300
#define OAI_FDW_DEFAULT_RETRY_AFTER 5
#define IntToConst(x) makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)(x)), false, true)
#define OidToConst(x) makeConst(OIDOID, -1, InvalidOid, 4, ObjectIdGetDatum(x), false, true)

/* list API has changed in v13 */
#if PG_VERSION_NUM < 130000
#define list_next(l, e) lnext((e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (element))
#else
#define list_next(l, e) lnext((l), (e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (list), (element))
#endif /* PG_VERSION_NUM */

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
	long request_timeout;	 /* Timeout for the entire HTTP request (connect + transfer) */
	char *identifier;		 /* The unique identifier of an item in a repository. */
	char *set;				 /* The set membership of the item for the purpose of selective harvesting. */
	char *url;				 /* Concatenated URL with the OAI request. */
	char *metadataPrefix;	 /* Metadata format in OAI requests issued to the repository. */
	char *proxy;			 /* Proxy for HTTP requests, if necessary. */
	char *proxyType;		 /* Proxy protocol (HTTPS, HTTP). */
	char *proxyUser;		 /* User name for proxy authentication. */
	char *proxyPassword;	 /* Password for proxy authentication. */
	char *from;				 /* Beginning of am interval to filter an OAI request. */
	char *until;			 /* End of an interval to filter an OAI request. */
	char *resumptionToken;	 /* Token to retrieve the next page of a result set. */
	char *requestVerb;		 /* Type of OAI request (GetRecord, ListRecords, ListIdentifiers,
								Identify, ListSets, ListMetadataFormats. */
	Oid foreigntableid;
	xmlDocPtr xmldoc;	  /* Result of an OAI request. */
	MemoryContext oaicxt; /* Memory Context for data manipulation. */
	MemoryContext tokencxt; /* Holds the resumption token across a reset of oaicxt. */
	List *records;		  /* List of OAI records retrieved. */
	int pageindex;		  /* Index of a record within a retrieved page (list). */
	int pagesize;		  /* Number of OAI records retrieved. */
	ForeignTable *foreign_table;
	ForeignServer *foreign_server;
	char *user;
	char *password;
	Cost startup_cost;
	Cost total_cost;

	struct OAIfdwTable *oaiTable; /* All necessary information of the FOREIGN TABLE used in a SQL statement */

	/*
	 * Conditions whose value is only known at execution time, e.g. $1 or
	 * now(): what they set in the request (the expressions are in
	 * ForeignScan.fdw_exprs).
	 */
	List *pushdown_exprs;
	List *pushdown_kinds;
	List *pushdown_states;			 /* ExprStates of fdw_exprs */
	bool pushdown_evaluated;		 /* evaluated for the current scan */
	bool pushdown_norows;			 /* a value is NULL: nothing can match */
	MemoryContext pushdowncxt;		 /* values derived from them */
	struct OAIRequestArgs *planArgs; /* request arguments set at planning */
} OAIFdwState;

/* The request arguments that conditions can set. */
typedef struct OAIRequestArgs
{
	char *requestVerb;
	char *identifier;
	char *metadataPrefix;
	char *set;
	char *from;
	char *until;
} OAIRequestArgs;

/* What a pushed down condition sets in the request. */
typedef enum OAIPushdownKind
{
	OAI_PUSHDOWN_NONE,
	OAI_PUSHDOWN_IDENTIFIER,
	OAI_PUSHDOWN_METADATAPREFIX,
	OAI_PUSHDOWN_DATESTAMP, /* '=': from and until */
	OAI_PUSHDOWN_FROM,
	OAI_PUSHDOWN_UNTIL,
	OAI_PUSHDOWN_SET
} OAIPushdownKind;

typedef struct OAIRecord
{
	char *identifier;
	char *content;
	char *datestamp;
	char *metadataPrefix;
	bool isDeleted;
	ArrayType *setsArray;
} OAIRecord;

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

/*
 * Datestamp granularity of a repository, as announced by Identify. Cached for
 * the lifetime of the backend, keyed by server and URL.
 */
typedef struct OAIGranularity
{
	Oid serverid;
	char *url;
	bool day; /* YYYY-MM-DD only */
} OAIGranularity;

static List *granularity_cache = NIL;

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
	/*
	 * Set by the libcurl callbacks when an allocation fails. The callbacks
	 * must not raise errors themselves (see WriteMemoryCallback), so the
	 * condition is recorded here and reported once the transfer has ended.
	 */
	bool alloc_failed;
	/*
	 * Content-type reported by the server when it is not one of the XML types
	 * oai_fdw expects; empty otherwise. Warned about after the transfer, for
	 * the same reason.
	 */
	char unsupported_ctype[128];
};

typedef struct OAIfdwTable
{
	char *name;					/* FOREIGN TABLE name */
	struct OAIfdwColumn **cols; /* List of columns of a FOREIGN TABLE */
} OAIfdwTable;

typedef struct OAIfdwColumn
{
	char *name;		/* Column name */
	char *oai_node; /* OAI node identifier */
	Oid pgtype;		/* PostgreSQL data type */
	int pgtypmod;	/* PostgreSQL type modifier */
	int pgattnum;	/* PostgreSQL attribute number */

} OAIfdwColumn;

static struct OAIFdwOption valid_options[] =
	{
		/* Foreign Server */
		{OAI_NODE_URL, ForeignServerRelationId, true, false},
		{OAI_NODE_METADATAPREFIX, ForeignServerRelationId, false, false},
		{OAI_SERVER_OPTION_HTTP_PROXY, ForeignServerRelationId, false, false},
		{OAI_SERVER_OPTION_CONNECT_TIMEOUT, ForeignServerRelationId, false, false},
		{OAI_SERVER_OPTION_REQUEST_TIMEOUT, ForeignServerRelationId, false, false},
		{OAI_SERVER_OPTION_CONNECTRETRY, ForeignServerRelationId, false, false},
		{OAI_SERVER_OPTION_REQUEST_REDIRECT, ForeignServerRelationId, false, false},
		{OAI_SERVER_OPTION_REQUEST_MAX_REDIRECT, ForeignServerRelationId, false, false},

		/* Foreign Table */
		{OAI_NODE_IDENTIFIER, ForeignTableRelationId, false, false},
		{OAI_NODE_METADATAPREFIX, ForeignTableRelationId, true, false},
		{OAI_NODE_SETSPEC, ForeignTableRelationId, false, false},
		{OAI_NODE_FROM, ForeignTableRelationId, false, false},
		{OAI_NODE_UNTIL, ForeignTableRelationId, false, false},

		/* Column OPTIONS */
		{OAI_NODE_COLUMN_OPTION, AttributeRelationId, true, false},

		/* User Mapping */
		{OAI_USERMAPPING_OPTION_USER, UserMappingRelationId, true, false},
		{OAI_USERMAPPING_OPTION_PASSWORD, UserMappingRelationId, false, false},
		{OAI_USERMAPPING_OPTION_PROXY_USER, UserMappingRelationId, false, false},
		{OAI_USERMAPPING_OPTION_PROXY_PASSWORD, UserMappingRelationId, false, false},

		/* EOList option */
		{NULL, InvalidOid, false, false}};

#define option_count (sizeof(valid_options) / sizeof(struct OAIFdwOption))

extern Datum oai_fdw_handler(PG_FUNCTION_ARGS);
extern Datum oai_fdw_validator(PG_FUNCTION_ARGS);
extern Datum oai_fdw_version(PG_FUNCTION_ARGS);
extern Datum oai_fdw_settings(PG_FUNCTION_ARGS);
extern Datum oai_fdw_listMetadataFormats(PG_FUNCTION_ARGS);
extern Datum oai_fdw_listSets(PG_FUNCTION_ARGS);
extern Datum oai_fdw_identity(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(oai_fdw_handler);
PG_FUNCTION_INFO_V1(oai_fdw_validator);
PG_FUNCTION_INFO_V1(oai_fdw_version);
PG_FUNCTION_INFO_V1(oai_fdw_settings);
PG_FUNCTION_INFO_V1(oai_fdw_listMetadataFormats);
PG_FUNCTION_INFO_V1(oai_fdw_listSets);
PG_FUNCTION_INFO_V1(oai_fdw_identity);

static void OAIFdwGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void OAIFdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *OAIFdwGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
static void OAIFdwBeginForeignScan(ForeignScanState *node, int eflags);
static void OAIExplainForeignScan(ForeignScanState *node, ExplainState *es);
static TupleTableSlot *OAIFdwIterateForeignScan(ForeignScanState *node);
static void OAIFdwReScanForeignScan(ForeignScanState *node);
static void OAIFdwEndForeignScan(ForeignScanState *node);
static TupleTableSlot *OAIFdwExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *OAIFdwExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *OAIFdwExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static List *OAIFdwImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
static char *SetTableName(const char *setSpec);

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
static char *deparseDatestampConst(Const *constant);
static char *deparseTextConst(Const *constant);
static OAIPushdownKind GetPushdownKind(const char *operName, const char *oaiNode, Oid vartype);
static void ApplyPushdown(OAIFdwState *state, OAIPushdownKind kind, Const *constant);
static bool EvaluatePushdownExpressions(ForeignScanState *node, OAIFdwState *state);
static int CheckURL(char *url);
static bool IsUTCdatetime(const char *value);
static long ParseRetryAfter(const char *headers);
static void OAIWaitRetryAfter(long seconds, const char *servername);
static void OAIFreeXmlDoc(OAIFdwState *state);
static char *OAIToServer(const xmlChar *str);
static char *OAINodeText(xmlNodePtr node);
static void AppendOAIArgument(StringInfo buf, CURL *curl, const char *name, const char *value);
static bool HasDayGranularity(OAIFdwState *state);
static void NormalizeDatestampArguments(OAIFdwState *state);
static OAIFdwState *GetOAIServerByName(const char *srvname);
static List *GetMetadataFormats(OAIFdwState *state);
static List *GetIdentity(OAIFdwState *state);
static List *GetSets(OAIFdwState *state);
static void RaiseOAIException(xmlNodePtr error);
static void CheckOAIResponse(OAIFdwState *state);
static Datum CreateDatum(int pgtype, int pgtypmod, char *value);
static void LoadOAIServerInfo(OAIFdwState *state);
static void LoadOAITableInfo(OAIFdwState *state);
static void LoadOAIUserMapping(OAIFdwState *state, Oid userid);
static void InitSession(OAIFdwState *state, RelOptInfo *baserel);
static List *SerializePlanData(OAIFdwState *state);
static struct OAIFdwState *DeserializePlanData(List *list);
static Const *CStringToConst(const char *str);
static char *ConstToCString(Const *constant);
void _PG_init(void);

void _PG_init(void)
{
	/*
	 * Initialize libcurl's global state once per backend process.
	 * Intentionally no matching _PG_fini()/curl_global_cleanup(): this is a
	 * single-threaded, long-lived backend process that may share the address
	 * space with other libcurl-using extensions (e.g. rdf_fdw), and
	 * _PG_fini() is not guaranteed to run on backend exit anyway. Global
	 * state is reclaimed by the OS when the backend process terminates.
	 */
	if (curl_global_init(CURL_GLOBAL_ALL) != CURLE_OK)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("oai_fdw: could not initialise libcurl")));

	xmlInitParser();
}

Datum oai_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	fdwroutine->GetForeignRelSize = OAIFdwGetForeignRelSize;
	fdwroutine->GetForeignPaths = OAIFdwGetForeignPaths;
	fdwroutine->GetForeignPlan = OAIFdwGetForeignPlan;
	fdwroutine->BeginForeignScan = OAIFdwBeginForeignScan;
	fdwroutine->ExplainForeignScan = OAIExplainForeignScan;
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
	curl_version_info_data *ver = curl_version_info(CURLVERSION_NOW);

	initStringInfo(&buffer);

	appendStringInfo(&buffer, "oai_fdw %s (PostgreSQL %s",
					 OAI_FDW_VERSION,
					 PG_VERSION);

#ifdef OAI_FDW_CC
	appendStringInfo(&buffer, ", compiled by %s", OAI_FDW_CC);
#endif

	appendStringInfo(&buffer, ", libxml %s, libcurl %s)",
					 LIBXML_DOTTED_VERSION,
					 ver->version);

	PG_RETURN_TEXT_P(cstring_to_text(buffer.data));
}

Datum oai_fdw_settings(PG_FUNCTION_ARGS)
{
	StringInfoData buffer;
	curl_version_info_data *ver = curl_version_info(CURLVERSION_NOW);

	initStringInfo(&buffer);

	appendStringInfo(&buffer, "oai_fdw %s,", OAI_FDW_VERSION);
	appendStringInfo(&buffer, "PostgreSQL %s,", PG_VERSION);
	appendStringInfo(&buffer, "libxml %s,", LIBXML_DOTTED_VERSION);
	appendStringInfo(&buffer, "libcurl %s,", ver->version);

	if (ver->ssl_version)
		appendStringInfo(&buffer, "ssl %s,", ver->ssl_version);
	if (ver->libz_version)
		appendStringInfo(&buffer, "zlib %s,", ver->libz_version);
	if (ver->libssh_version)
		appendStringInfo(&buffer, "libSSH %s,", ver->libssh_version);
#if LIBCURL_VERSION_NUM >= 0x074200 /* 7.66.0 added nghttp2_version */
	if (ver->nghttp2_version)
		appendStringInfo(&buffer, "nghttp2 %s,", ver->nghttp2_version);
#endif

#ifdef OAI_FDW_CC
	appendStringInfo(&buffer, "compiled by %s,", OAI_FDW_CC);
#endif

#ifdef OAI_FDW_BUILD_DATE
	appendStringInfo(&buffer, "built on %s", OAI_FDW_BUILD_DATE);
#endif

	PG_RETURN_TEXT_P(cstring_to_text(buffer.data));
}

/*
 * GetOAIServerByName
 * ------------------
 * Looks up an oai_fdw server by name for the support functions and IMPORT
 * FOREIGN SCHEMA, checks that the current user may use it, and loads its
 * options.
 */
OAIFdwState *GetOAIServerByName(const char *srvname)
{
	OAIFdwState *state = (OAIFdwState *)palloc0(sizeof(OAIFdwState));
	ForeignServer *server = GetForeignServerByName(srvname, true);
	ForeignDataWrapper *fdw;
	AclResult aclresult;

	elog(DEBUG2, "%s called: '%s'", __func__, srvname);

	if (!server)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
				 errmsg("FOREIGN SERVER does not exist: '%s'", srvname)));

	fdw = GetForeignDataWrapper(server->fdwid);

	if (!OidIsValid(fdw->fdwhandler) ||
		GetFdwRoutine(fdw->fdwhandler)->GetForeignRelSize != OAIFdwGetForeignRelSize)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_HANDLE),
				 errmsg("FOREIGN SERVER '%s' does not belong to oai_fdw", srvname)));

#if PG_VERSION_NUM >= 160000
	aclresult = object_aclcheck(ForeignServerRelationId, server->serverid, GetUserId(), ACL_USAGE);
#else
	aclresult = pg_foreign_server_aclcheck(server->serverid, GetUserId(), ACL_USAGE);
#endif
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER, server->servername);

	state->foreign_server = server;
	LoadOAIServerInfo(state);

	return state;
}

Datum oai_fdw_identity(PG_FUNCTION_ARGS)
{
	OAIFdwState *state;
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

		state = GetOAIServerByName(text_to_cstring(PG_GETARG_TEXT_PP(0)));

		/*
		 * Loading USER MAPPING (if any)
		 */
		LoadOAIUserMapping(state, GetUserId());

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
		int natts = funcctx->attinmeta->tupdesc->natts;
		Datum *values = (Datum *)palloc(natts * sizeof(Datum));
		bool *nulls = (bool *)palloc0(natts * sizeof(bool));
		HeapTuple tuple;
		Datum result;
		OAIFdwIdentityNode *identity_node = (OAIFdwIdentityNode *)list_nth((List *)funcctx->user_fctx, call_cntr);

		for (int i = 0; i < natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(funcctx->attinmeta->tupdesc, i);

			if (strcmp(NameStr(att->attname), "name") == 0)
				values[i] = CreateDatum(att->atttypid, att->atttypmod, identity_node->name);
			else if (strcmp(NameStr(att->attname), "description") == 0 && identity_node->description)
				values[i] = CreateDatum(att->atttypid, att->atttypmod, identity_node->description);
			else
				nulls[i] = true;
		}

		elog(DEBUG2, "  %s: creating heap tuple", __func__);

		tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}

Datum oai_fdw_listSets(PG_FUNCTION_ARGS)
{

	OAIFdwState *state;

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

		state = GetOAIServerByName(text_to_cstring(PG_GETARG_TEXT_PP(0)));

		/*
		 * Loading USER MAPPING (if any)
		 */
		LoadOAIUserMapping(state, GetUserId());

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
		int natts = funcctx->attinmeta->tupdesc->natts;
		Datum *values = (Datum *)palloc(natts * sizeof(Datum));
		bool *nulls = (bool *)palloc0(natts * sizeof(bool));
		HeapTuple tuple;
		Datum result;
		OAISet *set_node = (OAISet *)list_nth((List *)funcctx->user_fctx, call_cntr);

		for (int i = 0; i < natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(funcctx->attinmeta->tupdesc, i);

			if (strcmp(NameStr(att->attname), "setname") == 0 && set_node->setName)
				values[i] = CreateDatum(att->atttypid, att->atttypmod, set_node->setName);
			else if (strcmp(NameStr(att->attname), "setspec") == 0)
				values[i] = CreateDatum(att->atttypid, att->atttypmod, set_node->setSpec);
			else
				nulls[i] = true;
		}

		elog(DEBUG2, "  %s: creating heap tuple", __func__);

		tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}

Datum oai_fdw_listMetadataFormats(PG_FUNCTION_ARGS)
{
	OAIFdwState *state;

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

		state = GetOAIServerByName(text_to_cstring(PG_GETARG_TEXT_PP(0)));

		/*
		 * Loading USER MAPPING (if any)
		 */
		LoadOAIUserMapping(state, GetUserId());

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
		int natts = funcctx->attinmeta->tupdesc->natts;
		Datum *values = (Datum *)palloc(natts * sizeof(Datum));
		bool *nulls = (bool *)palloc0(natts * sizeof(bool));
		HeapTuple tuple;
		Datum result;
		OAIMetadataFormat *format = (OAIMetadataFormat *)list_nth((List *)funcctx->user_fctx, call_cntr);

		for (int i = 0; i < natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(funcctx->attinmeta->tupdesc, i);

			if (strcmp(NameStr(att->attname), "metadataprefix") == 0)
				values[i] = CreateDatum(att->atttypid, att->atttypmod, format->metadataPrefix);
			else if (strcmp(NameStr(att->attname), "schema") == 0 && format->schema)
				values[i] = CreateDatum(att->atttypid, att->atttypmod, format->schema);
			else if (strcmp(NameStr(att->attname), "metadatanamespace") == 0 && format->metadataNamespace)
				values[i] = CreateDatum(att->atttypid, att->atttypmod, format->metadataNamespace);
			else
				nulls[i] = true;
		}

		elog(DEBUG2, "  %s: creating heap tuple", __func__);

		tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else /* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
}

Datum oai_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	ListCell *cell;
	struct OAIFdwOption *opt;

	/* Initialize found state to not found */
	for (opt = valid_options; opt->optname; opt++)
		opt->optfound = false;

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
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							 errmsg("empty value in option '%s'", opt->optname)));

				if (strcmp(opt->optname, OAI_NODE_URL) == 0 || strcmp(opt->optname, OAI_SERVER_OPTION_HTTP_PROXY) == 0)
				{
					int return_code = CheckURL(defGetString(def));

					/* requests are restricted to http and https (CURLOPT_PROTOCOLS_STR) */
					if (return_code == OAI_SUCCESS && strcmp(opt->optname, OAI_NODE_URL) == 0 &&
						pg_strncasecmp(defGetString(def), "http://", 7) != 0 &&
						pg_strncasecmp(defGetString(def), "https://", 8) != 0)
						return_code = OAI_FAIL;

					if (return_code != OAI_SUCCESS)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: '%s'", opt->optname, defGetString(def))));
				}

				if (strcmp(opt->optname, OAI_SERVER_OPTION_CONNECT_TIMEOUT) == 0)
				{
					char *endptr;
					char *timeout_str = defGetString(def);
					long timeout_val = strtol(timeout_str, &endptr, 0);

					if (timeout_str[0] == '\0' || *endptr != '\0' || timeout_val < 0)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: %s", def->defname, timeout_str),
								 errhint("expected values are positive integers (timeout in seconds)")));
				}

				if (strcmp(opt->optname, OAI_SERVER_OPTION_REQUEST_TIMEOUT) == 0)
				{
					char *endptr;
					char *timeout_str = defGetString(def);
					long timeout_val = strtol(timeout_str, &endptr, 0);

					if (timeout_str[0] == '\0' || *endptr != '\0' || timeout_val < 0)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: %s", def->defname, timeout_str),
								 errhint("expected values are positive integers (timeout in seconds)")));
				}

				if (strcmp(opt->optname, OAI_SERVER_OPTION_CONNECTRETRY) == 0)
				{
					char *endptr;
					char *retry_str = defGetString(def);
					long retry_val = strtol(retry_str, &endptr, 0);

					if (retry_str[0] == '\0' || *endptr != '\0' || retry_val < 0)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: %s", def->defname, retry_str),
								 errhint("expected values are positive integers (retry attempts in case of failure)")));
				}

				if (strcmp(opt->optname, OAI_NODE_FROM) == 0 || strcmp(opt->optname, OAI_NODE_UNTIL) == 0)
				{
					if (!IsUTCdatetime(defGetString(def)))
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: %s", def->defname, defGetString(def)),
								 errhint("expected values are 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ssZ' (UTC)")));

					/* rejects dates that do not exist, e.g. 2020-02-30 */
					(void)DirectFunctionCall3(timestamp_in,
											  CStringGetDatum(defGetString(def)),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(-1));
				}

				if (strcmp(opt->optname, OAI_SERVER_OPTION_REQUEST_REDIRECT) == 0)
				{
					bool redirect;

					if (!parse_bool(defGetString(def), &redirect))
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: %s", def->defname, defGetString(def)),
								 errhint("expected values are 'true' or 'false'")));
				}

				if (strcmp(opt->optname, OAI_SERVER_OPTION_REQUEST_MAX_REDIRECT) == 0)
				{
					char *endptr;
					char *redirect_str = defGetString(def);
					long redirect_val = strtol(redirect_str, &endptr, 10);

					if (*endptr != '\0' || redirect_val < 0)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
								 errmsg("invalid %s: %s", def->defname, redirect_str),
								 errhint("expected values are positive integers (maximum number of redirects)")));
				}

				if (strcmp(opt->optname, OAI_NODE_COLUMN_OPTION) == 0)
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
								 errmsg("invalid %s option '%s'", OAI_NODE_COLUMN_OPTION, defGetString(def)),
								 errhint("Valid values for %s are '%s', '%s', '%s', '%s', '%s' and '%s'.",
										 OAI_NODE_COLUMN_OPTION,
										 OAI_NODE_IDENTIFIER,
										 OAI_NODE_CONTENT,
										 OAI_NODE_DATESTAMP,
										 OAI_NODE_SETSPEC,
										 OAI_NODE_METADATAPREFIX,
										 OAI_NODE_STATUS)));
					}
				}

				break;
			}
		}

		if (!optfound)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid oai_fdw option \"%s\"", def->defname)));
	}

	for (opt = valid_options; opt->optname; opt++)
	{
		/* Required option for this catalog type is missing? */
		if (catalog == opt->optcontext && opt->optrequired && !opt->optfound)
			ereport(ERROR, (
							   errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
							   errmsg("required option '%s' is missing", opt->optname)));
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

	elog(DEBUG2, "%s called", __func__);

	state->requestVerb = OAI_REQUEST_IDENTIFY;

	oaiExecuteResponse = ExecuteOAIRequest(state);

	if (oaiExecuteResponse == OAI_SUCCESS)
	{
		/* the document is not reclaimed on error, so it is released here */
		PG_TRY();
		{
			xmlNodePtr oai_root;
			xmlNodePtr Identity;
			xmlNodePtr xmlroot;

			CheckOAIResponse(state);
			xmlroot = xmlDocGetRootElement(state->xmldoc);

			for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next)
			{
				if (oai_root->type != XML_ELEMENT_NODE)
					continue;

				if (xmlStrcmp(oai_root->name, (xmlChar *)OAI_REQUEST_IDENTIFY) != 0)
					continue;

				for (Identity = oai_root->children; Identity != NULL; Identity = Identity->next)
				{
					OAIFdwIdentityNode *node;

					if (Identity->type != XML_ELEMENT_NODE)
						continue;

					node = (OAIFdwIdentityNode *)palloc0(sizeof(OAIFdwIdentityNode));
					node->name = pstrdup((char *)Identity->name);
					node->description = OAINodeText(Identity);
					result = lappend(result, node);
				}
			}
		}
		PG_CATCH();
		{
			OAIFreeXmlDoc(state);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	OAIFreeXmlDoc(state);

	elog(DEBUG2, "%s => finished", __func__);

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

	elog(DEBUG2, "%s called", __func__);

	state->requestVerb = OAI_REQUEST_LISTSETS;
	state->resumptionToken = NULL;

	/* the list may be split over several responses (spec 3.5) */
	do
	{
		oaiExecuteResponse = ExecuteOAIRequest(state);
		state->resumptionToken = NULL;

		if (oaiExecuteResponse == OAI_SUCCESS)
		{
			/* the document is not reclaimed on error, so it is released here */
			PG_TRY();
			{
				xmlNodePtr oai_root;
				xmlNodePtr ListSets;
				xmlNodePtr SetElement;
				xmlNodePtr xmlroot;

				CheckOAIResponse(state);
				xmlroot = xmlDocGetRootElement(state->xmldoc);

				for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next)
				{
					if (oai_root->type != XML_ELEMENT_NODE)
						continue;

					if (xmlStrcmp(oai_root->name, (xmlChar *)OAI_REQUEST_LISTSETS) != 0)
						continue;

					for (ListSets = oai_root->children; ListSets != NULL; ListSets = ListSets->next)
					{
						OAISet *set;

						if (ListSets->type != XML_ELEMENT_NODE)
							continue;

						if (xmlStrcmp(ListSets->name, (xmlChar *)OAI_RESPONSE_ELEMENT_RESUMPTIONTOKEN) == 0)
						{
							xmlChar *token = xmlNodeGetContent(ListSets);

							if (token && *token != '\0')
								state->resumptionToken = pstrdup((char *)token);
							if (token)
								xmlFree(token);
							continue;
						}

						if (xmlStrcmp(ListSets->name, (xmlChar *)"set") != 0)
							continue;

						set = (OAISet *)palloc0(sizeof(OAISet));

						for (SetElement = ListSets->children; SetElement != NULL; SetElement = SetElement->next)
						{
							if (SetElement->type != XML_ELEMENT_NODE)
								continue;

							if (xmlStrcmp(SetElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SETSPEC) == 0)
							{
								set->setSpec = OAINodeText(SetElement);
							}
							else if (xmlStrcmp(SetElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SETNAME) == 0)
							{
								set->setName = OAINodeText(SetElement);
							}
						}

						/* setSpec is mandatory; a set without it cannot be harvested */
						if (!set->setSpec)
						{
							elog(WARNING, "ignoring <set> without <setSpec> in %s response", state->requestVerb);
							continue;
						}

						result = lappend(result, set);
					}
				}
			}
			PG_CATCH();
			{
				OAIFreeXmlDoc(state);
				PG_RE_THROW();
			}
			PG_END_TRY();
		}

		OAIFreeXmlDoc(state);
	} while (state->resumptionToken);

	elog(DEBUG2, "%s => finished", __func__);

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

	elog(DEBUG2, "  %s called", __func__);

	state->requestVerb = OAI_REQUEST_LISTMETADATAFORMATS;

	oaiExecuteResponse = ExecuteOAIRequest(state);

	if (oaiExecuteResponse == OAI_SUCCESS)
	{
		/* the document is not reclaimed on error, so it is released here */
		PG_TRY();
		{
			xmlNodePtr oai_root;
			xmlNodePtr ListMetadataFormats;
			xmlNodePtr MetadataElement;
			xmlNodePtr xmlroot;

			CheckOAIResponse(state);
			xmlroot = xmlDocGetRootElement(state->xmldoc);

			for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next)
			{
				if (oai_root->type != XML_ELEMENT_NODE)
					continue;

				if (xmlStrcmp(oai_root->name, (xmlChar *)OAI_REQUEST_LISTMETADATAFORMATS) != 0)
					continue;

				for (ListMetadataFormats = oai_root->children; ListMetadataFormats != NULL; ListMetadataFormats = ListMetadataFormats->next)
				{
					OAIMetadataFormat *format;

					if (ListMetadataFormats->type != XML_ELEMENT_NODE)
						continue;
					if (xmlStrcmp(ListMetadataFormats->name, (xmlChar *)OAI_RESPONSE_ELEMENT_METADATAFORMAT) != 0)
						continue;

					format = (OAIMetadataFormat *)palloc0(sizeof(OAIMetadataFormat));

					for (MetadataElement = ListMetadataFormats->children; MetadataElement != NULL; MetadataElement = MetadataElement->next)
					{
						if (MetadataElement->type != XML_ELEMENT_NODE)
							continue;

						if (xmlStrcmp(MetadataElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_METADATAPREFIX) == 0)
						{
							format->metadataPrefix = OAINodeText(MetadataElement);
						}
						else if (xmlStrcmp(MetadataElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SCHEMA) == 0)
						{
							format->schema = OAINodeText(MetadataElement);
						}
						else if (xmlStrcmp(MetadataElement->name, (xmlChar *)OAI_RESPONSE_ELEMENT_METADATANAMESPACE) == 0)
						{
							format->metadataNamespace = OAINodeText(MetadataElement);
						}
					}

					if (!format->metadataPrefix)
					{
						elog(WARNING, "ignoring <metadataFormat> without <metadataPrefix> in %s response", state->requestVerb);
						continue;
					}

					result = lappend(result, format);
				}
			}
		}
		PG_CATCH();
		{
			OAIFreeXmlDoc(state);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	OAIFreeXmlDoc(state);

	elog(DEBUG2, "  %s => finished.", __func__);

	return result;
}

/*
 * IsUTCdatetime
 * -------------
 * Checks the formats OAI-PMH allows for 'from' and 'until' (spec 3.3.1):
 * YYYY-MM-DD and YYYY-MM-DDThh:mm:ssZ.
 */
static bool IsUTCdatetime(const char *value)
{
	const char *day = "dddd-dd-dd";
	const char *seconds = "dddd-dd-ddTdd:dd:ddZ";
	size_t len = strlen(value);
	const char *pattern = len == strlen(day) ? day : len == strlen(seconds) ? seconds : NULL;

	if (!pattern)
		return false;

	for (size_t i = 0; i < len; i++)
	{
		if (pattern[i] == 'd' ? !isdigit((unsigned char)value[i]) : value[i] != pattern[i])
			return false;
	}

	return true;
}

/**
 * Checks if a given URL is valid.
 */
static int CheckURL(char *url)
{

	CURLUcode code;
	CURLU *handler = curl_url();

	elog(DEBUG2, "%s called > '%s'", __func__, url);

	code = curl_url_set(handler, CURLUPART_URL, url, 0);

	curl_url_cleanup(handler);

	elog(DEBUG2, "  %s handler return code: %u", __func__, code);

	if (code != 0)
	{
		elog(DEBUG2, "%s: invalid URL (%u) > '%s'", __func__, code, url);

		return code;
	}

	return OAI_SUCCESS;
}

/*
 * WriteMemoryCallback / HeaderCallbackFunction
 * --------------------------------------------
 * These run inside curl_easy_perform(). They must therefore never raise a
 * PostgreSQL error: doing so longjmps straight out of libcurl, leaving its
 * handle and connection state inconsistent and skipping every cleanup below
 * the transfer. For the same reason they use malloc() rather than palloc(),
 * whose out-of-memory path throws.
 *
 * A failure is recorded in the MemoryStruct and signalled to libcurl by
 * returning a short count, which makes curl_easy_perform() fail cleanly with
 * CURLE_WRITE_ERROR; the caller then reports it as an ordinary error.
 */
static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t realsize = size * nmemb;
	struct MemoryStruct *mem = (struct MemoryStruct *)userp;
	char *ptr = realloc(mem->memory, mem->size + realsize + 1);

	if (!ptr)
	{
		mem->alloc_failed = true;
		return 0; /* abort the transfer */
	}

	mem->memory = ptr;
	memcpy(&(mem->memory[mem->size]), contents, realsize);
	mem->size += realsize;
	mem->memory[mem->size] = 0;

	return realsize;
}

static size_t HeaderCallbackFunction(char *contents, size_t size, size_t nmemb, void *userp)
{
	size_t nbytes = size * nmemb;
	struct MemoryStruct *mem = (struct MemoryStruct *)userp;
	char *ptr;
	char line[256];
	size_t linelen = Min(nbytes, sizeof(line) - 1);

	memcpy(line, contents, linelen);
	line[linelen] = '\0';

	while (linelen > 0 && (line[linelen - 1] == '\r' || line[linelen - 1] == '\n'))
		line[--linelen] = '\0';

	/*
	 * Record an unexpected content-type instead of warning from here: this
	 * runs inside libcurl, where raising an error is unsafe and even a WARNING
	 * would allocate. The caller emits the warning once the transfer is done.
	 * Only the final response counts: with redirects, every hop starts with
	 * a status line of its own.
	 */
	if (pg_strncasecmp(line, "HTTP/", 5) == 0)
		mem->unsupported_ctype[0] = '\0';
	else if (pg_strncasecmp(line, "content-type:", 13) == 0)
	{
		const char *value = line + 13;

		/* the whitespace before the value is optional (RFC 9110, 5.6.3) */
		while (*value == ' ' || *value == '\t')
			value++;

		if (pg_strncasecmp(value, "text/xml", 8) != 0 &&
			pg_strncasecmp(value, "application/xml", 15) != 0)
			strlcpy(mem->unsupported_ctype, line, sizeof(mem->unsupported_ctype));
	}

	ptr = realloc(mem->memory, mem->size + nbytes + 1);

	if (!ptr)
	{
		mem->alloc_failed = true;
		return 0; /* abort the transfer */
	}

	mem->memory = ptr;
	memcpy(&(mem->memory[mem->size]), contents, nbytes);
	mem->size += nbytes;
	mem->memory[mem->size] = 0;
	return nbytes;
}

/*
 * IsSensitiveHeader
 * -----------------
 * Returns the field-name of a sensitive HTTP header if `line` begins with
 * one, or NULL otherwise.  Comparison is case-insensitive (RFC 9110 §5.1).
 */
static const char *
IsSensitiveHeader(const char *line)
{
	static const struct
	{
		const char *name;
		size_t len;
	} sensitive_headers[] = {
		{"Authorization:", sizeof("Authorization:") - 1},
		{"Proxy-Authorization:", sizeof("Proxy-Authorization:") - 1},
		{NULL, 0}};

	for (int i = 0; sensitive_headers[i].name != NULL; i++)
	{
		if (strncasecmp(line, sensitive_headers[i].name,
						sensitive_headers[i].len) == 0)
			return sensitive_headers[i].name;
	}

	return NULL;
}

/*
 * CURLDebugCallback
 * -----------------
 * Custom libcurl debug callback. Routes all verbose output through
 * PostgreSQL's elog() at DEBUG3 level rather than writing directly to
 * stderr, and redacts Authorization headers so credentials are never
 * written to server logs.
 *
 * handle  : the curl handle (unused)
 * type    : category of the debug data
 * data    : pointer to the debug data (NOT null-terminated)
 * size    : number of bytes in data
 * userptr : user-supplied pointer (unused)
 */
static int
CURLDebugCallback(CURL *handle, curl_infotype type, char *data, size_t size, void *userptr)
{
	const char *prefix;
	StringInfoData buf;

	switch (type)
	{
	case CURLINFO_TEXT:
		prefix = "* ";
		break;
	case CURLINFO_HEADER_IN:
		prefix = "< ";
		break;
	case CURLINFO_HEADER_OUT:
		prefix = "> ";
		break;
	default:
		return 0; /* skip raw data blobs (bodies, SSL frames) */
	}

	/*
	 * curl's data pointer is NOT null-terminated, so copy it into a palloc'd
	 * buffer before using any string functions on it.
	 */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, data, (int)size);

	if (type == CURLINFO_HEADER_OUT)
	{
		/*
		 * CURLINFO_HEADER_OUT delivers the entire outgoing request header
		 * block (request line + all headers) as one multi-line chunk per
		 * invocation.  Split it line-by-line so each sensitive header can be
		 * redacted individually.
		 */
		char *pos = buf.data;

		while (*pos != '\0')
		{
			char *eol = pos + strcspn(pos, "\r\n");
			char saved = *eol;
			const char *match;

			*eol = '\0'; /* temporarily terminate the line */

			if (*pos != '\0') /* skip blank lines */
			{
				match = IsSensitiveHeader(pos);
				if (match)
					elog(DEBUG3, "[curl] > %s [REDACTED]", match);
				else
					elog(DEBUG3, "[curl] > %s", pos);
			}

			*eol = saved;
			pos = eol;
			while (*pos == '\r' || *pos == '\n')
				pos++;
		}
	}
	else
	{
		const char *match;

		/* Strip trailing CRLF for cleaner log output. */
		while (buf.len > 0 &&
			   (buf.data[buf.len - 1] == '\n' || buf.data[buf.len - 1] == '\r'))
			buf.data[--buf.len] = '\0';

		/*
		 * Redact sensitive response headers.  Informational text lines
		 * (CURLINFO_TEXT) are logged as-is — they never contain raw
		 * credential values.
		 */
		match = (type == CURLINFO_HEADER_IN) ? IsSensitiveHeader(buf.data) : NULL;
		if (match)
			elog(DEBUG3, "[curl] %s%s [REDACTED]", prefix, match);
		else
			elog(DEBUG3, "[curl] %s%s", prefix, buf.data);
	}

	pfree(buf.data);
	return 0;
}

/*
 * CURLProgressCallback
 * --------------------
 * Progress callback function for cURL requests. This allows us to
 * check for interruptions to immediatelly cancel the request.
 *
 * Like the other callbacks it must not raise an error: a pending cancel or
 * termination aborts the transfer instead, and the caller processes the
 * interrupt once the curl handle has been released.
 *
 * dltotal: Total bytes to download
 * dlnow: Bytes downloaded so far
 * ultotal: Total bytes to upload
 * ulnow: Bytes uploaded so far
 */
static int CURLProgressCallback(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow)
{
	if (InterruptPending && (QueryCancelPending || ProcDiePending))
		return 1; /* abort the transfer */

	return 0;
}

/**
 * Executes the HTTP request to the OAI repository using the
 * libcurl library.
 */
/*
 * OAIFreeXmlDoc
 * -------------
 * Releases the parsed response document and clears the pointer.
 *
 * libxml2 allocates these documents with malloc(), so they are invisible to
 * PostgreSQL's memory contexts and are not reclaimed when a query ends or a
 * transaction aborts - they have to be freed explicitly on every path.
 * Clearing the pointer keeps a later call from freeing it a second time.
 */
static void
OAIFreeXmlDoc(OAIFdwState *state)
{
	if (state->xmldoc)
	{
		xmlFreeDoc(state->xmldoc);
		state->xmldoc = NULL;
	}
}

/*
 * OAIToServer
 * -----------
 * OAI-PMH responses are UTF-8, and so is everything libxml2 returns.
 * Returns a palloc'd copy of str in the server encoding.
 */
static char *
OAIToServer(const xmlChar *str)
{
	const char *s = (const char *)str;
	char *converted = pg_any_to_server(s, strlen(s), PG_UTF8);

	return converted == s ? pstrdup(s) : converted;
}

/*
 * OAINodeText
 * -----------
 * Returns the text of a node in the server encoding, or NULL. libxml2's copy
 * is released before converting, as the conversion may fail.
 */
static char *
OAINodeText(xmlNodePtr node)
{
	xmlChar *content = xmlNodeGetContent(node);
	char *copy;

	if (!content)
		return NULL;

	copy = pstrdup((char *)content);
	xmlFree(content);

	return OAIToServer((xmlChar *)copy);
}

/*
 * AppendOAIArgument
 * -----------------
 * Appends "&name=value" to a request, with value converted from the server
 * encoding to UTF-8 (as OAI-PMH requires) and URL-encoded.
 */
static void
AppendOAIArgument(StringInfo buf, CURL *curl, const char *name, const char *value)
{
	char *utf8 = pg_server_to_any(value, strlen(value), PG_UTF8);
	char *encoded = curl_easy_escape(curl, utf8, 0);

	appendStringInfo(buf, "&%s=%s", name, encoded);
	curl_free(encoded);
}

/*
 * ParseRetryAfter
 * ---------------
 * Looks for a "Retry-After" header in the raw header block collected during a
 * request and returns the delay it asks for, in seconds.
 *
 * RFC 9110 section 10.2.3 permits either delta-seconds or an HTTP-date, and
 * both forms are accepted here. When a request followed redirects the header
 * block holds one set of headers per hop, so the last occurrence wins.
 *
 * Returns -1 when no usable Retry-After header is present.
 */
static long
ParseRetryAfter(const char *headers)
{
	static const char field[] = "retry-after:";
	const char *line;
	long result = -1;

	if (!headers)
		return -1;

	for (line = headers; *line != '\0';)
	{
		const char *eol = strpbrk(line, "\r\n");
		size_t len = eol ? (size_t)(eol - line) : strlen(line);

		if (len > sizeof(field) - 1 &&
			pg_strncasecmp(line, field, sizeof(field) - 1) == 0)
		{
			const char *v = line + sizeof(field) - 1;
			char value[128];
			size_t vlen;

			/* skip the optional whitespace between colon and field value */
			while (v < line + len && (*v == ' ' || *v == '\t'))
				v++;

			vlen = (size_t)(line + len - v);

			if (vlen > 0 && vlen < sizeof(value))
			{
				char *tail;
				long secs;

				memcpy(value, v, vlen);
				value[vlen] = '\0';

				/* delta-seconds */
				secs = strtol(value, &tail, 10);

				if (tail != value && *tail == '\0' && secs >= 0)
					result = secs;
				else
				{
					/* HTTP-date */
					time_t when = curl_getdate(value, NULL);

					if (when != (time_t)-1)
					{
						double diff = difftime(when, time(NULL));

						result = diff > 0 ? (long)diff : 0;
					}
				}
			}
		}

		if (!eol)
			break;

		line = eol + strspn(eol, "\r\n");
	}

	return result;
}

/*
 * OAIWaitRetryAfter
 * -----------------
 * Sleeps for the delay a repository requested through its Retry-After header.
 *
 * The wait is broken into one second slices driven by WaitLatch so that query
 * cancellation and backend termination stay responsive - a plain pg_usleep()
 * would ignore both until the whole delay had elapsed. Delays beyond
 * OAI_FDW_MAX_RETRY_AFTER are clamped, so that a repository cannot pin a
 * backend down for an arbitrary amount of time.
 */
static void
OAIWaitRetryAfter(long seconds, const char *servername)
{
	long remaining = seconds;

	if (remaining <= 0)
		return;

	if (remaining > OAI_FDW_MAX_RETRY_AFTER)
	{
		elog(WARNING, "'%s' asked to be retried after %ld seconds; waiting %d seconds instead",
			 servername, remaining, OAI_FDW_MAX_RETRY_AFTER);
		remaining = OAI_FDW_MAX_RETRY_AFTER;
	}

	elog(DEBUG1, "%s: honouring Retry-After from '%s': waiting %ld seconds",
		 __func__, servername, remaining);

	while (remaining > 0)
	{
		int rc;

		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch, OAI_WAIT_LATCH_FLAGS, 1000L, PG_WAIT_EXTENSION);

		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);

		remaining--;
	}

	CHECK_FOR_INTERRUPTS();
}

static int ExecuteOAIRequest(OAIFdwState *state)
{

	CURL *curl;
	CURLcode res;
	StringInfoData url_buffer;
	StringInfoData request_url;
	StringInfoData user_agent;
	char errbuf[CURL_ERROR_SIZE];
	struct MemoryStruct chunk;
	struct MemoryStruct chunk_header;
	long maxretries = state->maxretries;
	long connectTimeout = OAI_DEFAULT_CONNECT_TIMEOUT;
	long request_timeout = OAI_DEFAULT_REQUEST_TIMEOUT;

	struct curl_slist *headers = NULL;
	char *parse_error = NULL;
	long final_code = 0;

	if (state->connectTimeout)
		connectTimeout = state->connectTimeout;

	if (state->request_timeout)
		request_timeout = state->request_timeout;

	/*
	 * These buffers are filled by libcurl callbacks, so they are malloc'd
	 * rather than palloc'd; see WriteMemoryCallback. They are released on
	 * every exit path below.
	 */
	memset(&chunk, 0, sizeof(chunk));
	memset(&chunk_header, 0, sizeof(chunk_header));
	chunk.memory = malloc(1);
	chunk_header.memory = malloc(1);

	if (!chunk.memory || !chunk_header.memory)
	{
		free(chunk.memory);
		free(chunk_header.memory);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("%s: out of memory", __func__)));
	}

	chunk.memory[0] = '\0';
	chunk_header.memory[0] = '\0';

	initStringInfo(&url_buffer);

	elog(DEBUG2, "%s called: base url > '%s' ", __func__, state->url);

	curl = curl_easy_init();

	if (!curl)
	{
		free(chunk.memory);
		free(chunk_header.memory);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("%s: failed to initialize curl", __func__)));
	}

	appendStringInfo(&url_buffer, "verb=%s", state->requestVerb);

	if (strcmp(state->requestVerb, OAI_REQUEST_LISTRECORDS) == 0)
	{
		if (state->set)
			AppendOAIArgument(&url_buffer, curl, "set", state->set);

		if (state->from)
			AppendOAIArgument(&url_buffer, curl, "from", state->from);

		if (state->until)
			AppendOAIArgument(&url_buffer, curl, "until", state->until);

		if (state->metadataPrefix)
			AppendOAIArgument(&url_buffer, curl, "metadataPrefix", state->metadataPrefix);

		if (state->resumptionToken)
		{
			char *encoded_token;

			elog(DEBUG2, "  %s (%s): appending 'resumptionToken' > %s", __func__, state->requestVerb, state->resumptionToken);
			resetStringInfo(&url_buffer);

			/* URL-encode the resumption token to handle special characters like & */
			encoded_token = curl_easy_escape(curl, state->resumptionToken, 0);
			if (encoded_token)
			{
				elog(DEBUG2, "  %s (%s): encoded resumptionToken > %s", __func__, state->requestVerb, encoded_token);
				appendStringInfo(&url_buffer, "verb=%s&resumptionToken=%s", state->requestVerb, encoded_token);
				curl_free(encoded_token);
			}
			else
			{
				/* Fallback to unencoded if encoding fails */
				elog(DEBUG2, "  %s (%s): encoding failed, using raw token", __func__, state->requestVerb);
				appendStringInfo(&url_buffer, "verb=%s&resumptionToken=%s", state->requestVerb, state->resumptionToken);
			}
		}
	}
	else if (strcmp(state->requestVerb, OAI_REQUEST_GETRECORD) == 0)
	{

		if (state->identifier)
			AppendOAIArgument(&url_buffer, curl, "identifier", state->identifier);

		if (state->metadataPrefix)
			AppendOAIArgument(&url_buffer, curl, "metadataPrefix", state->metadataPrefix);
	}
	else if (strcmp(state->requestVerb, OAI_REQUEST_LISTIDENTIFIERS) == 0)
	{

		if (state->set)
			AppendOAIArgument(&url_buffer, curl, "set", state->set);

		if (state->from)
			AppendOAIArgument(&url_buffer, curl, "from", state->from);

		if (state->until)
			AppendOAIArgument(&url_buffer, curl, "until", state->until);

		if (state->metadataPrefix)
			AppendOAIArgument(&url_buffer, curl, "metadataPrefix", state->metadataPrefix);

		if (state->resumptionToken)
		{
			char *encoded_token;

			elog(DEBUG2, "  %s (%s): appending 'resumptionToken' > %s", __func__, state->requestVerb, state->resumptionToken);
			resetStringInfo(&url_buffer);

			/* URL-encode the resumption token to handle special characters like & */
			encoded_token = curl_easy_escape(curl, state->resumptionToken, 0);
			if (encoded_token)
			{
				elog(DEBUG2, "  %s (%s): encoded resumptionToken > %s", __func__, state->requestVerb, encoded_token);
				appendStringInfo(&url_buffer, "verb=%s&resumptionToken=%s", state->requestVerb, encoded_token);
				curl_free(encoded_token);
			}
			else
			{
				/* Fallback to unencoded if encoding fails */
				elog(DEBUG2, "  %s (%s): encoding failed, using raw token", __func__, state->requestVerb);
				appendStringInfo(&url_buffer, "verb=%s&resumptionToken=%s", state->requestVerb, state->resumptionToken);
			}
		}
	}
	else if (strcmp(state->requestVerb, OAI_REQUEST_LISTSETS) == 0)
	{
		if (state->resumptionToken)
		{
			char *encoded_token = curl_easy_escape(curl, state->resumptionToken, 0);
			appendStringInfo(&url_buffer, "&resumptionToken=%s", encoded_token);
			curl_free(encoded_token);
		}
	}
	else
	{
		if (strcmp(state->requestVerb, OAI_REQUEST_LISTMETADATAFORMATS) != 0 &&
			strcmp(state->requestVerb, OAI_REQUEST_LISTSETS) != 0 &&
			strcmp(state->requestVerb, OAI_REQUEST_IDENTIFY) != 0)
		{
			/*
			 * Nothing was requested, so no document is produced. Clear any
			 * document left over from a previous request, so that the caller
			 * cannot mistake it for the response to this one and free it twice.
			 */
			OAIFreeXmlDoc(state);
			free(chunk.memory);
			free(chunk_header.memory);
			curl_easy_cleanup(curl);

			return OAI_UNKNOWN_REQUEST;
		}
	}

	/*
	 * Build the full request URL. OAI-PMH arguments used to be sent through
	 * CURLOPT_POSTFIELDS, which made every request a POST: libcurl turns a POST
	 * into a GET on a 301/302 and drops the body while doing so, meaning that a
	 * redirected request reached the repository with no arguments at all. Using
	 * a plain GET with a query string keeps the arguments across redirects.
	 */
	initStringInfo(&request_url);
	appendStringInfo(&request_url, "%s%c%s", state->url,
					 strchr(state->url, '?') ? '&' : '?', url_buffer.data);

	elog(DEBUG1, "GET \"%s\"", request_url.data);

	if (curl)
	{
		errbuf[0] = 0;

		curl_easy_setopt(curl, CURLOPT_URL, request_url.data);
		curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);

#if ((LIBCURL_VERSION_MAJOR == 7 && LIBCURL_VERSION_MINOR < 85) || LIBCURL_VERSION_MAJOR < 7)
		curl_easy_setopt(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
#else
		curl_easy_setopt(curl, CURLOPT_PROTOCOLS_STR, "http,https");
#endif

		curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

		/*
		 * Without an asynchronous resolver, libcurl times out name lookups
		 * with SIGALRM, which PostgreSQL uses for its own timeouts.
		 */
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

		curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connectTimeout);
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, request_timeout);

		/* without it, a stalled connection would hang the request forever */
		curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1L);
		curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, (long)OAI_FDW_STALL_TIMEOUT);

		elog(DEBUG2, "  %s (%s): timeout > %ld", __func__, state->requestVerb, connectTimeout);
		elog(DEBUG2, "  %s (%s): max retry > %ld", __func__, state->requestVerb, maxretries);

		/* Proxy support: added in version 1.1.0 */
		if (state->proxy)
		{

			elog(DEBUG2, "%s (%s): proxy URL > '%s'", __func__, state->requestVerb, state->proxy);

			curl_easy_setopt(curl, CURLOPT_PROXY, state->proxy);

			if (strcmp(state->proxyType, OAI_SERVER_OPTION_HTTP_PROXY) == 0)
			{
				elog(DEBUG2, "%s (%s): proxy protocol > 'HTTP'", __func__, state->requestVerb);
				curl_easy_setopt(curl, CURLOPT_PROXYTYPE, CURLPROXY_HTTP);
			}
			if (state->proxyUser)
			{
				elog(DEBUG2, "%s (%s): entering proxy user ('%s').", __func__, state->requestVerb, state->proxyUser);
				curl_easy_setopt(curl, CURLOPT_PROXYUSERNAME, state->proxyUser);
			}
			if (state->proxyPassword)
			{
				elog(DEBUG2, "%s (%s): entering proxy user's password.", __func__, state->requestVerb);
				curl_easy_setopt(curl, CURLOPT_PROXYPASSWORD, state->proxyPassword);
			}
		}

		if (state->requestRedirect == true)
		{

			elog(DEBUG2, "  %s (%s): setting request redirect: %d", __func__, state->requestVerb, state->requestRedirect);
			curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

			if (state->requestMaxRedirect)
			{
				elog(DEBUG2, "  %s (%s): setting maxredirs: %ld", __func__, state->requestVerb, state->requestMaxRedirect);
				curl_easy_setopt(curl, CURLOPT_MAXREDIRS, state->requestMaxRedirect);
			}
		}

		/* Set the progress callback function */
		curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
		curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, CURLProgressCallback);

		/*
		 * Enable libcurl verbose output, but route it exclusively through
		 * CURLDebugCallback instead of stderr. The callback emits at DEBUG3
		 * (gated by log_min_messages) and redacts Authorization headers so
		 * credentials are never written to server logs.
		 */
		curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
		curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, CURLDebugCallback);
		curl_easy_setopt(curl, CURLOPT_DEBUGDATA, NULL);

		curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCallbackFunction);
		curl_easy_setopt(curl, CURLOPT_HEADERDATA, (void *)&chunk_header);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
		curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);

		if (state->user && state->password)
		{
			curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
			curl_easy_setopt(curl, CURLOPT_USERNAME, state->user);
			curl_easy_setopt(curl, CURLOPT_PASSWORD, state->password);
		}
		else if (state->user && !state->password)
		{
			curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
			curl_easy_setopt(curl, CURLOPT_USERNAME, state->user);
		}

		initStringInfo(&user_agent);
		appendStringInfo(&user_agent, "PostgreSQL/%s oai_fdw/%s libxml2/%s %s", PG_VERSION, OAI_FDW_VERSION, LIBXML_DOTTED_VERSION, curl_version());
		curl_easy_setopt(curl, CURLOPT_USERAGENT, user_agent.data);

		/* OAI-PMH responses are text/xml (spec 3.1.2.1) */
		headers = curl_slist_append(headers, "Accept: text/xml, application/xml");
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

		elog(DEBUG2, "  %s (%s): performing cURL request ... ", __func__, state->requestVerb);

		/*
		 * The Retry-After wait and the warnings below may raise an error, so
		 * the malloc'd buffers and the curl handle are released on the way out.
		 */
		PG_TRY();
		{
			res = curl_easy_perform(curl);

			for (long i = 1; res != CURLE_OK && res != CURLE_ABORTED_BY_CALLBACK && i <= maxretries; i++)
			{
				long attempt_code = 0;
				long connect_code = 0;
				long delay;

				curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &attempt_code);
				curl_easy_getinfo(curl, CURLINFO_HTTP_CONNECTCODE, &connect_code);

				/* other client errors, e.g. 401 or 404, will not go away by retrying */
				if (attempt_code >= 400 && attempt_code < 500 &&
					attempt_code != OAI_HTTP_REQUEST_TIMEOUT &&
					attempt_code != OAI_HTTP_TOO_MANY_REQUESTS)
					break;

				/* neither will a proxy refusing the tunnel, e.g. 407 */
				if (connect_code >= 400 && connect_code < 500)
					break;

				/*
				 * Every retry waits, growing from 5 seconds up to the maximum: a
				 * failure that goes away on its own, e.g. an overloaded or briefly
				 * unreachable repository, usually needs more than a few seconds.
				 *
				 * OAI-PMH flow control (spec section 3.4): with "503 Service
				 * Unavailable", as with "429 Too Many Requests" (RFC 6585), the
				 * repository may say how long to wait in a Retry-After header,
				 * which is honoured instead. It has to be read before the header
				 * buffer is cleared for the next attempt.
				 */
				delay = Min(OAI_FDW_DEFAULT_RETRY_AFTER << Min(i - 1, 16),
							OAI_FDW_MAX_RETRY_AFTER);

				if (attempt_code == OAI_HTTP_SERVICE_UNAVAILABLE ||
					attempt_code == OAI_HTTP_TOO_MANY_REQUESTS)
				{
					long retry_after = ParseRetryAfter(chunk_header.memory);

					if (retry_after >= 0)
						delay = retry_after;

					elog(WARNING, "'%s' is applying flow control (HTTP %ld), retrying in %ld seconds (%ld/%ld)",
						 state->foreign_server->servername, attempt_code, delay, i, maxretries);
				}
				else
					elog(WARNING, "request to '%s' failed, retrying in %ld seconds (%ld/%ld)",
						 state->foreign_server->servername, delay, i, maxretries);

				/* libcurl's reason, for the server log only: its wording varies */
				if (errbuf[0] != '\0')
					ereport(LOG,
							(errmsg("request to '%s' failed: %s",
									state->foreign_server->servername, errbuf),
							 errhidestmt(true)));

				OAIWaitRetryAfter(delay, state->foreign_server->servername);

				/* discard any partial data from the failed attempt */
				chunk.size = 0;
				chunk.memory[0] = '\0';
				chunk.alloc_failed = false;
				chunk_header.size = 0;
				chunk_header.memory[0] = '\0';
				chunk_header.alloc_failed = false;
				chunk_header.unsupported_ctype[0] = '\0';

				res = curl_easy_perform(curl);
			}
		}
		PG_CATCH();
		{
			free(chunk.memory);
			free(chunk_header.memory);
			curl_slist_free_all(headers);
			curl_easy_cleanup(curl);
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* aborted by CURLProgressCallback: process the pending interrupt */
		if (res == CURLE_ABORTED_BY_CALLBACK)
		{
			free(chunk.memory);
			free(chunk_header.memory);
			curl_slist_free_all(headers);
			curl_easy_cleanup(curl);

			CHECK_FOR_INTERRUPTS();

			ereport(ERROR,
					(errcode(ERRCODE_QUERY_CANCELED),
					 errmsg("OAI request to '%s' was cancelled", state->url)));
		}

		/*
		 * Report the conditions the libcurl callbacks recorded rather than
		 * raising: an allocation failure is fatal, an unexpected content-type
		 * is only worth a warning.
		 */
		if (chunk.alloc_failed || chunk_header.alloc_failed)
		{
			free(chunk.memory);
			free(chunk_header.memory);
			curl_slist_free_all(headers);
			curl_easy_cleanup(curl);

			ereport(ERROR,
					(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
					 errmsg("%s: out of memory while reading the response from '%s'",
							__func__, state->url)));
		}

		/*
		 * A redirect that was not followed: its body is not the answer, and
		 * would only be reported as an invalid response.
		 */
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &final_code);

		if (res == CURLE_OK && final_code >= 300 && final_code < 400)
		{
			char *location = NULL;
			char *target;

			curl_easy_getinfo(curl, CURLINFO_REDIRECT_URL, &location);
			target = location ? pstrdup(location) : NULL;

			free(chunk.memory);
			free(chunk_header.memory);
			curl_slist_free_all(headers);
			curl_easy_cleanup(curl);

			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("OAI request was redirected: HTTP %ld", final_code),
					 target ? errdetail("Location: \"%s\"", target) : 0,
					 errhint("Set '%s' on the FOREIGN SERVER to follow redirects, or use the new URL.",
							 OAI_SERVER_OPTION_REQUEST_REDIRECT)));
		}

		if (chunk_header.unsupported_ctype[0] != '\0')
			elog(WARNING, "unsupported content-type: \"%s\"", chunk_header.unsupported_ctype);

		if (res != CURLE_OK)
		{
			long response_code = 0;
			bool has_body = (chunk.size > 0 && chunk.memory);
			StringInfoData display_body;

			/*
			 * Retrieve the status code up front: it is reported in the messages
			 * below, including the DEBUG1 line taken when there is no body.
			 */
			curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

			initStringInfo(&display_body);

			if (has_body)
			{
				/*
				 * Truncate the error body before logging or including in
				 * error messages.  Endpoints may return large HTML pages on
				 * errors (e.g. from misconfigured proxies), which would
				 * flood server logs.
				 */
				if (chunk.size > OAI_FDW_MAX_ERROR_BODY)
				{
					appendBinaryStringInfo(&display_body, chunk.memory, OAI_FDW_MAX_ERROR_BODY);
					appendStringInfoString(&display_body, "... (truncated)");
				}
				else
				{
					appendStringInfoString(&display_body, chunk.memory);
				}
				elog(DEBUG1, "%s: error response body: %s", __func__, display_body.data);
			}
			else
			{
				elog(DEBUG1, "%s: no response body available for HTTP error %ld", __func__, response_code);
			}
			if (chunk.memory)
				free(chunk.memory);
			if (chunk_header.memory)
				free(chunk_header.memory);
			curl_slist_free_all(headers);
			curl_easy_cleanup(curl);

			/* a timeout, e.g. a stalled transfer, is not about the HTTP status */
			if (res == CURLE_OPERATION_TIMEDOUT)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						 errmsg("OAI request timed out: %s", errbuf),
						 errdetail("URL: \"%s\"", request_url.data)));

			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("OAI request failed: HTTP %ld", response_code),
					 response_code == OAI_HTTP_SERVICE_UNAVAILABLE || response_code == OAI_HTTP_TOO_MANY_REQUESTS
						 ? errhint("The repository is still applying flow control after %ld retries. Raise '%s' on the FOREIGN SERVER or harvest again later.",
								   maxretries, OAI_SERVER_OPTION_CONNECTRETRY)
						 : errhint("Check your request parameters and try again."),
					 errdetail("URL: \"%s\"", request_url.data)));
		}
		else
		{
			long response_code;
			curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
			/*
			 * NOERROR/NOWARNING keep libxml2 from writing its messages, with
			 * fragments of the response, straight to the server's stderr.
			 */
			state->xmldoc = xmlReadMemory(chunk.memory, chunk.size, NULL, NULL,
										  XML_PARSE_NOBLANKS | XML_PARSE_NONET |
											  XML_PARSE_NOERROR | XML_PARSE_NOWARNING);

			if (!state->xmldoc)
			{
				const xmlError *error = xmlGetLastError();

				if (error && error->message)
					parse_error = pchomp(error->message);
			}

			elog(DEBUG1, "HTTP %ld, %ld bytes", response_code, chunk.size);

			elog(DEBUG2, "  %s (%s): http response code = %ld", __func__, state->requestVerb, response_code);
			elog(DEBUG2, "  %s (%s): http response size = %ld", __func__, state->requestVerb, chunk.size);
			elog(DEBUG2, "  %s (%s): http response header = \n%s", __func__, state->requestVerb, chunk_header.memory);
		}
	}

	if (chunk.memory)
		free(chunk.memory);
	if (chunk_header.memory)
		free(chunk_header.memory);
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (!state->xmldoc && parse_error)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("invalid XML response from '%s'", state->url),
				 errdetail("%s", parse_error)));

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
	elog(DEBUG2, "%s called.", __func__);

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

			if (strcmp(def->defname, OAI_NODE_COLUMN_OPTION) == 0)
			{
				char *option_value = defGetString(def);
				char *attname = NameStr(attr->attname);
				state->numfdwcols++;

				if (strcmp(option_value, OAI_NODE_STATUS) == 0)
				{
					if (attr->atttypid != BOOLOID)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
								 errhint("OAI %s must be of type 'boolean'.",
										 OAI_NODE_STATUS)));
				}
				else if (strcmp(option_value, OAI_NODE_IDENTIFIER) == 0 || strcmp(option_value, OAI_NODE_METADATAPREFIX) == 0)
				{
					if (attr->atttypid != TEXTOID &&
						attr->atttypid != VARCHAROID)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
								 errhint("OAI %s must be of type 'text' or 'varchar'.",
										 option_value)));
				}
				else if (strcmp(option_value, OAI_NODE_CONTENT) == 0)
				{
					hasContentForeignColumn = true;

					if (attr->atttypid != TEXTOID &&
						attr->atttypid != VARCHAROID &&
						attr->atttypid != XMLOID)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
								 errhint("OAI %s expects one of the following types: 'xml', 'text' or 'varchar'.",
										 OAI_NODE_CONTENT)));
				}
				else if (strcmp(option_value, OAI_NODE_SETSPEC) == 0)
				{
					if (attr->atttypid != TEXTARRAYOID &&
						attr->atttypid != VARCHARARRAYOID)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("invalid data type for '%s.%s': %d",
										relname, attname, attr->atttypid),
								 errhint("OAI %s expects one of the following types: 'text[]', 'varchar[]'.",
										 OAI_NODE_SETSPEC)));
				}
				else if (strcmp(option_value, OAI_NODE_DATESTAMP) == 0)
				{
					if (attr->atttypid != TIMESTAMPOID)
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

	/* If the foreign table has no "oai_attribute = 'content'" there is no need
	 * to retrieve the document itself. The ListIdentifiers request lists the
	 * whole OAI header */
	if (!hasContentForeignColumn)
	{
		state->requestVerb = OAI_REQUEST_LISTIDENTIFIERS;
		elog(DEBUG2, "  %s: the foreign table '%s' has no 'content' OAI node. Request type set to '%s'",
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

	elog(DEBUG2, "%s => finished.", __func__);
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

	elog(DEBUG2, "  %s called", __func__);
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

				if (strcmp(def->defname, OAI_NODE_COLUMN_OPTION) == 0)
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

	elog(DEBUG2, "%s: type > %u", __func__, type);

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
	Node *left;
	Node *right;
	OAIPushdownKind kind;

	elog(DEBUG2, "%s called for expr->type %u", __func__, expr->type);

	switch (expr->type)
	{

	case T_OpExpr:

		elog(DEBUG2, "  %s: case T_OpExpr", __func__);
		oper = (OpExpr *)expr;

		/* only binary operators can be pushed down */
		if (list_length(oper->args) != 2)
			break;

		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper->opno));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "%s: cache lookup failed for operator %u", __func__, oper->opno);

		operName = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);

		ReleaseSysCache(tuple);

		elog(DEBUG2, "  %s: opername > %s", __func__, operName);

		left = linitial(oper->args);
		right = lsecond(oper->args);

		if (!IsA(left, Var))
			break; /* let PG evaluate locally */

		var = (Var *)left;
		oaiNode = GetOAINodeFromColumn(state->foreign_table->relid, var->varattno);

		if (!oaiNode)
			break;

		kind = GetPushdownKind(operName, oaiNode, var->vartype);

		/* strictness lets a NULL value end the scan, see EvaluatePushdownExpressions() */
		if (kind == OAI_PUSHDOWN_NONE || !op_strict(oper->opno))
			break;

		if (IsA(right, Const))
			ApplyPushdown(state, kind, (Const *)right);
		else if (!contain_var_clause(right) &&
				 !contain_volatile_functions(right) &&
				 !contain_subplans(right))
		{
			/*
			 * The value is only known at execution time, e.g. $1 in a generic
			 * plan or now(): see EvaluatePushdownExpressions().
			 */
			state->pushdown_exprs = lappend(state->pushdown_exprs, right);
			state->pushdown_kinds = lappend_int(state->pushdown_kinds, kind);
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

/*
 * Returns NULL for values an OAI UTCdatetime cannot express (infinity,
 * years outside 1..9999), so that they are not pushed down.
 */
static char *deparseTimestamp(Datum datum)
{

	struct pg_tm datetime_tm;
	fsec_t datetime_fsec;
	StringInfoData s;
	Timestamp ts = DatumGetTimestamp(datum);

	if (TIMESTAMP_NOT_FINITE(ts) ||
		timestamp2tm(ts, NULL, &datetime_tm, &datetime_fsec, NULL, NULL) != 0 ||
		datetime_tm.tm_year < 1 || datetime_tm.tm_year > 9999)
		return NULL;

	initStringInfo(&s);
	appendStringInfo(&s, "%04d-%02d-%02dT%02d:%02d:%02dZ",
					 datetime_tm.tm_year,
					 datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
					 datetime_tm.tm_min, datetime_tm.tm_sec);

	return s.data;
}

/*
 * deparseDatestampConst
 * ---------------------
 * Converts a constant compared with a datestamp column into an OAI
 * UTCdatetime, or returns NULL if it cannot be pushed down. A timestamptz
 * is converted in the session time zone, as PostgreSQL does when comparing
 * it with the (timestamp) column.
 */
static char *deparseDatestampConst(Const *constant)
{
	if (constant->constisnull)
		return NULL;

	if (constant->consttype == TIMESTAMPOID)
		return deparseTimestamp(constant->constvalue);

	if (constant->consttype == DATEOID)
		return deparseTimestamp(DirectFunctionCall1(date_timestamp, constant->constvalue));

	if (constant->consttype == TIMESTAMPTZOID)
		return deparseTimestamp(DirectFunctionCall1(timestamptz_timestamp, constant->constvalue));

	return NULL;
}

/*
 * deparseTextConst
 * ----------------
 * Returns the value of a text or varchar constant, or NULL for any other
 * constant.
 */
static char *deparseTextConst(Const *constant)
{
	if (constant->constisnull ||
		(constant->consttype != TEXTOID && constant->consttype != VARCHAROID))
		return NULL;

	return datumToString(constant->constvalue, constant->consttype);
}

/*
 * GetPushdownKind
 * ---------------
 * Returns what a condition "column operator value" sets in the request, or
 * OAI_PUSHDOWN_NONE if it cannot be pushed down.
 */
static OAIPushdownKind GetPushdownKind(const char *operName, const char *oaiNode, Oid vartype)
{
	bool is_text = vartype == TEXTOID || vartype == VARCHAROID;
	bool is_datestamp = strcmp(oaiNode, OAI_NODE_DATESTAMP) == 0 && vartype == TIMESTAMPOID;

	if (strcmp(operName, "=") == 0)
	{
		if (strcmp(oaiNode, OAI_NODE_IDENTIFIER) == 0 && is_text)
			return OAI_PUSHDOWN_IDENTIFIER;
		if (strcmp(oaiNode, OAI_NODE_METADATAPREFIX) == 0 && is_text)
			return OAI_PUSHDOWN_METADATAPREFIX;
		if (is_datestamp)
			return OAI_PUSHDOWN_DATESTAMP;
	}

	if ((strcmp(operName, ">=") == 0 || strcmp(operName, ">") == 0) && is_datestamp)
		return OAI_PUSHDOWN_FROM;

	if ((strcmp(operName, "<=") == 0 || strcmp(operName, "<") == 0) && is_datestamp)
		return OAI_PUSHDOWN_UNTIL;

	if ((strcmp(operName, "<@") == 0 || strcmp(operName, "@>") == 0 || strcmp(operName, "&&") == 0) &&
		strcmp(oaiNode, OAI_NODE_SETSPEC) == 0 &&
		(vartype == TEXTARRAYOID || vartype == VARCHARARRAYOID))
		return OAI_PUSHDOWN_SET;

	return OAI_PUSHDOWN_NONE;
}

/*
 * ApplyPushdown
 * -------------
 * Sets the request argument a condition's value stands for. Values that
 * cannot be pushed down are ignored: the condition is evaluated locally.
 */
static void ApplyPushdown(OAIFdwState *state, OAIPushdownKind kind, Const *constant)
{
	char *value;

	switch (kind)
	{
	case OAI_PUSHDOWN_IDENTIFIER:
		if ((value = deparseTextConst(constant)) != NULL)
		{
			state->requestVerb = OAI_REQUEST_GETRECORD;
			state->identifier = value;
			elog(DEBUG2, "  %s: request type set to '%s' with identifier '%s'", __func__, OAI_REQUEST_GETRECORD, state->identifier);
		}
		break;

	case OAI_PUSHDOWN_METADATAPREFIX:
		if ((value = deparseTextConst(constant)) != NULL)
		{
			state->metadataPrefix = value;
			elog(DEBUG2, "  %s: metadataPrefix set to '%s'", __func__, state->metadataPrefix);
		}
		break;

	case OAI_PUSHDOWN_DATESTAMP:
		if ((value = deparseDatestampConst(constant)) != NULL)
		{
			state->from = value;
			state->until = value;
		}
		break;

	case OAI_PUSHDOWN_FROM:
		if ((value = deparseDatestampConst(constant)) != NULL)
			state->from = value;
		break;

	case OAI_PUSHDOWN_UNTIL:
		if ((value = deparseDatestampConst(constant)) != NULL)
			state->until = value;
		break;

	case OAI_PUSHDOWN_SET:
	{
		ArrayType *array;
		int numitems;

		if (constant->constisnull)
			break;

		array = DatumGetArrayTypeP(constant->constvalue);
		numitems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

		if (numitems > 1)
		{
			elog(WARNING, "The OAI standard requests do not support multiple '%s' attributes. This filter will be applied AFTER the OAI request.", OAI_NODE_SETSPEC);
			elog(DEBUG2, "  %s: clearing '%s' attribute.", __func__, OAI_NODE_SETSPEC);
			state->set = NULL;
		}
		else if (numitems == 1)
		{
			bool isnull;
			Datum item;

			ArrayIterator iterator = array_create_iterator(array, 0, NULL);

			while (array_iterate(iterator, &item, &isnull))
			{
				/* a NULL element matches nothing, so there is no set to request */
				if (isnull)
					continue;

				state->set = datumToString(item, TEXTOID);
				elog(DEBUG2, "  %s: setSpec set to '%s'", __func__, state->set);
			}

			array_free_iterator(iterator);
		}
		break;
	}

	case OAI_PUSHDOWN_NONE:
		break;
	}
}

/*
 * EvaluatePushdownExpressions
 * ---------------------------
 * Evaluates the conditions whose value is only known at execution time and
 * sets the request arguments accordingly, on top of those set at planning.
 * Runs when a scan starts, so that a rescan picks up new parameter values.
 *
 * Returns false if a value is NULL: the operators are strict, so no row can
 * match and no request is needed.
 */
static bool EvaluatePushdownExpressions(ForeignScanState *node, OAIFdwState *state)
{
	ForeignScan *fs = (ForeignScan *)node->ss.ps.plan;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	MemoryContext oldcxt;
	ListCell *lc_expr;
	ListCell *lc_state;
	ListCell *lc_kind;

	state->requestVerb = state->planArgs->requestVerb;
	state->identifier = state->planArgs->identifier;
	state->metadataPrefix = state->planArgs->metadataPrefix;
	state->set = state->planArgs->set;
	state->from = state->planArgs->from;
	state->until = state->planArgs->until;

	MemoryContextReset(state->pushdowncxt);
	oldcxt = MemoryContextSwitchTo(state->pushdowncxt);

	forthree(lc_expr, fs->fdw_exprs, lc_state, state->pushdown_states, lc_kind, state->pushdown_kinds)
	{
		Node *expr = (Node *)lfirst(lc_expr);
		Oid type = exprType(expr);
		int16 typlen;
		bool typbyval;
		bool isnull;
		Datum value = ExecEvalExpr((ExprState *)lfirst(lc_state), econtext, &isnull);

		if (isnull)
		{
			MemoryContextSwitchTo(oldcxt);
			return false;
		}

		get_typlenbyval(type, &typlen, &typbyval);
		ApplyPushdown(state, (OAIPushdownKind)lfirst_int(lc_kind),
					  makeConst(type, exprTypmod(expr), exprCollation(expr),
								typlen, value, isnull, typbyval));
	}

	MemoryContextSwitchTo(oldcxt);

	return true;
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
	state->startup_cost = 10000.0;
	/* estimate total cost as startup cost + 10 * (returned rows) */
	state->total_cost = state->startup_cost + baserel->rows * 10.0;

	baserel->fdw_private = state;
}

static void OAIFdwGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{

	struct OAIFdwState *state = (struct OAIFdwState *)baserel->fdw_private;
	Path *path = (Path *)create_foreignscan_path(root, baserel,
												 NULL,			/* default pathtarget */
												 baserel->rows, /* rows */
#if PG_VERSION_NUM >= 180000
												 0, /* no parallel pathflags */
#endif
												 state->startup_cost, /* startup cost */
												 state->total_cost,	  /* total cost */
												 NIL,				  /* no pathkeys */
												 NULL,				  /* no required outer relids */
												 NULL,				  /* no fdw_outerpath */
#if PG_VERSION_NUM >= 170000
												 NIL,	/* no fdw_restrictinfo */
#endif													/* PG_VERSION_NUM */
												 NULL); /* no fdw_private */
	add_path(baserel, path);
}

static ForeignScan *OAIFdwGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
	OAIFdwState *state = baserel->fdw_private;
	List *fdw_private = NIL;

	InitSession(state, baserel);

	scan_clauses = extract_actual_clauses(scan_clauses, false);
	fdw_private = SerializePlanData(state);

	return make_foreignscan(tlist,
							scan_clauses,
							baserel->relid,
							state->pushdown_exprs, /* evaluated when the scan starts */
							fdw_private, /* pass along our state */
							NIL,		 /* no custom tlist; our scan tuple looks like tlist */
							NIL,		 /* no quals we will recheck */
							outer_plan);
}

static void OAIFdwBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fs = (ForeignScan *)node->ss.ps.plan;
	struct OAIFdwState *state = DeserializePlanData(fs->fdw_private);
	Oid userid;

	node->fdw_state = (void *)state;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	state->foreign_table = GetForeignTable(state->foreigntableid);
	state->foreign_server = GetForeignServer(state->foreign_table->serverid);

	/*
	 * Use the user mapping of the role the permissions are checked as, e.g.
	 * the owner of a view, as ExecCheckPermissions() and postgres_fdw do.
	 */
#if PG_VERSION_NUM >= 160000
	userid = OidIsValid(fs->checkAsUser) ? fs->checkAsUser : GetUserId();
#else
	{
		RangeTblEntry *rte = rt_fetch(fs->scan.scanrelid, node->ss.ps.state->es_range_table);

		userid = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
	}
#endif
	LoadOAIUserMapping(state, userid); /* restores user/password/proxy creds */

	state->oaicxt = AllocSetContextCreate(CurrentMemoryContext,
										  "oai_fdw_ctx",
										  ALLOCSET_DEFAULT_SIZES);
	/*
	 * Kept separate from oaicxt, which is reset for every page: the resumption
	 * token has to outlive the page it arrived with.
	 */
	state->tokencxt = AllocSetContextCreate(CurrentMemoryContext,
											"oai_fdw_token_ctx",
											ALLOCSET_SMALL_SIZES);

	state->pushdowncxt = AllocSetContextCreate(CurrentMemoryContext,
											   "oai_fdw_pushdown_ctx",
											   ALLOCSET_SMALL_SIZES);
	state->pushdown_states = ExecInitExprList(fs->fdw_exprs, (PlanState *)node);

	state->planArgs = (OAIRequestArgs *)palloc(sizeof(OAIRequestArgs));
	state->planArgs->requestVerb = state->requestVerb;
	state->planArgs->identifier = state->identifier;
	state->planArgs->metadataPrefix = state->metadataPrefix;
	state->planArgs->set = state->set;
	state->planArgs->from = state->from;
	state->planArgs->until = state->until;
}

static OAIRecord *FetchNextOAIRecord(OAIFdwState **state)
{
	if ((*state)->pageindex == (*state)->pagesize)
	{
		elog(DEBUG3, "%s: EOF > %d/%d", __func__, (*state)->pageindex, (*state)->pagesize);
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

/*
 * OAITextDatum
 * ------------
 * Converts a string for a text, varchar or xml column. varchar goes through
 * its input function, so that the column's length limit is applied.
 */
static Datum OAITextDatum(char *value, Oid pgtype, int pgtypmod)
{
	if (pgtype == VARCHAROID)
		return DirectFunctionCall3(varcharin,
								   CStringGetDatum(value),
								   ObjectIdGetDatum(InvalidOid),
								   Int32GetDatum(pgtypmod));

	return CStringGetTextDatum(value);
}

static void CreateOAITuple(TupleTableSlot *slot, OAIFdwState *state, OAIRecord *oai)
{
	elog(DEBUG2, "%s called", __func__);

	for (int i = 0; i < state->numcols; i++)
	{
		Oid pgtype = state->oaiTable->cols[i]->pgtype;
		char *oai_node = state->oaiTable->cols[i]->oai_node;
		char *colname = state->oaiTable->cols[i]->name;
		int pgtypmod = state->oaiTable->cols[i]->pgtypmod;

		if (oai_node)
		{
			slot->tts_isnull[i] = false;

			if (strcmp(oai_node, OAI_NODE_STATUS) == 0)
				slot->tts_values[i] = BoolGetDatum(oai->isDeleted);
			else if (strcmp(oai_node, OAI_NODE_IDENTIFIER) == 0)
			{
				if (oai->identifier)
					slot->tts_values[i] = OAITextDatum(oai->identifier, pgtype, pgtypmod);
				else
					slot->tts_isnull[i] = true;
			}
			else if (strcmp(oai_node, OAI_NODE_METADATAPREFIX) == 0)
			{
				if (oai->metadataPrefix)
					slot->tts_values[i] = OAITextDatum(oai->metadataPrefix, pgtype, pgtypmod);
				else
					slot->tts_isnull[i] = true;
			}
			else if (strcmp(oai_node, OAI_NODE_CONTENT) == 0)
			{
				if (oai->content)
					slot->tts_values[i] = OAITextDatum(oai->content, pgtype, pgtypmod);
				else
					slot->tts_isnull[i] = true;
			}
			else if (strcmp(oai_node, OAI_NODE_SETSPEC) == 0)
			{
				if (oai->setsArray && pgtype == VARCHARARRAYOID)
				{
					/* the elements are built as text; varchar[] needs its own element type */
					Datum *elems;
					bool *nulls;
					int nelems;

					deconstruct_array(oai->setsArray, TEXTOID, -1, false, 'i', &elems, &nulls, &nelems);
					slot->tts_values[i] = PointerGetDatum(construct_array(elems, nelems, VARCHAROID, -1, false, 'i'));
				}
				else if (oai->setsArray)
					slot->tts_values[i] = PointerGetDatum(oai->setsArray);
				else
					slot->tts_isnull[i] = true;
			}
			else if (strcmp(oai_node, OAI_NODE_DATESTAMP) == 0)
			{
				if (oai->datestamp)
				{
					HeapTuple tuple;
					regproc typinput;
					Datum datum;

					datum = CStringGetDatum(oai->datestamp);
					/* find the appropriate conversion function */
					tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtype));

					if (!HeapTupleIsValid(tuple))
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								 errmsg("cache lookup failed for type %u > column '%s'", pgtype, colname)));

					typinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;
					ReleaseSysCache(tuple);

					slot->tts_values[i] = OidFunctionCall3(
						typinput,
						datum,
						ObjectIdGetDatum(InvalidOid),
						Int32GetDatum(pgtypmod));
				}
				else
					slot->tts_isnull[i] = true;
			}
			else
				slot->tts_isnull[i] = true;
		}
		else
		{
			slot->tts_isnull[i] = true;
			slot->tts_values[i] = PointerGetDatum(NULL);
		}
	}
}

static void OAIExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	OAIFdwState *state = (OAIFdwState *)node->fdw_state;

	if (state)
	{
		if (state->foreign_server && strlen(state->foreign_server->servername) > 0)
			ExplainPropertyText("Foreign Server", state->foreign_server->servername, es);

		if (state->url && strlen(state->url) > 0)
			ExplainPropertyText("Foreign Server URL", state->url, es);

		if (state->requestVerb && strlen(state->requestVerb) > 0)
			ExplainPropertyText("requestVerb", state->requestVerb, es);

		if (state->set && strlen(state->set) > 0)
			ExplainPropertyText("setSpec", state->set, es);

		if (state->metadataPrefix && strlen(state->metadataPrefix) > 0)
			ExplainPropertyText("metadataPrefix", state->metadataPrefix, es);

		if (state->from && strlen(state->from) > 0)
			ExplainPropertyText("from", state->from, es);

		if (state->until && strlen(state->until) > 0)
			ExplainPropertyText("until", state->until, es);

		/*
		 * Arguments whose value is only known when the scan runs, e.g. from
		 * $1 or now(). EXPLAIN ANALYZE shows their values above instead.
		 */
		if (state->pushdown_kinds != NIL && !state->pushdown_evaluated)
		{
			static const char *const names[] = {
				[OAI_PUSHDOWN_IDENTIFIER] = "identifier",
				[OAI_PUSHDOWN_METADATAPREFIX] = "metadataPrefix",
				[OAI_PUSHDOWN_DATESTAMP] = "from, until",
				[OAI_PUSHDOWN_FROM] = "from",
				[OAI_PUSHDOWN_UNTIL] = "until",
				[OAI_PUSHDOWN_SET] = "setSpec"};
			bool present[lengthof(names)] = {false};
			StringInfoData buf;
			ListCell *lc;

			foreach (lc, state->pushdown_kinds)
				present[lfirst_int(lc)] = true;

			initStringInfo(&buf);

			for (int kind = 0; kind < lengthof(names); kind++)
			{
				if (present[kind])
					appendStringInfo(&buf, "%s%s", buf.len > 0 ? ", " : "", names[kind]);
			}

			ExplainPropertyText("Runtime arguments", buf.data, es);
		}
	}
}

static TupleTableSlot *OAIFdwIterateForeignScan(ForeignScanState *node)
{

	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	struct OAIFdwState *state = (struct OAIFdwState *)node->fdw_state;
	OAIRecord *record;
	MemoryContext old_cxt;

	elog(DEBUG2, "%s called.", __func__);

	ExecClearTuple(slot);

	/* Returns an empty tuple in case there is no mapping for OAI nodes and columns */
	if (state->numfdwcols == 0)
		return slot;

	if (!state->pushdown_evaluated)
	{
		state->pushdown_norows = !EvaluatePushdownExpressions(node, state);
		state->pushdown_evaluated = true;
	}

	if (state->pushdown_norows)
		return slot;

	old_cxt = MemoryContextSwitchTo(state->oaicxt);

	/*
	 * Load the first page when this function is called for the first time.
	 */
	if (state->rowcount == 0)
		LoadOAIRecords(&state);

	record = FetchNextOAIRecord(&state);

	/*
	 * The current page is exhausted. Keep requesting pages for as long as the
	 * repository hands out resumption tokens and none of them has produced a
	 * record yet: a page may legitimately come back empty while still carrying
	 * a token, and returning here would end the scan silently, dropping every
	 * record behind that token. ExecuteOAIRequest() checks for interrupts, so
	 * the loop stays cancellable.
	 */
	while (record == NULL && state->resumptionToken)
	{
		LoadOAIRecords(&state);
		record = FetchNextOAIRecord(&state);
	}

	MemoryContextSwitchTo(old_cxt);

	if (record != NULL)
	{
		MemoryContext per_tuple = node->ss.ps.ps_ExprContext->ecxt_per_tuple_memory;
		MemoryContext old = MemoryContextSwitchTo(per_tuple);

		elog(DEBUG2, "  %s: creating OAI tuple", __func__);
		CreateOAITuple(slot, state, record);
		MemoryContextSwitchTo(old);

		elog(DEBUG2, "  %s: storing virtual tuple", __func__);
		ExecStoreVirtualTuple(slot);
		pfree(record);
	}

	elog(DEBUG3, "%s => returning tuple (rowcount: %d)", __func__, state->rowcount);

	return slot;
}

/*
 * CheckOAIResponse
 * ----------------
 * Checks that a response is an OAI-PMH document answering the request, so
 * that e.g. an HTML maintenance page does not pass for an empty result, and
 * raises the OAI errors it reports. The document is released on error.
 */
static void CheckOAIResponse(OAIFdwState *state)
{
	PG_TRY();
	{
		xmlNodePtr root;
		xmlNodePtr node;
		bool answered = false;

		if (!state->xmldoc)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("invalid XML response from '%s'", state->url)));

		root = xmlDocGetRootElement(state->xmldoc);

		if (!root || xmlStrcmp(root->name, (xmlChar *)OAI_XML_ROOT_ELEMENT) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("invalid %s response from '%s'", state->requestVerb, state->url),
					 errdetail("The response is not an OAI-PMH document.")));

		for (node = root->children; node != NULL; node = node->next)
		{
			if (node->type != XML_ELEMENT_NODE)
				continue;

			if (xmlStrcmp(node->name, (xmlChar *)"error") == 0)
			{
				RaiseOAIException(node);
				answered = true;
			}
			else if (xmlStrcmp(node->name, (xmlChar *)state->requestVerb) == 0)
				answered = true;
		}

		if (!answered)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("invalid %s response from '%s'", state->requestVerb, state->url),
					 errdetail("The response contains neither <%s> nor <error>.", state->requestVerb)));
	}
	PG_CATCH();
	{
		OAIFreeXmlDoc(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void RaiseOAIException(xmlNodePtr error)
{
	xmlChar *code = xmlGetProp(error, (xmlChar *)"code");
	xmlChar *cont = xmlNodeGetContent(error);
	char *ccode;
	char *ccont;

	/* Guard the RAW pointers before any string op */
	if (!code)
	{
		if (cont)
			xmlFree(cont);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("invalid OAI error response: missing 'code' attribute")));
	}

	ccode = pstrdup((char *)code);
	ccont = cont ? pstrdup((char *)cont) : pstrdup("");

	xmlFree(code);
	if (cont)
		xmlFree(cont);

	ccont = OAIToServer((xmlChar *)ccont);

	if (strcmp(ccode, OAI_ERROR_ID_DOES_NOT_EXIST) == 0 ||
		strcmp(ccode, OAI_ERROR_NO_RECORD_MATCH) == 0 ||
		strcmp(ccode, OAI_ERROR_NO_SET_HIERARCHY) == 0 ||
		strcmp(ccode, OAI_ERROR_NO_METADATA_FORMATS) == 0)
		ereport(WARNING,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("OAI %s: %s", ccode, ccont)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("OAI %s: %s", ccode, ccont)));
}

/*
 * HasDayGranularity
 * -----------------
 * Returns true if the repository only supports YYYY-MM-DD datestamps. The
 * Identify request is only sent the first time a server is asked about.
 */
static bool HasDayGranularity(OAIFdwState *state)
{
	ListCell *lc;
	List *identity;
	char *verb = state->requestVerb;
	OAIGranularity *entry;
	MemoryContext oldcxt;
	bool day = false;

	foreach (lc, granularity_cache)
	{
		entry = (OAIGranularity *)lfirst(lc);

		if (entry->serverid == state->foreign_server->serverid && strcmp(entry->url, state->url) == 0)
			return entry->day;
	}

	identity = GetIdentity(state);
	state->requestVerb = verb;

	foreach (lc, identity)
	{
		OAIFdwIdentityNode *node = (OAIFdwIdentityNode *)lfirst(lc);

		if (strcmp(node->name, "granularity") == 0 && node->description)
			day = strstr(node->description, "hh") == NULL;
	}

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	entry = (OAIGranularity *)palloc(sizeof(OAIGranularity));
	entry->serverid = state->foreign_server->serverid;
	entry->url = pstrdup(state->url);
	entry->day = day;
	granularity_cache = lappend(granularity_cache, entry);
	MemoryContextSwitchTo(oldcxt);

	return day;
}

/*
 * NormalizeDatestampArguments
 * ---------------------------
 * Brings 'from' and 'until' to a granularity the repository accepts (spec
 * 3.3.2): a day-granular repository rejects time components, and both
 * arguments must have the same granularity. Widening a bound is safe, as
 * the conditions are evaluated locally anyway.
 */
static void NormalizeDatestampArguments(OAIFdwState *state)
{
	MemoryContext cxt = GetMemoryChunkContext(state);
	bool from_day;
	bool until_day;

	if (!state->from && !state->until)
		return;

	from_day = state->from && strlen(state->from) == 10;
	until_day = state->until && strlen(state->until) == 10;

	if (HasDayGranularity(state))
	{
		if (state->from && !from_day)
			state->from = MemoryContextStrdup(cxt, pnstrdup(state->from, 10));
		if (state->until && !until_day)
			state->until = MemoryContextStrdup(cxt, pnstrdup(state->until, 10));
	}
	else if (state->from && state->until && from_day != until_day)
	{
		if (from_day)
			state->from = MemoryContextStrdup(cxt, psprintf("%sT00:00:00Z", state->from));
		else
			state->until = MemoryContextStrdup(cxt, psprintf("%sT23:59:59Z", state->until));
	}
}

static void LoadOAIRecords(struct OAIFdwState **state)
{
	xmlNodePtr xmlroot;
	xmlNodePtr oaipmh;
	xmlNodePtr headerElements;
	xmlNodePtr ListRecordsRequest;

	char *token = NULL;

	elog(DEBUG2, "%s called.", __func__);

	/*
	 * The records of the page that has just been consumed are not needed any
	 * more, so the scan context is reset before the next page is requested.
	 * Without this, a harvest accumulates every page it has already returned
	 * and memory use grows with the size of the whole repository rather than
	 * with the size of a single page.
	 *
	 * The resumption token pointing at the next page was allocated in that
	 * same context, so it has to be carried across the reset. tokencxt is
	 * reset first, releasing the copy made for the previous page - which has
	 * by now been used to build the request that produced this one.
	 */
	if ((*state)->resumptionToken)
	{
		MemoryContextReset((*state)->tokencxt);
		token = MemoryContextStrdup((*state)->tokencxt, (*state)->resumptionToken);
	}

	MemoryContextReset((*state)->oaicxt);

	(*state)->resumptionToken = token;
	/* Removes all retrieved records, if any.*/
	(*state)->records = NIL;
	/* Sets the page size and index to zero.*/
	(*state)->pagesize = 0;
	(*state)->pageindex = 0;

	/* GetRecord takes no from/until, so their granularity does not matter */
	if (!(*state)->resumptionToken &&
		strcmp((*state)->requestVerb, OAI_REQUEST_GETRECORD) != 0)
		NormalizeDatestampArguments(*state);

	if (ExecuteOAIRequest(*state) == OAI_SUCCESS)
	{
		PG_TRY();
		{
			/*
			 * After executing an OAI request the resumption token is no longer
			 * needed. A new resumption token will be loaded in case there are
			 * still records left to be retrieved.
			 */
			(*state)->resumptionToken = NULL;

			CheckOAIResponse(*state);
			xmlroot = xmlDocGetRootElement((*state)->xmldoc);

			if (strcmp((*state)->requestVerb, OAI_REQUEST_LISTIDENTIFIERS) == 0)
			{
				for (oaipmh = xmlroot->children; oaipmh != NULL; oaipmh = oaipmh->next)
				{
					if (xmlStrcmp(oaipmh->name, (xmlChar *)(*state)->requestVerb) != 0)
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

							OAIRecord *oai = (OAIRecord *)palloc0(sizeof(OAIRecord));
							xmlChar *status;

							oai->setsArray = NULL;
							oai->isDeleted = false;
							oai->metadataPrefix = pstrdup((*state)->metadataPrefix);
							status = xmlGetProp(ListRecordsRequest, (xmlChar *)OAI_NODE_STATUS);

							if (status)
							{
								if (xmlStrcmp(status, (xmlChar *)OAI_RESPONSE_ELEMENT_DELETED) == 0)
									oai->isDeleted = true;
								xmlFree(status);
							}

							for (headerElements = ListRecordsRequest->children; headerElements != NULL; headerElements = headerElements->next)
							{
								/*
								 * OAI header values are plain text, so they must be read with
								 * xmlNodeGetContent(), which resolves XML escapes. Serialising
								 * the node instead (xmlNodeDump) yields the *escaped* markup and
								 * would turn an identifier such as "oai:ex.org/a&b" into
								 * "oai:ex.org/a&amp;b".
								 */
								char *content = OAINodeText(headerElements);

								if (!content)
									continue;

								if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_IDENTIFIER) == 0)
									oai->identifier = content;
								else if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SETSPEC) == 0)
									appendTextArray(&oai->setsArray, content);
								else if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_DATESTAMP) == 0)
									oai->datestamp = content;
							}

							elog(DEBUG2, "  %s (%s): Appending record list -> %s", __func__, (*state)->requestVerb, oai->identifier);

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

					if (xmlStrcmp(oaipmh->name, (xmlChar *)(*state)->requestVerb) != 0)
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
							OAIRecord *oai = (OAIRecord *)palloc0(sizeof(OAIRecord));
							xmlNodePtr record;

							oai->metadataPrefix = pstrdup((*state)->metadataPrefix);
							oai->isDeleted = false;
							oai->setsArray = NULL;

							for (record = ListRecordsRequest->children; record != NULL; record = record->next)
							{

								if (xmlStrcmp(record->name, (xmlChar *)OAI_RESPONSE_ELEMENT_METADATA) == 0)
								{
									xmlNodePtr root = record->children;
									xmlNodePtr copy;
									xmlBufferPtr buffer;

									/* <metadata> holds a single element: skip comments, PIs and text */
									while (root && root->type != XML_ELEMENT_NODE)
										root = root->next;

									if (!root)
										continue;

									/* Copy necessary to include the namespaces in the buffer output */
									copy = xmlCopyNode(root, 1);

									buffer = xmlBufferCreate();
									xmlNodeDump(buffer, (*state)->xmldoc, copy, 0, 1);

									elog(DEBUG2, "  %s (%s): XML Buffer size: %d", __func__, (*state)->requestVerb, buffer->size);

									oai->content = pstrdup((char *)buffer->content);

									elog(DEBUG2, "  %s (%s): freeing node copy.", __func__, (*state)->requestVerb);
									xmlFreeNode(copy);
									elog(DEBUG2, "  %s (%s): freeing xml content buffer.", __func__, (*state)->requestVerb);
									xmlBufferFree(buffer);

									/* after releasing libxml2's copies, as the conversion may fail */
									oai->content = OAIToServer((xmlChar *)oai->content);
								}

								if (xmlStrcmp(record->name, (xmlChar *)OAI_RESPONSE_ELEMENT_HEADER) == 0)
								{

									xmlChar *status = xmlGetProp(record, (xmlChar *)OAI_NODE_STATUS);
									if (status)
									{
										if (xmlStrcmp(status, (xmlChar *)OAI_RESPONSE_ELEMENT_DELETED) == 0)
											oai->isDeleted = true;
										xmlFree(status);
									}

									for (headerElements = record->children; headerElements != NULL; headerElements = headerElements->next)
									{
										/*
										 * See the note in the ListIdentifiers branch above: header
										 * values must be read with xmlNodeGetContent() so that XML
										 * escapes are resolved into the plain text they represent.
										 */
										char *content = OAINodeText(headerElements);

										if (!content)
											continue;

										if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_IDENTIFIER) == 0)
										{
											oai->identifier = content;
											elog(DEBUG2, "  %s (%s): setting identifier to OAI object > '%s'", __func__, (*state)->requestVerb, oai->identifier);
										}
										else if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_SETSPEC) == 0)
										{
											char *array_element = content;
											elog(DEBUG2, "  %s (%s): setting setspec to OAI object > '%s'", __func__, (*state)->requestVerb, array_element);

											appendTextArray(&oai->setsArray, array_element);
										}
										else if (xmlStrcmp(headerElements->name, (xmlChar *)OAI_RESPONSE_ELEMENT_DATESTAMP) == 0)
										{
											oai->datestamp = content;
											elog(DEBUG2, "  %s (%s): setting datestamp to OAI object > '%s'", __func__, (*state)->requestVerb, oai->datestamp);
										}

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
		PG_CATCH();
		{
			/*
			 * RaiseOAIException() and the checks above raise errors from the
			 * middle of the walk. Without this the document would never be
			 * freed, and since libxml2 memory is outside PostgreSQL's memory
			 * contexts it would stay lost for the life of the backend.
			 */
			OAIFreeXmlDoc(*state);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	OAIFreeXmlDoc(*state);
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
	struct OAIFdwState *state = (struct OAIFdwState *)node->fdw_state;

	if (!state)
		return;

	if (state->oaicxt)
		MemoryContextReset(state->oaicxt);

	if (state->tokencxt)
		MemoryContextReset(state->tokencxt);

	OAIFreeXmlDoc(state);

	state->rowcount = 0;
	state->pageindex = 0;
	state->pagesize = 0;
	state->records = NIL;
	state->resumptionToken = NULL;
	state->pushdown_evaluated = false;
}

static void OAIFdwEndForeignScan(ForeignScanState *node)
{
	struct OAIFdwState *state = (struct OAIFdwState *)node->fdw_state;
	if (!state)
		return;

	OAIFreeXmlDoc(state);

	if (state->oaicxt)
	{
		MemoryContextDelete(state->oaicxt);
		state->oaicxt = NULL;
	}

	if (state->tokencxt)
	{
		MemoryContextDelete(state->tokencxt);
		state->tokencxt = NULL;
	}

	elog(DEBUG2, "%s exit oai_fdw: so long .. \n", __func__);
}

/*
 * SetTableName
 * ------------
 * Returns the name of the foreign table IMPORT FOREIGN SCHEMA creates for a
 * set. Longer setSpecs (e.g. EPrints' hex-encoded ones) would be truncated
 * to the same name, so they are shortened and made unique with a hash of
 * the whole setSpec instead.
 */
static char *SetTableName(const char *setSpec)
{
	int len = strlen(setSpec);
	uint32 hash = 2166136261u; /* FNV-1a */

	if (len < NAMEDATALEN)
		return pstrdup(setSpec);

	for (int i = 0; i < len; i++)
		hash = (hash ^ (unsigned char)setSpec[i]) * 16777619u;

	return psprintf("%.*s_%08x",
					pg_mbcliplen(setSpec, len, NAMEDATALEN - 1 - 9),
					setSpec, hash);
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

	elog(DEBUG2, "%s called: '%s'", __func__, server->servername);
	state = GetOAIServerByName(server->servername);
	LoadOAIUserMapping(state, GetUserId());

	elog(DEBUG2, "  %s: parsing statements", __func__);

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
				ereport(ERROR,
						(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
						 errmsg("invalid 'metadataprefix': '%s'", defGetString(def))));

			format = defGetString(def);
			format_set = true;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("invalid FOREIGN SCHEMA OPTION: '%s'", def->defname)));
	}

	if (!format_set)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("missing 'metadataprefix' OPTION."),
				 errhint("A OAI Foreign Table must have a fixed 'metadataprefix'. Execute 'SELECT * FROM OAI_ListMetadataFormats('%s')' to see which formats are offered in the OAI Repository.", server->servername)));

	if (strcmp(stmt->remote_schema, "oai_sets") == 0)
	{
		List *tables = NIL;

		all_sets = GetSets(state);

		if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO || stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
		{
			/* only sets that exist in the repository are imported */
			bool limit_to = stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO;
			ListCell *cell_sets;

			foreach (cell_sets, all_sets)
			{
				OAISet *set = (OAISet *)lfirst(cell_sets);
				ListCell *cell_list;
				bool listed = false;

				foreach (cell_list, stmt->table_list)
				{
					RangeVar *rv = (RangeVar *)lfirst(cell_list);

					if (strcmp(rv->relname, SetTableName(set->setSpec)) == 0)
					{
						listed = true;
						break;
					}
				}

				if (listed == limit_to)
					tables = lappend(tables, set);
			}
		}
		else if (stmt->list_type == FDW_IMPORT_SCHEMA_ALL)
			tables = all_sets;

		foreach (cell, tables)
		{
			StringInfoData buffer;
			OAISet *set = (OAISet *)lfirst(cell);
			initStringInfo(&buffer);

			appendStringInfo(&buffer, "\nCREATE FOREIGN TABLE %s (\n", quote_identifier(SetTableName(set->setSpec)));
			appendStringInfo(&buffer, "  id text                OPTIONS (oai_node 'identifier'),\n");
			appendStringInfo(&buffer, "  xmldoc xml             OPTIONS (oai_node 'content'),\n");
			appendStringInfo(&buffer, "  sets text[]            OPTIONS (oai_node 'setspec'),\n");
			appendStringInfo(&buffer, "  updatedate timestamp   OPTIONS (oai_node 'datestamp'),\n");
			appendStringInfo(&buffer, "  format text            OPTIONS (oai_node 'metadataprefix'),\n");
			appendStringInfo(&buffer, "  status boolean         OPTIONS (oai_node 'status')\n");
			appendStringInfo(&buffer, ") SERVER %s OPTIONS (metadataPrefix %s, setspec %s);\n", quote_identifier(server->servername), quote_literal_cstr(format), quote_literal_cstr(set->setSpec));

			sql_commands = lappend(sql_commands, pstrdup(buffer.data));

			elog(DEBUG2, "%s: IMPORT FOREIGN SCHEMA (%s): \n%s", __func__, stmt->remote_schema, buffer.data);
		}

		elog(NOTICE, "Foreign tables to be created in schema '%s': %d", stmt->local_schema, list_length(sql_commands));
	}
	else if (strcmp(stmt->remote_schema, "oai_repository") == 0)
	{
		StringInfoData buffer;
		StringInfoData tblname;
		initStringInfo(&buffer);
		initStringInfo(&tblname);
		appendStringInfo(&tblname, "%s_repository", server->servername);
		appendStringInfo(&buffer, "\nCREATE FOREIGN TABLE %s (\n", quote_identifier(tblname.data));
		appendStringInfo(&buffer, "  id text                OPTIONS (oai_node 'identifier'),\n");
		appendStringInfo(&buffer, "  xmldoc xml             OPTIONS (oai_node 'content'),\n");
		appendStringInfo(&buffer, "  sets text[]            OPTIONS (oai_node 'setspec'),\n");
		appendStringInfo(&buffer, "  updatedate timestamp   OPTIONS (oai_node 'datestamp'),\n");
		appendStringInfo(&buffer, "  format text            OPTIONS (oai_node 'metadataprefix'),\n");
		appendStringInfo(&buffer, "  status boolean         OPTIONS (oai_node 'status')\n");
		appendStringInfo(&buffer, ") SERVER %s OPTIONS (metadataPrefix %s);\n", quote_identifier(server->servername), quote_literal_cstr(format));

		sql_commands = lappend(sql_commands, pstrdup(buffer.data));

		elog(DEBUG2, "%s: IMPORT FOREIGN SCHEMA: \n%s", __func__, buffer.data);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_NAME),
				 errmsg("invalid FOREIGN SCHEMA: '%s'", stmt->remote_schema)));

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
static Datum CreateDatum(int pgtype, int pgtypmod, char *value)
{
	regproc typinput;
	HeapTuple tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtype));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("cache lookup failed for type %u", pgtype)));

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

static void LoadOAIUserMapping(OAIFdwState *state, Oid userid)
{
	Datum datum;
	HeapTuple tp;
	bool isnull;
	UserMapping *um;
	List *options = NIL;
	ListCell *cell;
	bool usermatch = true;

	elog(DEBUG2, "%s called", __func__);

	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(userid),
						 ObjectIdGetDatum(state->foreign_server->serverid));

	if (!HeapTupleIsValid(tp))
	{
		elog(DEBUG2, "%s: not found for the specific user -- try PUBLIC", __func__);
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(state->foreign_server->serverid));
	}

	if (!HeapTupleIsValid(tp))
	{
		elog(DEBUG2, "%s: user mapping not found for user \"%s\", server \"%s\"",
			 __func__, MappingUserName(userid), state->foreign_server->servername);

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
		um->userid = userid;
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
					elog(DEBUG2, "%s: %s '%s'", __func__, def->defname, state->user);
				}

				if (strcmp(def->defname, OAI_USERMAPPING_OPTION_PASSWORD) == 0)
				{
					state->password = pstrdup(strVal(def->arg));
					elog(DEBUG2, "%s: %s '*******'", __func__, def->defname);
				}

				if (strcmp(def->defname, OAI_USERMAPPING_OPTION_PROXY_USER) == 0)
				{
					state->proxyUser = pstrdup(defGetString(def));
					elog(DEBUG2, "%s: proxy user '%s'", __func__, state->proxyUser);
				}

				if (strcmp(def->defname, OAI_USERMAPPING_OPTION_PROXY_PASSWORD) == 0)
				{
					state->proxyPassword = pstrdup(defGetString(def));
					elog(DEBUG2, "%s: proxy password '*******'", __func__);
				}
			}
		}

		ReleaseSysCache(tp);
	}
}

static void LoadOAITableInfo(OAIFdwState *state)
{
	TupleDesc tupdesc;
	Relation rel;
	ListCell *cell;

	elog(DEBUG2, "%s called", __func__);

#if PG_VERSION_NUM < 130000
	rel = heap_open(state->foreign_table->relid, NoLock);
#else
	rel = table_open(state->foreign_table->relid, NoLock);
#endif

	state->numcols = rel->rd_att->natts;
	tupdesc = rel->rd_att;

	/*
	 *Loading FOREIGN TABLE strucuture (columns and their OPTION values)
	 */
	state->oaiTable = (struct OAIfdwTable *)palloc0(sizeof(struct OAIfdwTable));
	state->oaiTable->cols = (struct OAIfdwColumn **)palloc0(sizeof(struct OAIfdwColumn *) * state->numcols);

	for (int i = 0; i < state->numcols; i++)
	{
		List *options = GetForeignColumnOptions(state->foreign_table->relid, i + 1);
		ListCell *lc;

		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		state->oaiTable->cols[i] = (struct OAIfdwColumn *)palloc0(sizeof(struct OAIfdwColumn));
		state->oaiTable->cols[i]->pgtype = attr->atttypid;
		state->oaiTable->cols[i]->name = pstrdup(NameStr(attr->attname));
		state->oaiTable->cols[i]->pgtypmod = attr->atttypmod;
		state->oaiTable->cols[i]->pgattnum = attr->attnum;

		foreach (lc, options)
		{
			DefElem *def = (DefElem *)lfirst(lc);

			if (strcmp(def->defname, OAI_NODE_COLUMN_OPTION) == 0)
			{
				elog(DEBUG2, "  %s: (%d) adding oai_node > '%s'", __func__, i, defGetString(def));
				state->oaiTable->cols[i]->oai_node = pstrdup(defGetString(def));
			}
		}
	}
#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	/*
	 * Loading FOREIGN TABLE OPTIONS
	 */
	foreach (cell, state->foreign_table->options)
	{
		DefElem *def = lfirst_node(DefElem, cell);

		if (strcmp(OAI_NODE_METADATAPREFIX, def->defname) == 0)
			state->metadataPrefix = defGetString(def);
		else if (strcmp(OAI_NODE_FROM, def->defname) == 0)
			state->from = defGetString(def);
		else if (strcmp(OAI_NODE_UNTIL, def->defname) == 0)
			state->until = defGetString(def);
		else if (strcmp(OAI_NODE_SETSPEC, def->defname) == 0)
			state->set = defGetString(def);
	}
}

/*
 * LoadOAIServerInfo
 * -----------------
 * Loads the options of state->foreign_server, for scans and, through
 * GetOAIServerByName(), for the support functions and IMPORT FOREIGN SCHEMA.
 */
static void LoadOAIServerInfo(OAIFdwState *state)
{
	state->requestRedirect = false;
	state->requestMaxRedirect = 0;
	state->maxretries = OAI_DEFAULT_MAX_RETRY;

	if (state->foreign_server)
	{
		ListCell *cell;

		foreach (cell, state->foreign_server->options)
		{
			DefElem *def = lfirst_node(DefElem, cell);

			if (strcmp(OAI_NODE_URL, def->defname) == 0)
				state->url = defGetString(def);
			else if (strcmp(OAI_NODE_METADATAPREFIX, def->defname) == 0)
				state->metadataPrefix = defGetString(def);
			else if (strcmp(OAI_SERVER_OPTION_HTTP_PROXY, def->defname) == 0)
			{
				state->proxy = defGetString(def);
				state->proxyType = OAI_SERVER_OPTION_HTTP_PROXY;
			}
			/* servers created before 1.13 may still have them; user mappings win */
			else if (strcmp(OAI_USERMAPPING_OPTION_PROXY_USER, def->defname) == 0)
				state->proxyUser = defGetString(def);
			else if (strcmp(OAI_USERMAPPING_OPTION_PROXY_PASSWORD, def->defname) == 0)
				state->proxyPassword = defGetString(def);
			else if (strcmp(OAI_SERVER_OPTION_CONNECT_TIMEOUT, def->defname) == 0)
			{
				char *tailpt;
				char *timeout_str = defGetString(def);
				state->connectTimeout = strtol(timeout_str, &tailpt, 0);
			}
			else if (strcmp(OAI_SERVER_OPTION_REQUEST_TIMEOUT, def->defname) == 0)
			{
				char *tailpt;
				char *timeout_str = defGetString(def);
				state->request_timeout = strtol(timeout_str, &tailpt, 0);
			}
			else if (strcmp(OAI_SERVER_OPTION_CONNECTRETRY, def->defname) == 0)
			{
				char *tailpt;
				char *maxretry_str = defGetString(def);
				state->maxretries = strtol(maxretry_str, &tailpt, 0);
			}
			else if (strcmp(OAI_SERVER_OPTION_REQUEST_REDIRECT, def->defname) == 0)
				state->requestRedirect = defGetBoolean(def);
			else if (strcmp(OAI_SERVER_OPTION_REQUEST_MAX_REDIRECT, def->defname) == 0)
			{
				char *tailpt;
				char *maxredirect_str = defGetString(def);
				state->requestMaxRedirect = strtol(maxredirect_str, &tailpt, 10);

				/* only servers created before the validator checked it */
				if (*tailpt != '\0' || state->requestMaxRedirect < 0)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							 errmsg("invalid %s: %s", OAI_SERVER_OPTION_REQUEST_MAX_REDIRECT, maxredirect_str)));
			}
			else
				elog(WARNING, "Invalid SERVER OPTION > '%s'", def->defname);
		}
	}
}

static void InitSession(OAIFdwState *state, RelOptInfo *baserel)
{
	elog(DEBUG2, "%s called", __func__);

	/*
	 * Loading SERVER OPTIONS
	 */
	LoadOAIServerInfo(state);

	/*
	 * Loading FOREIGN TABLE structure and OPTIONS
	 */
	LoadOAITableInfo(state);

	OAIRequestPlanner(state, baserel);
}

/*
 * SerializePlanData
 * -----------------
 * Converts parameters into Const variables, so that it can be properly
 * stored by the plan
 *
 * returns a List containing all converted parameterrs.
 */
static List *SerializePlanData(OAIFdwState *state)
{
	List *result = NIL;
	ListCell *cell;

	elog(DEBUG2, "%s called", __func__);

	result = lappend(result, IntToConst((int)state->numcols));
	result = lappend(result, IntToConst((int)state->numfdwcols));
	result = lappend(result, IntToConst((int)state->rowcount));
	result = lappend(result, IntToConst((int)state->requestRedirect));
	result = lappend(result, IntToConst((int)state->requestMaxRedirect));
	result = lappend(result, IntToConst((int)state->maxretries));
	result = lappend(result, IntToConst((int)state->connectTimeout));
	result = lappend(result, IntToConst((int)state->request_timeout));
	result = lappend(result, CStringToConst(state->identifier));
	result = lappend(result, CStringToConst(state->set));
	result = lappend(result, CStringToConst(state->url));
	result = lappend(result, CStringToConst(state->metadataPrefix));
	result = lappend(result, CStringToConst(state->proxy));
	result = lappend(result, CStringToConst(state->proxyType));
	result = lappend(result, CStringToConst(state->from));
	result = lappend(result, CStringToConst(state->until));
	result = lappend(result, CStringToConst(state->resumptionToken));
	result = lappend(result, CStringToConst(state->requestVerb));
	result = lappend(result, OidToConst(state->foreigntableid));

	elog(DEBUG2, "%s: serializing table with %d columns", __func__, state->numcols);
	for (int i = 0; i < state->numcols; ++i)
	{
		elog(DEBUG2, "%s: column name '%s'", __func__, state->oaiTable->cols[i]->name);
		result = lappend(result, CStringToConst(state->oaiTable->cols[i]->name));

		if (state->oaiTable->cols[i]->oai_node)
		{
			elog(DEBUG2, "%s: oai_node '%s'", __func__, state->oaiTable->cols[i]->oai_node);
			result = lappend(result, CStringToConst(state->oaiTable->cols[i]->oai_node));
		}
		else
		{
			elog(DEBUG2, "%s: column contains no oai_node", __func__);
			result = lappend(result, CStringToConst(""));
		}

		elog(DEBUG2, "%s: pgtypmod '%d'", __func__, state->oaiTable->cols[i]->pgtypmod);
		result = lappend(result, IntToConst(state->oaiTable->cols[i]->pgtypmod));

		elog(DEBUG2, "%s: pgattnum '%d'", __func__, state->oaiTable->cols[i]->pgattnum);
		result = lappend(result, IntToConst(state->oaiTable->cols[i]->pgattnum));

		elog(DEBUG2, "%s: pgtype '%u'", __func__, state->oaiTable->cols[i]->pgtype);
		result = lappend(result, OidToConst(state->oaiTable->cols[i]->pgtype));
	}

	/* what the expressions in fdw_exprs set, in the same order */
	result = lappend(result, IntToConst(list_length(state->pushdown_kinds)));
	foreach (cell, state->pushdown_kinds)
		result = lappend(result, IntToConst(lfirst_int(cell)));

	elog(DEBUG2, "%s exit", __func__);
	return result;
}

/*
 * DeserializePlanData
 * -------------------
 * Converts Const variables created using SerializePlanData back
 * into pointers.
 *
 * IMPORTANT: fields must be extracted in the EXACT same order they were
 * appended in SerializePlanData(); any mismatch silently produces wrong values.
 *
 * returns a OAIFdwState containing all converted parameterrs.
 */
static struct OAIFdwState *DeserializePlanData(List *list)
{
	struct OAIFdwState *state = (struct OAIFdwState *)palloc0(sizeof(OAIFdwState));
	ListCell *cell = list_head(list);
	int numkinds;

	elog(DEBUG2, "%s called", __func__);

	state->numcols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	state->pagesize = 0;
	cell = list_next(list, cell);

	state->numfdwcols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	state->rowcount = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	state->requestRedirect = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	state->requestMaxRedirect = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	state->maxretries = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	state->connectTimeout = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	state->request_timeout = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	state->identifier = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->set = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->url = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->metadataPrefix = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->proxy = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->proxyType = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->from = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->until = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->resumptionToken = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->requestVerb = ConstToCString(lfirst(cell));
	cell = list_next(list, cell);

	state->foreigntableid = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	elog(DEBUG2, "  %s: deserializing table with %d columns", __func__, state->numcols);
	state->oaiTable = (struct OAIfdwTable *)palloc0(sizeof(struct OAIfdwTable));
	state->oaiTable->cols = (struct OAIfdwColumn **)palloc0(sizeof(struct OAIfdwColumn *) * state->numcols);

	for (int i = 0; i < state->numcols; ++i)
	{
		state->oaiTable->cols[i] = (struct OAIfdwColumn *)palloc0(sizeof(struct OAIfdwColumn));

		state->oaiTable->cols[i]->name = ConstToCString(lfirst(cell));
		cell = list_next(list, cell);
		elog(DEBUG2, "  %s: column name '%s'", __func__, state->oaiTable->cols[i]->name);

		state->oaiTable->cols[i]->oai_node = ConstToCString(lfirst(cell));
		cell = list_next(list, cell);

		state->oaiTable->cols[i]->pgtypmod = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);

		state->oaiTable->cols[i]->pgattnum = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);

		state->oaiTable->cols[i]->pgtype = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
	}

	numkinds = DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	for (int i = 0; i < numkinds; ++i)
	{
		state->pushdown_kinds = lappend_int(state->pushdown_kinds, DatumGetInt32(((Const *)lfirst(cell))->constvalue));
		cell = list_next(list, cell);
	}

	elog(DEBUG2, "%s exit", __func__);
	return state;
}

/*
 * CStringToConst
 * -----------------
 * Wraps a C string in a Const node
 *
 * str: the C string to wrap (NULL produces a null Const)
 *
 * returns a Const node wrapping the given string
 */
static Const *CStringToConst(const char *str)
{
	if (str == NULL)
		return makeNullConst(TEXTOID, -1, InvalidOid);
	else
		return makeConst(TEXTOID, -1, InvalidOid, -1, PointerGetDatum(cstring_to_text(str)), false, false);
}

/*
 * ConstToCString
 * -----------------
 * Extracts a string from a Const
 *
 * constant: the Const node to extract from
 *
 * returns a palloc'ed copy.
 */
static char *ConstToCString(Const *constant)
{
	Assert(constant != NULL);

	if (constant->constisnull)
		return NULL;
	else
		return text_to_cstring(DatumGetTextP(constant->constvalue));
}