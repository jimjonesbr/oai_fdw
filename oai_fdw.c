
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

//#include "access/table.h"
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
#include <curl/curl.h>
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

#define OAI_FDW_VERSION "1.0.0dev"
#define OAI_REQUEST_LISTRECORDS "ListRecords"
#define OAI_REQUEST_LISTIDENTIFIERS "ListIdentifiers"
#define OAI_REQUEST_IDENTIFY "Identify"
#define OAI_REQUEST_LISTSETS "ListSets"
#define OAI_REQUEST_GETRECORD "GetRecord"
#define OAI_REQUEST_LISTMETADATAFORMATS "ListMetadataFormats"
#define OAI_REQUEST_LISTSETS "ListSets"

#define OAI_XML_ROOT_ELEMENT "OAI-PMH"
#define OAI_NODE_IDENTIFIER "identifier"
#define OAI_NODE_CONTENT "content"
#define OAI_NODE_DATESTAMP "datestamp"
#define OAI_NODE_SETSPEC "setspec"
#define OAI_NODE_METADATAPREFIX "metadataprefix"
#define OAI_NODE_URL "url"
#define OAI_NODE_FROM "from"
#define OAI_NODE_UNTIL "until"
#define OAI_NODE_STATUS "status"
#define OAI_NODE_OPTION "oai_node"

#define OAI_ERROR_ID_DOES_NOT_EXIST "idDoesNotExist"
#define OAI_ERROR_NO_RECORD_MATCH "noRecordsMatch"

#define OAI_SUCCESS 0
#define OAI_FAIL 1
#define OAI_UNKNOWN_REQUEST 2
#define ERROR_CODE_MISSING_PREFIX 1

PG_MODULE_MAGIC;

typedef struct oai_fdw_state {

	int 	current_row;
	int 	numcols;
	int 	numfdwcols;
	int		rowcount;
	char	*completeListSize;
	char	*identifier;
	char 	*document;
	char 	*date;
	char 	*set;
	char 	*url;
	char 	*from;
	char 	*until;
	char 	*metadataPrefix;
	char 	*resumptionToken;
	char	*requestType;
	char	*current_identifier;

	Oid 		foreigntableid;
	List 		*columnlist;
	xmlNodePtr 	xmlroot;

} oai_fdw_state;

typedef struct oai_Record {

	char *identifier;
	char *content;
	char *datestamp;
	char *metadataPrefix;
	char *completeListSize;
	bool isDeleted;
	ArrayType *setsArray;

} oai_Record;


typedef struct oai_fdw_TableOptions {

	Oid foreigntableid;
	List *columnlist;

	int numcols;
	int numfdwcols;
	char *metadataPrefix;
	char *from;
	char *until;
	char *url;
	char *set;
	char* identifier;
	char* requestType;

} oai_fdw_TableOptions;

typedef struct OAIMetadataFormat {

	char *metadataPrefix;
	char *schema;
	char *metadataNamespace;

} OAIMetadataFormat;

typedef struct OAISet {

	char *setSpec;
	char *setName;

} OAISet;

typedef struct OAIIdentityNode {

	char *name;
	char *description;

} OAIIdentityNode;

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

static struct OAIFdwOption valid_options[] =
{

	{OAI_NODE_URL, ForeignServerRelationId, true, false},
	{OAI_NODE_METADATAPREFIX, ForeignServerRelationId, false, false},

	{OAI_NODE_IDENTIFIER, ForeignTableRelationId, false, false},
	{OAI_NODE_METADATAPREFIX, ForeignTableRelationId, true, false},
	{OAI_NODE_SETSPEC, ForeignTableRelationId, false, false},
	{OAI_NODE_FROM, ForeignTableRelationId, false, false},
	{OAI_NODE_UNTIL, ForeignTableRelationId, false, false},

	{OAI_NODE_OPTION, AttributeRelationId, true, false},

	/* EOList marker */
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

void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
void oai_fdw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
ForeignScan *oai_fdw_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
void oai_fdw_BeginForeignScan(ForeignScanState *node, int eflags);
TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node);
void oai_fdw_ReScanForeignScan(ForeignScanState *node);
void oai_fdw_EndForeignScan(ForeignScanState *node);
static List* oai_fdw_ImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid serverOid);

void init_string(struct string *s);
size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s);
void appendTextArray(ArrayType **array, char* text_element) ;
int executeOAIRequest(oai_fdw_state *state, struct string *xmlResponse);
void listRecordsRequest(oai_fdw_state *state);
void createOAITuple(TupleTableSlot *slot, oai_fdw_state *state, oai_Record *oai );
oai_Record *fetchNextRecord(TupleTableSlot *slot, oai_fdw_state *state);
int LoadOAIRecords(oai_fdw_state *state);

static void deparseExpr(Expr *expr, oai_fdw_TableOptions *opts);
static char *datumToString(Datum datum, Oid type);

static char *getColumnOption(Oid foreigntableid, int16 attnum);
static void deparseWhereClause(List *conditions, oai_fdw_TableOptions *opts);
static void deparseSelectColumns(oai_fdw_TableOptions *opts);
static void requestPlanner(oai_fdw_TableOptions *opts, ForeignTable *ft, RelOptInfo *baserel);
static char *deparseTimestamp(Datum datum);
List *GetMetadataFormats(char *url);
List *GetIdentity(char *url);
List *GetSets(char *url);
int check_url(char *url);

Datum oai_fdw_version(PG_FUNCTION_ARGS) {

	StringInfoData buffer;
	initStringInfo(&buffer);

	appendStringInfo(&buffer,"oai fdw = %s,",OAI_FDW_VERSION);
	appendStringInfo(&buffer," libxml = %s,",LIBXML_DOTTED_VERSION);
	appendStringInfo(&buffer," libcurl = %s",curl_version());

    PG_RETURN_TEXT_P(cstring_to_text(buffer.data));
}

Datum oai_fdw_identity(PG_FUNCTION_ARGS) {

	ForeignServer *server;
	text *srvname_text = PG_GETARG_TEXT_P(0);
	const char * srvname = text_to_cstring(srvname_text);

	FuncCallContext     *funcctx;
	AttInMetadata       *attinmeta;
	TupleDesc            tupdesc;
	int                  call_cntr;
	int                  max_calls;
	MemoryContext        oldcontext;
	List                *identity;

	if (SRF_IS_FIRSTCALL())	{

		char *url = NULL;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		server = GetForeignServerByName(srvname, true);

		if(server) {

			ListCell   *cell;

			foreach(cell, server->options) {

				DefElem *def = lfirst_node(DefElem, cell);
				if(strcmp(def->defname,"url")==0) url = defGetString(def);

			}

		} else {

			ereport(ERROR,
			  (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
			   errmsg("FOREIGN SERVER does not exist: '%s'",srvname)));



		}

		identity = GetIdentity(url);
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

		OAIIdentityNode *set = (OAIIdentityNode *) list_nth((List*) funcctx->user_fctx, call_cntr);

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

List *GetIdentity(char *url) {

	struct string xmlStream;
	int oaiExecuteResponse;
	oai_fdw_state *state = (oai_fdw_state *) palloc0(sizeof(oai_fdw_state));
	List *result = NIL;

	state->url = url;
	state->requestType = OAI_REQUEST_IDENTIFY;

	oaiExecuteResponse = executeOAIRequest(state,&xmlStream);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlNodePtr xmlroot = NULL;
		xmlDocPtr xmldoc;
		xmlNodePtr oai_root;
		xmlNodePtr Identity;

		xmldoc = xmlReadMemory(xmlStream.ptr, strlen(xmlStream.ptr), NULL, NULL, XML_PARSE_SAX1);

		if (!xmldoc || (xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {
			xmlFreeDoc(xmldoc);
			xmlCleanupParser();
			elog(WARNING,"invalid MARC21/XML document.");
		}

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next) {

			if (oai_root->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(oai_root->name, (xmlChar*) "Identify") != 0) continue;

			for (Identity = oai_root->children; Identity != NULL; Identity = Identity->next) {

				OAIIdentityNode *node = (OAIIdentityNode *)palloc0(sizeof(OAIIdentityNode));

				if (Identity->type != XML_ELEMENT_NODE)	continue;

				node->name = (char*) Identity->name;
				node->description = (char*) xmlNodeGetContent(Identity);

				result = lappend(result, node);

			}

		}

	}

	return result;

}


List *GetSets(char *url) {

	struct string xmlStream;
	int oaiExecuteResponse;
	oai_fdw_state *state = (oai_fdw_state *) palloc0(sizeof(oai_fdw_state));
	List *result = NIL;

	state->url = url;
	state->requestType = OAI_REQUEST_LISTSETS;

	oaiExecuteResponse = executeOAIRequest(state,&xmlStream);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlNodePtr xmlroot = NULL;
		xmlDocPtr xmldoc;
		xmlNodePtr oai_root;
		xmlNodePtr ListSets;
		xmlNodePtr SetElement;

		xmldoc = xmlReadMemory(xmlStream.ptr, strlen(xmlStream.ptr), NULL, NULL, XML_PARSE_SAX1);

		if (!xmldoc || (xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {
			xmlFreeDoc(xmldoc);
			xmlCleanupParser();
			elog(WARNING,"invalid MARC21/XML document.");
		}

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next) {

			if (oai_root->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(oai_root->name, (xmlChar*) "ListSets") != 0) continue;

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

	return result;
}

List *GetMetadataFormats(char *url) {

	struct string xmlStream;
	int oaiExecuteResponse;
	oai_fdw_state *state = (oai_fdw_state *) palloc0(sizeof(oai_fdw_state));
	List *result = NIL;

	state->url = url;
	state->requestType = OAI_REQUEST_LISTMETADATAFORMATS;

	oaiExecuteResponse = executeOAIRequest(state,&xmlStream);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlNodePtr xmlroot = NULL;
		xmlDocPtr xmldoc;
		xmlNodePtr oai_root;
		xmlNodePtr ListMetadataFormats;
		xmlNodePtr MetadataElement;

		xmldoc = xmlReadMemory(xmlStream.ptr, strlen(xmlStream.ptr), NULL, NULL, XML_PARSE_SAX1);

		if (!xmldoc || (xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {
			xmlFreeDoc(xmldoc);
			xmlCleanupParser();
			elog(WARNING,"invalid MARC21/XML document.");
		}

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next) {

			if (oai_root->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(oai_root->name, (xmlChar*) "ListMetadataFormats") != 0) continue;

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

	return result;
}



Datum oai_fdw_handler(PG_FUNCTION_ARGS) {

	FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	fdwroutine->GetForeignRelSize = oai_fdw_GetForeignRelSize;
	fdwroutine->GetForeignPaths = oai_fdw_GetForeignPaths;
	fdwroutine->GetForeignPlan = oai_fdw_GetForeignPlan;
	fdwroutine->BeginForeignScan = oai_fdw_BeginForeignScan;
	fdwroutine->IterateForeignScan = oai_fdw_IterateForeignScan;
	fdwroutine->ReScanForeignScan = oai_fdw_ReScanForeignScan;
	fdwroutine->EndForeignScan = oai_fdw_EndForeignScan;
	fdwroutine->ImportForeignSchema = oai_fdw_ImportForeignSchema;

	PG_RETURN_POINTER(fdwroutine);

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
						 errmsg("empty value for option '%s'",opt->optname)));

				}

				if(strcmp(opt->optname, OAI_NODE_URL)==0){

					if(check_url(defGetString(def))!=CURLE_OK) {

						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							 errmsg("invalid %s: '%s'",OAI_NODE_URL, defGetString(def))));

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

//				opt->optfound = true;
//				optfound = true;

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

/*cURL code*/
void init_string(struct string *s) {
	s->len = 0;
	s->ptr = malloc(s->len+1);
	if (s->ptr == NULL) {
		fprintf(stderr, "malloc() failed\n");
		exit(EXIT_FAILURE);
	}
	s->ptr[0] = '\0';
}

/*cURL code*/
size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s) {

	size_t new_len = s->len + size*nmemb;
	s->ptr = realloc(s->ptr, new_len+1);
	if (s->ptr == NULL) {
		fprintf(stderr, "realloc() failed\n");
		exit(EXIT_FAILURE);
	}
	memcpy(s->ptr+s->len, ptr, size*nmemb);
	s->ptr[new_len] = '\0';
	s->len = new_len;

	return size*nmemb;

}

int check_url(char *url)
{
    CURL *curl;
    CURLcode response;

    curl = curl_easy_init();

    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url);

        /* don't write output to stdout */
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1);

        /* Perform the request */
        response = curl_easy_perform(curl);

        /* always cleanup */
        curl_easy_cleanup(curl);

    } else {

    	return CURLE_FAILED_INIT;
    }

    return response;

}

Datum oai_fdw_listSets(PG_FUNCTION_ARGS) {

	ForeignServer *server;
	text *srvname_text = PG_GETARG_TEXT_P(0);
	char *srvname = text_to_cstring(srvname_text);
	MemoryContext   oldcontext;
	FuncCallContext     *funcctx;
	AttInMetadata       *attinmeta;
	TupleDesc            tupdesc;
	int                  call_cntr;
	int                  max_calls;

	if (SRF_IS_FIRSTCALL())	{

		List *sets;
		char *url = NULL;
		server = GetForeignServerByName(srvname, true);
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if(server) {

			ListCell   *cell;

			foreach(cell, server->options) {

				DefElem *def = lfirst_node(DefElem, cell);
				if(strcmp(def->defname,"url")==0) url = defGetString(def);

			}

		} else {

			ereport(ERROR,(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
					       errmsg("FOREIGN SERVER does not exist: '%s'",srvname)));

		}

		sets = GetSets(url);

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

	ForeignServer *server;
	text *srvname_text = PG_GETARG_TEXT_P(0);
	const char * srvname = text_to_cstring(srvname_text);

	FuncCallContext     *funcctx;
	int                  call_cntr;
	int                  max_calls;
	TupleDesc            tupdesc;
	AttInMetadata       *attinmeta;
	MemoryContext  oldcontext;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{

		char *url = NULL;
		List *formats;
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		server = GetForeignServerByName(srvname, true);

		if(server) {

			ListCell   *cell;

			foreach(cell, server->options) {

				DefElem *def = lfirst_node(DefElem, cell);

				if(strcmp(def->defname,"url")==0) url = defGetString(def);

			}

		} else {

			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
					 errmsg("FOREIGN SERVER does not exist: '%s'",srvname)));

		}

		formats = GetMetadataFormats(url);
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

int executeOAIRequest(oai_fdw_state *state, struct string *xmlResponse) {

	CURL *curl;
	CURLcode res;
	StringInfoData url_bufffer;
	initStringInfo(&url_bufffer);

	elog(DEBUG1,"executeOAIRequest called: url > %s ",state->url);

	appendStringInfo(&url_bufffer,"verb=%s",state->requestType);

	if(strcmp(state->requestType,OAI_REQUEST_LISTRECORDS)==0) {

		if(state->set) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'set' > %s",state->requestType,state->set);
			appendStringInfo(&url_bufffer,"&set=%s",state->set);

		}

		if(state->from) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'from' > %s",state->requestType,state->from);
			appendStringInfo(&url_bufffer,"&from=%s",state->from);

		}

		if(state->until) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'until' > %s",state->requestType,state->until);
			appendStringInfo(&url_bufffer,"&until=%s",state->until);

		}

		if(state->metadataPrefix) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'metadataPrefix' > %s",state->requestType,state->metadataPrefix);
			appendStringInfo(&url_bufffer,"&metadataPrefix=%s",state->metadataPrefix);

		}

		if(state->resumptionToken) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'resumptionToken' > %s",state->requestType,state->resumptionToken);
			resetStringInfo(&url_bufffer);
			appendStringInfo(&url_bufffer,"verb=%s&resumptionToken=%s",state->requestType,state->resumptionToken);

			state->resumptionToken = NULL;

		}

	} else if(strcmp(state->requestType,OAI_REQUEST_GETRECORD)==0) {

		if(state->identifier) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'identifier' > %s",state->requestType,state->identifier);
			appendStringInfo(&url_bufffer,"&identifier=%s",state->identifier);

		}

		if(state->metadataPrefix) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'metadataPrefix' > %s",state->requestType,state->metadataPrefix);
			appendStringInfo(&url_bufffer,"&metadataPrefix=%s",state->metadataPrefix);

		}

	} else if(strcmp(state->requestType,OAI_REQUEST_LISTIDENTIFIERS)==0) {


		if(state->set) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'set' > %s",state->requestType,state->set);
			appendStringInfo(&url_bufffer,"&set=%s",state->set);

		}

		if(state->from) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'from' > %s",state->requestType,state->from);
			appendStringInfo(&url_bufffer,"&from=%s",state->from);


		}

		if(state->until) {

			elog(DEBUG2,"  %s: appending 'until' > %s",state->requestType,state->until);
			appendStringInfo(&url_bufffer,"&until=%s",state->until);

		}

		if(state->metadataPrefix) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'metadataPrefix' > %s",state->requestType,state->metadataPrefix);
			appendStringInfo(&url_bufffer,"&metadataPrefix=%s",state->metadataPrefix);

		}


		if(state->resumptionToken) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'resumptionToken' > %s",state->requestType,state->resumptionToken);
			resetStringInfo(&url_bufffer);
			appendStringInfo(&url_bufffer,"verb=%s&resumptionToken=%s",state->requestType,state->resumptionToken);

			state->resumptionToken = NULL;

		}

	} else {

		if(strcmp(state->requestType,OAI_REQUEST_LISTMETADATAFORMATS)!=0 &&
		   strcmp(state->requestType,OAI_REQUEST_LISTSETS)!=0 &&
		   strcmp(state->requestType,OAI_REQUEST_IDENTIFY)!=0) {

			return OAI_UNKNOWN_REQUEST;

		}

	}


	elog(DEBUG1,"  executeOAIRequest (%s): url build > %s?%s",state->requestType,state->url,url_bufffer.data);

	curl = curl_easy_init();

	if(curl) {

		init_string(xmlResponse);

		curl_easy_setopt(curl, CURLOPT_URL, state->url);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, url_bufffer.data);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, xmlResponse);
		curl_easy_setopt(curl, CURLOPT_FAILONERROR, true);

		res = curl_easy_perform(curl);

		if (res != CURLE_OK) {

			long http_code = 0;
			curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_code);
			curl_easy_cleanup(curl);

			ereport(ERROR,
				   (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					errmsg("OAI %s request failed. The HTTP request '%s?%s' returned an error: %ld",state->requestType,state->url,url_bufffer.data,http_code)));

		}

	} else {

		elog(DEBUG2, "  => %s: curl failed",state->requestType);
		return OAI_FAIL;

	}

	curl_easy_cleanup(curl);

	elog(DEBUG2, "  => %s: curl http_return > '%d'",state->requestType, res);

	return OAI_SUCCESS;


}


void listRecordsRequest(oai_fdw_state *state) {

	struct string xmlStream;
	int oaiExecuteResponse;

	elog(DEBUG1,"ListRecordsRequest/LoadData called");

	oaiExecuteResponse = executeOAIRequest(state,&xmlStream);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlDocPtr xmldoc;
		xmlNodePtr recordsList;
		xmlNodePtr record;

		xmlInitParser();

		xmldoc = xmlReadMemory(xmlStream.ptr, strlen(xmlStream.ptr), NULL, NULL, XML_PARSE_SAX1);

		if (!xmldoc || (state->xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {

			xmlFreeDoc(xmldoc);
			xmlCleanupParser();

			ereport(ERROR,
			  (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			  (errmsg("Invalid XML response for URL '%s'",state->url))));

		}

		for (recordsList = state->xmlroot->children; recordsList != NULL; recordsList = recordsList->next) {

			if (recordsList->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(recordsList->name, (xmlChar*)state->requestType)==0) {

				if (!xmldoc || (state->xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {

					xmlFreeDoc(xmldoc);
					xmlCleanupParser();

				}

				for (record = recordsList->children; record!= NULL; record = record->next) {

					if (record->type != XML_ELEMENT_NODE) continue;

					if (xmlStrcmp(record->name, (xmlChar*)"resumptionToken")==0){

						if(xmlGetProp(record, (xmlChar*) "completeListSize")) {

							state->completeListSize = (char*) xmlGetProp(record, (xmlChar*) "completeListSize");

						}

					}

				}

			} else if (xmlStrcmp(recordsList->name, (xmlChar*)"error")==0) {

				char *errorMessage = (char*)xmlNodeGetContent(recordsList);
				char *errorCode = (char*) xmlGetProp(recordsList, (xmlChar*) "code");

				if(strcmp(errorCode,OAI_ERROR_ID_DOES_NOT_EXIST)==0 ||
				   strcmp(errorCode,OAI_ERROR_NO_RECORD_MATCH)==0) {

					state->xmlroot = NULL;
					ereport(WARNING,
							(errcode(ERRCODE_NO_DATA_FOUND),
							errmsg("OAI %s: %s",errorCode,errorMessage)));

				} else {

					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							errmsg("OAI %s: %s",errorCode,errorMessage)));

				}


			}

		}

	}

}

static void requestPlanner(oai_fdw_TableOptions *opts, ForeignTable *ft, RelOptInfo *baserel) {

	List *conditions = baserel->baserestrictinfo;
	bool hasContentForeignColumn = false;

#if PG_VERSION_NUM < 130000
	Relation rel = heap_open(ft->relid, NoLock);
#else
	Relation rel = table_open(ft->relid, NoLock);;
#endif

	//Relation rel = table_open(ft->relid, NoLock);


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
		elog(DEBUG1,"  requestPlanner: the foreign table '%s' has no 'content' OAI attribute. Request type set to '%s'",
				NameStr(rel->rd_rel->relname),OAI_REQUEST_LISTIDENTIFIERS);

	}

	deparseWhereClause(conditions,opts);

	if(opts->numfdwcols!=0) {

		opts->columnlist = baserel->reltarget->exprs;
		deparseSelectColumns(opts);

	}


	//table_close(rel, NoLock);
#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

}


static char *getColumnOption(Oid foreigntableid, int16 attnum) {

	List *options;
	char *optionValue = NULL;

#if PG_VERSION_NUM < 130000
	Relation rel = heap_open(foreigntableid, NoLock);
#else
	Relation rel = table_open(foreigntableid, NoLock);;
#endif

	//Relation rel = table_open(foreigntableid, NoLock);


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

	//StringInfoData result;
	regproc typoutput;
	HeapTuple tuple;
	char *str;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));

	if (!HeapTupleIsValid(tuple)) elog(ERROR, "cache lookup failed for type %u", type);

	typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;

	ReleaseSysCache(tuple);

	elog(DEBUG1,"datumToString: type > %u",type);

	switch (type) {

	case TEXTOID:
	case VARCHAROID:
		str =  DatumGetCString(OidFunctionCall1(typoutput, datum));
		break;
	default:
		return NULL;
	}

	return str;

}


static void deparseExpr(Expr *expr, oai_fdw_TableOptions *opts){

	OpExpr *oper;
	Var *varleft;
	//Var *varright;
	HeapTuple tuple;
//	char *opername, *left, *right, *arg, oprkind;
//	Oid leftargtype, rightargtype;
	char *opername;
	//Oid leftargtype;


	char* leftargOption;

	elog(DEBUG1,"deparseExpr: expr->type %u",expr->type);

	switch (expr->type)	{

	case T_Var:

		elog(DEBUG1,"  deparseExpr: T_Var");
		varleft = (Var *)expr;
		leftargOption = getColumnOption(opts->foreigntableid,varleft->varattno);

		break;

	case T_OpExpr:

		elog(DEBUG1,"  deparseExpr: case T_OpExpr");
		oper = (OpExpr *)expr;

		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper->opno));

		if (!HeapTupleIsValid(tuple)) elog(ERROR, "cache lookup failed for operator %u", oper->opno);

		opername = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);
		//oprkind = ((Form_pg_operator)GETSTRUCT(tuple))->oprkind;
		//leftargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprleft;
		//rightargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprright;
		//schema = ((Form_pg_operator)GETSTRUCT(tuple))->oprnamespace;

//		return opername;


		ReleaseSysCache(tuple);


		elog(DEBUG1,"  deparseExpr: opername > %s",opername);

		varleft  = (Var *) linitial(oper->args);
		leftargOption = getColumnOption(opts->foreigntableid,varleft->varattno);

		if (strcmp(opername, "=") == 0){

			//varright = (Var *) lsecond(oper->args);

			if (strcmp(leftargOption,OAI_NODE_IDENTIFIER) ==0 && (varleft->vartype == TEXTOID || varleft->vartype == VARCHAROID)) {

				Const *constant = (Const*) lsecond(oper->args);

				opts->requestType = OAI_REQUEST_GETRECORD;
				opts->identifier = datumToString(constant->constvalue,constant->consttype);

				elog(DEBUG1,"  deparseExpr: request type set to '%s' with identifier '%s'",OAI_REQUEST_GETRECORD,opts->identifier);

			}


			if (strcmp(leftargOption,OAI_NODE_DATESTAMP) ==0 && (varleft->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->from = deparseTimestamp(constant->constvalue);
				opts->until = deparseTimestamp(constant->constvalue);

			}

			if (strcmp(leftargOption,OAI_NODE_METADATAPREFIX) ==0 && (varleft->vartype == TEXTOID || varleft->vartype == VARCHAROID)) {

				Const *constant = (Const*) lsecond(oper->args);

				opts->metadataPrefix = datumToString(constant->constvalue,constant->consttype);

				elog(DEBUG1,"  deparseExpr: metadataPrefix set to '%s'",opts->metadataPrefix);

			}


		}

		if (strcmp(opername, ">=") == 0 || strcmp(opername, ">") == 0){

			if (strcmp(leftargOption,OAI_NODE_DATESTAMP) ==0 && (varleft->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->from = deparseTimestamp(constant->constvalue);

			}

		}

		if (strcmp(opername, "<=") == 0 || strcmp(opername, "<") == 0){

			if (strcmp(leftargOption,OAI_NODE_DATESTAMP) ==0 && (varleft->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->until = deparseTimestamp(constant->constvalue);

			}


		}

		if (strcmp(opername, "<@") == 0 || strcmp(opername, "@>") == 0 || strcmp(opername, "&&") == 0 ){

			if (strcmp(leftargOption,OAI_NODE_SETSPEC) ==0 && (varleft->vartype == TEXTARRAYOID || varleft->vartype == VARCHARARRAYOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				ArrayType *array = (ArrayType*) constant->constvalue;

				int numitems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

				if(numitems > 1) {

					elog(WARNING,"The OAI standard requests do not support multiple '%s' attributes. This filter will be applied AFTER the OAI request.",OAI_NODE_SETSPEC);
					elog(DEBUG1,"  deparseExpr: clearing '%s' attribute.",OAI_NODE_SETSPEC);
					opts->set = NULL;

				} else if (numitems == 1) {


					bool isnull;
					Datum value;

					ArrayIterator iterator = array_create_iterator(array,0,NULL);


					while (array_iterate(iterator, &value, &isnull)) {

						elog(DEBUG1,"  deparseExpr: setSpec set to '%s'",datumToString(value,TEXTOID));
						opts->set = datumToString(value,TEXTOID);

					}

					array_free_iterator(iterator);

				}

			}

		}

		break;

	default:
		elog(DEBUG1,"NONE");
		break;

	}

}


static void deparseSelectColumns(oai_fdw_TableOptions *opts){

	ListCell *cell;
	Var *variable;
	bool hasContentForeignColumn = false;
	char *columnOption;

	elog(DEBUG1,"deparseSelectColumns called");

	foreach(cell, opts->columnlist) {

		Expr * expr = (Expr *) lfirst(cell);

		elog(DEBUG1,"  deparseSelectColumns: evaluating expr->type = %u",expr->type);

		if(expr->type == T_Var)	{

			variable = (Var *)expr;
			columnOption = getColumnOption(opts->foreigntableid,variable->varattno);

			//elog(DEBUG1,"  deparseSelectColumns: evaluating variable->varattnosyn = %u",variable->varattnosyn);

			if(strcmp(columnOption,OAI_NODE_CONTENT)==0) {

				elog(DEBUG1,"  deparseSelectColumns: content OAI option detected");
				hasContentForeignColumn = true;

			}


		} else {



		}


	}

	if(!hasContentForeignColumn) {
		opts->requestType = OAI_REQUEST_LISTIDENTIFIERS;
		elog(DEBUG1,"  deparseSelectColumns: no content OAI option detected. Request type set to '%s'",OAI_REQUEST_LISTIDENTIFIERS);
	}

}


static char *deparseTimestamp(Datum datum) {

	struct pg_tm datetime_tm;
	//int32 tzoffset;
	fsec_t datetime_fsec;
	StringInfoData s;

	/* get the parts */
	//tzoffset = 0;
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


static void deparseWhereClause(List *conditions, oai_fdw_TableOptions *opts){

	ListCell *cell;
	int i = 0;

	foreach(cell, conditions) {

		Expr  *expr = (Expr *) lfirst(cell);

		elog(DEBUG1,"round %d, expr->type %u",++i,expr->type);

		/* extract WHERE clause from RestrictInfo */
		if (IsA(expr, RestrictInfo)) {

			RestrictInfo *ri = (RestrictInfo *) expr;
			expr = ri->clause;

		}

		deparseExpr(expr,opts);

	}


}

void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	ForeignTable *ft = GetForeignTable(foreigntableid);
	ForeignServer *server = GetForeignServer(ft->serverid);

	oai_fdw_TableOptions *opts = (oai_fdw_TableOptions *)palloc0(sizeof(oai_fdw_TableOptions));
	ListCell *cell;

	elog(DEBUG1,"oai_fdw_GetForeignRelSize: requestType > %s", opts->requestType);

	foreach(cell, server->options) {

		DefElem *def = lfirst_node(DefElem, cell);

		if (strcmp("url", def->defname) == 0) {

			opts->url = defGetString(def);

		} else if (strcmp("metadataprefix", def->defname) == 0) {

			opts->metadataPrefix = defGetString(def);

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

	requestPlanner(opts, ft, baserel);
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

	List *fdw_private;
	oai_fdw_TableOptions *opts = baserel->fdw_private;
	oai_fdw_state *state = (oai_fdw_state *) palloc0(sizeof(oai_fdw_state));

	state->url = opts->url;
	state->set = opts->set;
	state->from = opts->from;
	state->until = opts->until;
	state->metadataPrefix = opts->metadataPrefix;
	state->foreigntableid = opts->foreigntableid;
	state->numcols = opts->numcols;
	state->requestType = opts->requestType;
	state->identifier = opts->identifier;
	state->columnlist = opts->columnlist;
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

void oai_fdw_BeginForeignScan(ForeignScanState *node, int eflags) {

	ForeignScan *fs = (ForeignScan *) node->ss.ps.plan;
	oai_fdw_state *state = (oai_fdw_state *) linitial(fs->fdw_private);

	if(eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

	state->current_row = 0;

	node->fdw_state = state;

}

oai_Record *fetchNextRecord(TupleTableSlot *slot, oai_fdw_state *state) {

	xmlNodePtr headerList;
	xmlNodePtr header;
	xmlNodePtr rootNode;
	xmlNodePtr rec;
	xmlNodePtr metadata;
	oai_Record *oai = (oai_Record*) palloc0(sizeof(oai_Record));
	bool getNext = false;
    bool eof = false;

    if(!state->xmlroot) return NULL;

	oai->metadataPrefix = state->metadataPrefix;
	oai->isDeleted = false;

	elog(DEBUG1,"fetchNextRecord for '%s'",state->requestType);

	xmlInitParser(); //????????????????

	if(strcmp(state->requestType,OAI_REQUEST_LISTIDENTIFIERS)==0) {

		for (rootNode = state->xmlroot->children; rootNode!= NULL; rootNode = rootNode->next) {

			if (rootNode->type != XML_ELEMENT_NODE) continue;
			if (xmlStrcmp(rootNode->name, (xmlChar*)state->requestType)!=0) continue;

			for (header = rootNode->children; header != NULL; header = header->next) {

				if (header->type != XML_ELEMENT_NODE) continue;

				if(header->next) {

					if (xmlStrcmp(header->next->name, (xmlChar*)"resumptionToken")==0) {

						char *token = (char*)xmlNodeGetContent(header->next);

						if(strlen(token) != 0) {

							state->resumptionToken = token;
							elog(DEBUG1,"  fetchNextRecord: (%s) token detected in current page > %s",state->requestType,token);

						} else {

							elog(DEBUG1,"  fetchNextRecord: empty token in '%s' (EOF)",state->requestType);
							eof = true;

						}

					}

				}

				if (xmlStrcmp(header->name, (xmlChar*) "header") != 0) continue;

				if(xmlGetProp(header, (xmlChar*) OAI_NODE_STATUS)){
					char *status = (char*) xmlGetProp(header, (xmlChar*) OAI_NODE_STATUS);
					if (strcmp(status,"deleted")) oai->isDeleted = true;
				}

				for (headerList = header->children; headerList!= NULL; headerList = headerList->next) {

					char *node_content;

					if (headerList->type != XML_ELEMENT_NODE) continue;

					node_content = (char*)xmlNodeGetContent(headerList);

					if (xmlStrcmp(headerList->name, (xmlChar*) "identifier")==0)  oai->identifier = node_content;

					if (xmlStrcmp(headerList->name, (xmlChar*) "setSpec")==0) appendTextArray(&oai->setsArray,node_content);

					if (xmlStrcmp(headerList->name, (xmlChar*) "datestamp")==0)	oai->datestamp= node_content;

				}

				if(!state->current_identifier) {

					state->current_identifier = oai->identifier;
					elog(DEBUG1,"  fetchNextRecord: (%s) setting first current identifier > %s",state->requestType,state->current_identifier );
					return oai;

				}

				if (strcmp(state->current_identifier,oai->identifier)==0){

					elog(DEBUG1,"  fetchNextRecord: (%s) there is a match! next iteration will be returned > %s",state->requestType,oai->identifier);
					oai->setsArray = NULL;
					getNext = true;

					if(eof) return NULL;

				} else if(getNext == true) {

					state->current_identifier = oai->identifier;
					elog(DEBUG1,"  fetchNextRecord: (%s) getNext=true > %s",state->requestType,state->current_identifier);
					return oai;

				}

				if(!header->next && !state->resumptionToken) {
					elog(DEBUG1,"  fetchNextRecord: (%s) wrapping up .. ",state->requestType);
					return NULL;
				}


			}

		}

	}


	if(strcmp(state->requestType,OAI_REQUEST_LISTRECORDS)==0 || strcmp(state->requestType,OAI_REQUEST_GETRECORD)==0) {

		for (rootNode = state->xmlroot->children; rootNode!= NULL; rootNode = rootNode->next) {

			if (rootNode->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(rootNode->name, (xmlChar*)state->requestType)!=0) continue;

			for (rec = rootNode->children; rec != NULL; rec = rec->next) {

				if (rec->type != XML_ELEMENT_NODE) continue;

				if(rec->next) {

					if (xmlStrcmp(rec->next->name, (xmlChar*)"resumptionToken")==0) {

						char *token = (char*)xmlNodeGetContent(rec->next);

						if(strlen(token) != 0) {

							state->resumptionToken = token;
							elog(DEBUG2,"  fetchNextRecord: (%s): Token detected in current page > %s",state->requestType,token);

						}
					}


				}



				for (metadata = rec->children; metadata != NULL; metadata = metadata->next) {

					if (metadata->type != XML_ELEMENT_NODE) continue;

					if (xmlStrcmp(metadata->name, (xmlChar*) "metadata") == 0) {

						xmlDocPtr xmldoc = NULL; //??
						xmlNodePtr oai_xml_document = metadata->children;
						xmlBufferPtr buffer = xmlBufferCreate();
						size_t content_size = (size_t) xmlNodeDump(buffer, xmldoc, oai_xml_document, 0, 1);

						elog(DEBUG2,"  fetchNextRecord: (%s) XML Buffer size: %ld",state->requestType,content_size);

						oai->content = (char*) buffer->content;
						oai->setsArray = NULL;

						for (header = rec->children; header != NULL; header = header->next) {

							if (header->type != XML_ELEMENT_NODE) continue;
							if (xmlStrcmp(header->name, (xmlChar*) "header") != 0) continue;

							if( xmlGetProp(header, (xmlChar*) OAI_NODE_STATUS)){
								char *status = (char*) xmlGetProp(header, (xmlChar*) OAI_NODE_STATUS);
								if (strcmp(status,"deleted")) oai->isDeleted = true;
							}

							for (headerList = header->children; headerList!= NULL; headerList = headerList->next) {

								char *node_content;

								if (headerList->type != XML_ELEMENT_NODE) continue;

								node_content = (char*)xmlNodeGetContent(headerList);

								if (xmlStrcmp(headerList->name, (xmlChar*) "identifier")==0) oai->identifier = node_content;

								if (xmlStrcmp(headerList->name, (xmlChar*) "setSpec")==0) appendTextArray(&oai->setsArray,node_content);

								if (xmlStrcmp(headerList->name, (xmlChar*) "datestamp")==0)	oai->datestamp= node_content;

							}

						}

					}

				}


				if(!state->current_identifier) {

					state->current_identifier = oai->identifier;
					elog(DEBUG1,"  fetchNextRecord: (%s) setting first current identifier > %s",state->requestType,state->current_identifier );
					return oai;

				}

				if (strcmp(state->current_identifier,oai->identifier)==0){

					elog(DEBUG1,"  fetchNextRecord: (%s) match > %s",state->requestType,oai->identifier);
					getNext = true;

				} else if(getNext == true) {

					state->current_identifier = oai->identifier;
					elog(DEBUG1,"  fetchNextRecord: (%s) setting new current identifier > %s",state->requestType,state->current_identifier);
					return oai;

				}


				if(!rec->next && !state->resumptionToken) return NULL;

			}

		}
	}

	return NULL;

}

void createOAITuple(TupleTableSlot *slot, oai_fdw_state *state, oai_Record *oai ) {

	elog(DEBUG1,"createOAITuple called: numcols = %d\n\n "
			"- identifier > %s\n "
			"- datestamp: %s\n "
			"- content: %s\n",state->numcols,oai->identifier,oai->datestamp,oai->content);

	for (int i = 0; i < state->numcols; i++) {

		List * options = GetForeignColumnOptions(state->foreigntableid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			DefElem *def = (DefElem*) lfirst(lc);

			if (strcmp(def->defname, OAI_NODE_OPTION)==0) {

				char *option_value = defGetString(def);

				if (strcmp(option_value,OAI_NODE_STATUS)==0){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = oai->isDeleted;


				} else if (strcmp(option_value,OAI_NODE_IDENTIFIER)==0 && oai->identifier){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->identifier);


				} else if (strcmp(option_value,OAI_NODE_METADATAPREFIX)==0 && oai->metadataPrefix){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->metadataPrefix);


				} else if (strcmp(option_value,OAI_NODE_CONTENT)==0 && oai->content){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum((char*)oai->content);

				} else if (strcmp(option_value,OAI_NODE_SETSPEC)==0 && oai->setsArray){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = (Datum) DatumGetArrayTypeP(oai->setsArray);
					elog(DEBUG2,"    createOAITuple: setspec added to tuple");

				}  else if (strcmp(option_value,OAI_NODE_DATESTAMP)==0 && oai->datestamp){

					char lowstr[MAXDATELEN + 1];
					char *field[MAXDATEFIELDS];
					int ftype[MAXDATEFIELDS];
					int nf;
					int parseError = ParseDateTime(oai->datestamp, lowstr, MAXDATELEN, field, ftype, MAXDATEFIELDS, &nf);

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);


					if (parseError == 0) {

						int dtype;
						struct pg_tm date;
						fsec_t fsec = 0;
						int tz = 0;

						int decodeError = DecodeDateTime(field, ftype, nf, &dtype, &date, &fsec, &tz);

						Timestamp tmsp;
						tm2timestamp(&date, fsec, &tz, &tmsp);

						if (decodeError == 0) {

							elog(DEBUG2,"    createOAITuple: datestamp can be decoded");

							slot->tts_isnull[i] = false;
							slot->tts_values[i] = (Datum) TimestampTzGetDatum(tmsp);

							elog(DEBUG2,"  createOAITuple: datestamp (\"%s\") parsed and decoded.",oai->datestamp);


						} else {


							slot->tts_isnull[i] = true;
							slot->tts_values[i] = (Datum) NULL;
							elog(WARNING,"  createOAITuple: could not decode datestamp: %s", oai->datestamp);

						}


					} else {


						slot->tts_isnull[i] = true;
						slot->tts_values[i] = (Datum) NULL;

						elog(WARNING,"  createOAITuple: could not parse datestamp: %s", oai->datestamp);

					}

				}

			}

		}

	}

	state->rowcount++;
	elog(DEBUG1,"  createOAITuple finished (rowcount: %d)",state->rowcount);

}

TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node) {

	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	oai_fdw_state *state = node->fdw_state;
	oai_Record *oai = (oai_Record *)palloc0(sizeof(oai_Record));

	elog(DEBUG1,"oai_fdw_IterateForeignScan called");

	ExecClearTuple(slot);

	/* Returns empty tuple in case there is no mapping for OAI nodes and columns*/
	if(state->numfdwcols == 0) return slot;

	if(state->rowcount == 0 || state->resumptionToken) {

		elog(DEBUG2,"  oai_fdw_IterateForeignScan: loading OAI records (token: \"%s\")", state->resumptionToken);

		LoadOAIRecords(state);
		state->current_identifier = NULL;
		elog(DEBUG1,"completeListSize: %s (rowcount: %d)",state->completeListSize,state->rowcount);

	}

	// FIXME: slot parameter isn't used in function fetchNextRecord
	oai = fetchNextRecord(slot, state);

	if(oai) {

		createOAITuple(slot, state, oai);
		elog(DEBUG2,"  oai_fdw_IterateForeignScan: OAI tuple created");

		ExecStoreVirtualTuple(slot);
		elog(DEBUG2,"  oai_fdw_IterateForeignScan: virtual tuple stored");

	}

	elog(DEBUG1,"  => oai_fdw_IterateForeignScan: returning tuple (rowcount: %d)", state->rowcount);

	return slot;

}

int LoadOAIRecords(oai_fdw_state *state) {

	elog(DEBUG1,"loadOAIRecords called: current_row > %d",state->current_row);


	if(state->resumptionToken || state->rowcount == 0) {

		elog(DEBUG2,"  loadOAIRecords: loading page ...");
		listRecordsRequest(state);

	} else {

		return 0;
	}


	return 1;

}

void appendTextArray(ArrayType **array, char* text_element) {

	/* Array build variables */
	size_t arr_nelems = 0;
	size_t arr_elems_size = 1;
	Oid elem_type = TEXTOID;
	int16 elem_len;
	bool elem_byval;
	char elem_align;
	Datum *arr_elems = palloc0(arr_elems_size*sizeof(Datum));

	elog(DEBUG2,"appendTextArray called with element > %s",text_element);

	get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

	if(*array == NULL) {

		/*Array has no elements */
		arr_elems[arr_nelems] = CStringGetTextDatum(text_element);
		elog(DEBUG2,"  array empty! adding value at arr_elems[%ld]",arr_nelems);
		arr_nelems++;

	} else {

		bool isnull;
		Datum value;
		ArrayIterator iterator = array_create_iterator(*array,0,NULL);
		elog(DEBUG2,"  current array size: %d",ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array)));

		arr_elems_size *= ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array))+1;
		arr_elems = repalloc(arr_elems, arr_elems_size*sizeof(Datum));

		while (array_iterate(iterator, &value, &isnull)) {

			if (isnull) continue;

			elog(DEBUG2,"  re-adding element: arr_elems[%ld]. arr_elems_size > %ld",arr_nelems,arr_elems_size);

			arr_elems[arr_nelems] = value;
			arr_nelems++;

		}
		array_free_iterator(iterator);

		elog(DEBUG2,"  adding new element: arr_elems[%ld]. arr_elems_size > %ld",arr_nelems,arr_elems_size);
		arr_elems[arr_nelems++] = CStringGetTextDatum(text_element);

	}

	elog(DEBUG2,"  => construct_array called: arr_nelems > %ld arr_elems_size %ld",arr_nelems,arr_elems_size);

	*array = construct_array(arr_elems, arr_nelems, elem_type, elem_len, elem_byval, elem_align);

}

void oai_fdw_ReScanForeignScan(ForeignScanState *node) {

	oai_fdw_state *state = node->fdw_state;
	state->current_row = 0;

}

void oai_fdw_EndForeignScan(ForeignScanState *node) {

}

static List* oai_fdw_ImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid serverOid) {

	ForeignServer* server;
	ListCell   *cell;
	List *sql_commands = NIL;
	List *all_sets = NIL;
	char *format = "oai_dc";
	char *url = NULL;
	bool format_set = false;

	server = GetForeignServer(serverOid);

	foreach(cell, server->options) {

		DefElem *def = lfirst_node(DefElem, cell);
		if(strcmp(def->defname,"url")==0) {
			url = defGetString(def);
			all_sets = GetSets(url);
		}

	}


	foreach(cell, stmt->options) {

		DefElem *def = lfirst_node(DefElem, cell);
		if(strcmp(def->defname,"metadataprefix")==0) {

			ListCell *cell_formats;
			List *formats = NIL;
			bool found = false;
			formats = GetMetadataFormats(url);

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

			elog(DEBUG1,"IMPORT FOREIGN SCHEMA (%s): \n%s",stmt->remote_schema,buffer.data);

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

		elog(DEBUG1,"IMPORT FOREIGN SCHEMA: \n%s",buffer.data);

	} else {

		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_NAME),
				 errmsg("invalid FOREIGN SCHEMA: '%s'",stmt->remote_schema)));

	}

	return sql_commands;

}
