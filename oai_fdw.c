
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

#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <commands/explain.h>
#include <curl/curl.h>
#include <libxml/tree.h>
#include <catalog/pg_collation.h>

//#include <optimizer/optimizer.h>

//#include <c.h>
//#include <funcapi.h> //TODO:NECESSARY?

//#include <postgres_ext.h>
//#include <stdbool.h>
//#include <pgtime.h>
//#include <utils/fmgrprotos.h>

//#include <time.h>
#include <funcapi.h>

#include "lib/stringinfo.h"

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

//#include "catalog/pg_aggregate.h"
//#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
//#include "catalog/pg_operator.h"
//#include "catalog/pg_proc.h"
//#include "catalog/pg_type.h"


#define OAI_FDW_VERSION "1.0.0dev"

#define OAI_REQUEST_LISTRECORDS "ListRecords"
#define OAI_REQUEST_LISTIDENTIFIERS "ListIdentifiers"
#define OAI_REQUEST_IDENTIFY "Identify"
#define OAI_REQUEST_LISTSETS "ListSets"
#define OAI_REQUEST_GETRECORD "GetRecord"
#define OAI_REQUEST_LISTMETADATAFORMATS "ListMetadataFormats"

#define OAI_XML_ROOT_ELEMENT "OAI-PMH"
#define OAI_ATTRIBUTE_IDENTIFIER "identifier"
#define OAI_ATTRIBUTE_CONTENT "content"
#define OAI_ATTRIBUTE_DATESTAMP "datestamp"
#define OAI_ATTRIBUTE_SETSPEC "setspec"
#define OAI_ATTRIBUTE_METADATAPREFIX "metadataprefix"
#define OAI_ATTRIBUTE_URL "url"
#define OAI_ATTRIBUTE_FROM "from"
#define OAI_ATTRIBUTE_UNTIL "until"
#define OAI_ATTRIBUTE_STATUS "status"
#define OAI_COLUMN_OPTION "oai_node"

#define OAI_ERROR_ID_DOES_NOT_EXIST "idDoesNotExist"
#define OAI_ERROR_NO_RECORD_MATCH "noRecordsMatch"

#define OAI_SUCCESS 0
#define OAI_FAIL 1
#define OAI_UNKNOWN_REQUEST 2
#define ERROR_CODE_MISSING_PREFIX 1

PG_MODULE_MAGIC;

typedef struct oai_fdw_state {

	int end;

	int current_row;
	int numcols;
	int numfdwcols;
	int  rowcount;
	char *identifier;
	char *document;
	char *date;
	char *set;
	char *url;
	char *from;
	char *until;
	char *metadataPrefix;
	char *resumptionToken;
	char* requestType;
	char* current_identifier;

	Oid foreigntableid;
	List *columnlist;
	xmlNodePtr xmlroot;

} oai_fdw_state;

typedef struct oai_Record {

	char *identifier;
	char *content;
	char *datestamp;
	char *metadataPrefix;
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

typedef struct MetadataFormat {

	char *metadataPrefix;
	char *schema;
	char *metadataNamespace;

} MetadataFormat;

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

/*
 * Valid options for ogr_fdw.
 * ForeignDataWrapperRelationId (no options)
 * ForeignServerRelationId (CREATE SERVER options)
 * UserMappingRelationId (CREATE USER MAPPING options)
 * ForeignTableRelationId (CREATE FOREIGN TABLE options)
 *
 * {optname, optcontext, optrequired, optfound}
 */
static struct OAIFdwOption valid_options[] =
{

	{OAI_ATTRIBUTE_URL, ForeignServerRelationId, true, false},
	{OAI_ATTRIBUTE_METADATAPREFIX, ForeignServerRelationId, true, false},

	{OAI_ATTRIBUTE_IDENTIFIER, ForeignTableRelationId, false, false},
	{OAI_ATTRIBUTE_METADATAPREFIX, ForeignTableRelationId, false, false},
	{OAI_ATTRIBUTE_SETSPEC, ForeignTableRelationId, false, false},
	{OAI_ATTRIBUTE_FROM, ForeignTableRelationId, false, false},
	{OAI_ATTRIBUTE_UNTIL, ForeignTableRelationId, false, false},

	{OAI_COLUMN_OPTION, AttributeRelationId, true, false},

	/* EOList marker */
	{NULL, InvalidOid, false, false}
};

#define option_count (sizeof(valid_options)/sizeof(struct OAIFdwOption))


extern Datum oai_fdw_handler(PG_FUNCTION_ARGS);
extern Datum oai_fdw_validator(PG_FUNCTION_ARGS);
extern Datum oai_fdw_version(PG_FUNCTION_ARGS);
extern Datum oai_fdw_listMetadataFormats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(oai_fdw_handler);
PG_FUNCTION_INFO_V1(oai_fdw_validator);
PG_FUNCTION_INFO_V1(oai_fdw_version);
PG_FUNCTION_INFO_V1(oai_fdw_listMetadataFormats);


void _PG_init(void);
//extern PGDLLEXPORT void _PG_init(void);


void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
void oai_fdw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
ForeignScan *oai_fdw_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
void oai_fdw_BeginForeignScan(ForeignScanState *node, int eflags);
TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node);
void oai_fdw_ReScanForeignScan(ForeignScanState *node);
void oai_fdw_EndForeignScan(ForeignScanState *node);

void init_string(struct string *s);
size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s);
void appendTextArray(ArrayType **array, char* text_element) ;
int executeOAIRequest(oai_fdw_state **state, struct string *xmlResponse);
void listRecordsRequest(oai_fdw_state *state);
void createOAITuple(TupleTableSlot *slot, oai_fdw_state *state, oai_Record *oai );
oai_Record *fetchNextRecord(TupleTableSlot *slot, oai_fdw_state *state);
int loadOAIRecords(oai_fdw_state *state);

static void deparseExpr(Expr *expr, oai_fdw_TableOptions *opts);
static char *datumToString(Datum datum, Oid type);

static char *getColumnOption(Oid foreigntableid, int16 attnum);
static void deparseWhereClause(List *conditions, oai_fdw_TableOptions *opts);
static void deparseSelectColumns(oai_fdw_TableOptions *opts);
static void requestPlanner(oai_fdw_TableOptions *opts, ForeignTable *ft, RelOptInfo *baserel);
static char *deparseTimestamp(Datum datum);
List *getMetadataFormats(char *url);

void _PG_init(void) {

}

Datum oai_fdw_version(PG_FUNCTION_ARGS) {

	StringInfoData url_bufffer;
	initStringInfo(&url_bufffer);

	appendStringInfo(&url_bufffer,"oai fdw = %s,",OAI_FDW_VERSION);
	appendStringInfo(&url_bufffer," libxml = %s",LIBXML_DOTTED_VERSION);
	appendStringInfo(&url_bufffer," libcurl = %s,",curl_version());

    PG_RETURN_TEXT_P(cstring_to_text(url_bufffer.data));
}

List *getMetadataFormats(char *url) {

	struct string xmlStream;
	int oaiExecuteResponse;
	oai_fdw_state *state = (oai_fdw_state *) palloc0(sizeof(oai_fdw_state));
	List *result = NIL;

	state->url = url;
	state->requestType = OAI_REQUEST_LISTMETADATAFORMATS;

	oaiExecuteResponse = executeOAIRequest(&state,&xmlStream);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlNodePtr xmlroot = NULL;
		xmlDocPtr xmldoc;
		xmldoc = xmlReadMemory(xmlStream.ptr, strlen(xmlStream.ptr), NULL, NULL, XML_PARSE_SAX1);

		xmlNodePtr oai_root;
		xmlNodePtr ListMetadataFormats;

		xmlNodePtr MetadataElement;


		if (!xmldoc || (xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {
			xmlFreeDoc(xmldoc);
			xmlCleanupParser();
			elog(WARNING,"invalid MARC21/XML document.");
		}

		//elog(WARNING,"DOC %s",xmlStream.ptr);

		for (oai_root = xmlroot->children; oai_root != NULL; oai_root = oai_root->next) {

			if (oai_root->type != XML_ELEMENT_NODE) continue;

			//elog(WARNING,"OAI_ROOT");
			if (xmlStrcmp(oai_root->name, (xmlChar*) "ListMetadataFormats") != 0) continue;

			for (ListMetadataFormats = oai_root->children; ListMetadataFormats != NULL; ListMetadataFormats = ListMetadataFormats->next) {

				//elog(WARNING,"  LISTMETADATAFORMATS > %s",ListMetadataFormats->name);

				if (ListMetadataFormats->type != XML_ELEMENT_NODE)	continue;
				if (xmlStrcmp(ListMetadataFormats->name, (xmlChar*) "metadataFormat") != 0) continue;

				MetadataFormat *format = (MetadataFormat *)palloc0(sizeof(MetadataFormat));

				for (MetadataElement = ListMetadataFormats->children; MetadataElement != NULL; MetadataElement = MetadataElement->next) {

					if (MetadataElement->type != XML_ELEMENT_NODE)	continue;

					if (xmlStrcmp(MetadataElement->name, (xmlChar*) "metadataPrefix") == 0) {

						format->metadataPrefix = palloc(sizeof(char)*strlen(xmlNodeGetContent(MetadataElement))+1);
						format->metadataPrefix = xmlNodeGetContent(MetadataElement);

					} else if (xmlStrcmp(MetadataElement->name, (xmlChar*) "schema") == 0) {

						format->schema = palloc(sizeof(char)*strlen(xmlNodeGetContent(MetadataElement))+1);
						format->schema = xmlNodeGetContent(MetadataElement);

					} else if (xmlStrcmp(MetadataElement->name, (xmlChar*) "metadataNamespace") == 0) {

						format->metadataNamespace = palloc(sizeof(char)*strlen(xmlNodeGetContent(MetadataElement))+1);
						format->metadataNamespace = xmlNodeGetContent(MetadataElement);

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
	PG_RETURN_POINTER(fdwroutine);

}


Datum oai_fdw_validator(PG_FUNCTION_ARGS) {

	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

//	Relation rel = table_open(PG_GETARG_DATUM(0), NoLock);
//
//	table_close(rel, NoLock);

	struct OAIFdwOption* opt;
	const char* source = NULL, *driver = NULL;
	const char* config_options = NULL, *open_options = NULL;

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

				if(strcmp(opt->optname, OAI_ATTRIBUTE_URL)==0){

					if(!check_url(defGetString(def))) {

						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							 errmsg("invalid %s: '%s'",OAI_ATTRIBUTE_URL, defGetString(def))));

					}

				}

				/* Store some options for testing later */
				if (strcmp(opt->optname, OAI_COLUMN_OPTION) == 0) {

					if(strcmp(defGetString(def), OAI_ATTRIBUTE_IDENTIFIER) !=0 &&
					   strcmp(defGetString(def), OAI_ATTRIBUTE_METADATAPREFIX) !=0 &&
					   strcmp(defGetString(def), OAI_ATTRIBUTE_SETSPEC) !=0 &&
					   strcmp(defGetString(def), OAI_ATTRIBUTE_DATESTAMP) !=0 &&
					   strcmp(defGetString(def), OAI_ATTRIBUTE_CONTENT) !=0 &&
					   strcmp(defGetString(def), OAI_ATTRIBUTE_STATUS) !=0) {

						ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							 errmsg("invalid %s option '%s'",OAI_COLUMN_OPTION, defGetString(def)),
							 errhint("Valid column options for oai_fdw are:\nCREATE SERVER: '%s', '%s'\nCREATE TABLE: '%s','%s', '%s', '%s' and '%s'",
								OAI_ATTRIBUTE_URL,
								OAI_ATTRIBUTE_METADATAPREFIX,
								OAI_ATTRIBUTE_METADATAPREFIX,
								OAI_ATTRIBUTE_SETSPEC,
								OAI_ATTRIBUTE_FROM,
								OAI_ATTRIBUTE_UNTIL,
								OAI_ATTRIBUTE_STATUS)));

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

		//elog(WARNING,"control: catalog %u > opt->optname = %s | catalog = %u | required = %d | found = %d",catalog,opt->optname, opt->optcontext, opt->optrequired, opt->optfound);
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
    }

    return (response == CURLE_OK) ? 1 : 0;
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

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{

		const char *url;

		server = GetForeignServerByName(srvname, true);

		if(server) {

			ListCell   *cell;

			foreach(cell, server->options) {

				DefElem *def = lfirst_node(DefElem, cell);

				if(strcmp(def->defname,"url")==0) {
					url = defGetString(def);
				}

			}

		}


		MemoryContext   oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		List *formats = getMetadataFormats(url);

		funcctx->user_fctx = formats; //getMetadataFormats("https://sammlungen.ulb.uni-muenster.de/oai");

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* total number of tuples to be returned */
		//funcctx->max_calls = PG_GETARG_UINT32(0);
		if(formats)	funcctx->max_calls = formats->length;

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					       errmsg("function returning record called in context that cannot accept type record")));

		/*
		 * generate attribute metadata needed later to produce tuples from raw
		 * C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);

		//elog(WARNING,"GOT HERE - END FIRST LOOP");

	}


	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	//int i = 1;

	if (call_cntr < max_calls)    /* do when there is more left to send */
	    {
	        char       **values;
	        HeapTuple    tuple;
	        Datum        result;

	       // ListCell   *cell;
	       // foreach(cell, formats) {

	        //	MetadataFormat *format = (MetadataFormat *) lfirst_node(DefElem, cell);
	        	//elog(WARNING,"metadataPrefix ## > %s",format->metadataPrefix);

	        //}

	        size_t max_prefix = 0;
	        size_t max_schema = 0;
	        size_t max_nspace = 0;

	        ListCell *cell;

	        foreach(cell, funcctx->user_fctx) {

				//DefElem *def = lfirst_node(DefElem, cell);
	        	MetadataFormat *format = (MetadataFormat *) lfirst_node(DefElem, cell);

	        	if(strlen(format->metadataPrefix)>max_prefix) max_prefix = strlen(format->metadataPrefix);
	        	if(strlen(format->schema)>max_schema) max_schema = strlen(format->schema);
	        	if(strlen(format->metadataNamespace)>max_nspace) max_nspace = strlen(format->metadataNamespace);
				//elog(WARNING,">>>>>  %s",(format->metadataPrefix));
	        	//elog(WARNING,"%s (%d), %s (%d), %s (%d)",format->metadataPrefix,strlen(format->metadataPrefix), format->schema, strlen(format->schema), format->metadataNamespace, strlen(format->metadataNamespace));

	        }


	        MetadataFormat *format = (MetadataFormat *) list_nth((List*) funcctx->user_fctx, call_cntr);
	        //format->metadataPrefix[strlen(format->metadataPrefix)] = '\0';
	        //format->schema[strlen(format->schema)] = '\0';
	        //format->metadataNamespace[strlen(format->metadataNamespace)] = '\0';
	        /*
	         * Prepare a values array for building the returned tuple.
	         * This should be an array of C strings which will
	         * be processed later by the type input functions.
	         */

	        //elog(WARNING,"ANTES DO PALLOC >>> \n\n'%s' \n'%s' \n'%s'\n", format->metadataPrefix, format->schema, format->metadataNamespace);

	        const size_t MAX_SIZE = 500;
	        values = (char **) palloc(3 * sizeof(char *));
	        values[0] = (char *) palloc(MAX_SIZE * sizeof(char));
	        values[1] = (char *) palloc(MAX_SIZE * sizeof(char));
	        values[2] = (char *) palloc(MAX_SIZE * sizeof(char));

	       // values[0] = (char *) palloc(strlen(format->metadataPrefix)+1 * sizeof(char));
	       // values[1] = (char *) palloc(strlen(format->schema)+1 * sizeof(char));
	       // values[2] = (char *) palloc((strlen(format->metadataNamespace)+1) * sizeof(char));

	       // elog(WARNING,"%d %d %d",strlen(format->metadataPrefix), strlen(format->schema), strlen(format->metadataNamespace));

	       // snprintf(values[0], strlen(format->metadataPrefix)+1, "%s", format->metadataPrefix);
	       // snprintf(values[1], strlen(format->schema)+1, "%s", format->schema);
	       // snprintf(values[2], strlen(format->metadataNamespace)+1, "%s", format->metadataNamespace);

	        snprintf(values[0], MAX_SIZE, "%s", format->metadataPrefix);
	        snprintf(values[1], MAX_SIZE, "%s", format->schema);
	        snprintf(values[2], MAX_SIZE, "%s", format->metadataNamespace);

//	        snprintf(values[0], strlen(format->metadataPrefix)+1, "%s", format->metadataPrefix);
//	        snprintf(values[1], strlen(format->schema)+1, "%s", format->schema);
//	        snprintf(values[2], strlen(format->metadataNamespace)+1, "%s", format->metadataNamespace);

	        //strncpy(values[0],format->metadataPrefix,strlen(format->metadataPrefix));
	        //strncpy(values[1],format->schema,strlen(format->schema));
	        //strncpy(values[2],format->metadataNamespace,strlen(format->metadataNamespace));

	       // elog(WARNING,"GOT HERE BuildTupleFromCStrings max_prefix = %d, max_schema = %d, max_ns = %d",max_prefix,max_schema,max_nspace);

	        /* build a tuple */


	        tuple = BuildTupleFromCStrings(attinmeta, values);

	        //elog(WARNING,"GOT HERE HeapTupleGetDatum");

	        /* make the tuple into a datum */
	        result = HeapTupleGetDatum(tuple);

	        /* clean up (this is not really necessary) */
	        pfree(values[0]);
	        pfree(values[1]);
	        pfree(values[2]);
	        pfree(values);

	        //elog(WARNING,"GOT HERE SRF_RETURN_NEXT");

	        SRF_RETURN_NEXT(funcctx, result);
	    }
	    else    /* do when there is no more left */
	    {
	        SRF_RETURN_DONE(funcctx);
	    }


	/*

	ForeignServer *server = GetForeignServerByName(srvname, true);

	//TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	//https://www.postgresql.org/docs/current/xfunc-c.html
	PG_RETURN_NULL();


	if(server){

		ListCell *cell;

		foreach(cell, server->options) {

			DefElem *def = lfirst_node(DefElem, cell);

			PG_RETURN_POINTER(cstring_to_text(defGetString(def)));

			elog(WARNING,"SRV > %s",defGetString(def));

			if (strcmp("url", def->defname) == 0) {

				//opts->url = defGetString(def);
				url = defGetString(def);

			} else if (strcmp("metadataprefix", def->defname) == 0) {

				//opts->metadataPrefix = defGetString(def);

			} else {

				elog(WARNING,"Invalid SERVER OPTION > '%s'",def->defname);
			}

		}
	}

    PG_RETURN_TEXT_P(cstring_to_text(url));

    **/
}

int executeOAIRequest(oai_fdw_state **state, struct string *xmlResponse) {

	CURL *curl;
	CURLcode res;
	//long resCode;
	StringInfoData url_bufffer;
	initStringInfo(&url_bufffer);

	elog(DEBUG1,"executeOAIRequest called: url > %s ",(*state)->url);

	appendStringInfo(&url_bufffer,"verb=%s",(*state)->requestType);

	if(strcmp((*state)->requestType,OAI_REQUEST_LISTRECORDS)==0) {

		if((*state)->set) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'set' > %s",(*state)->requestType,(*state)->set);
			appendStringInfo(&url_bufffer,"&set=%s",(*state)->set);

		}

		if((*state)->from) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'from' > %s",(*state)->requestType,(*state)->from);
			appendStringInfo(&url_bufffer,"&from=%s",(*state)->from);

		}

		if((*state)->until) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'until' > %s",(*state)->requestType,(*state)->until);
			appendStringInfo(&url_bufffer,"&until=%s",(*state)->until);

		}

		if((*state)->metadataPrefix) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'metadataPrefix' > %s",(*state)->requestType,(*state)->metadataPrefix);
			appendStringInfo(&url_bufffer,"&metadataPrefix=%s",(*state)->metadataPrefix);

		}

		if((*state)->resumptionToken) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'resumptionToken' > %s",(*state)->requestType,(*state)->resumptionToken);
			resetStringInfo(&url_bufffer);
			appendStringInfo(&url_bufffer,"verb=%s&resumptionToken=%s",(*state)->requestType,(*state)->resumptionToken);

			(*state)->resumptionToken = NULL;

		}

	} else if(strcmp((*state)->requestType,OAI_REQUEST_GETRECORD)==0) {

		if((*state)->identifier) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'identifier' > %s",(*state)->requestType,(*state)->identifier);
			appendStringInfo(&url_bufffer,"&identifier=%s",(*state)->identifier);

		}

		if((*state)->metadataPrefix) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'metadataPrefix' > %s",(*state)->requestType,(*state)->metadataPrefix);
			appendStringInfo(&url_bufffer,"&metadataPrefix=%s",(*state)->metadataPrefix);

		}

	} else if(strcmp((*state)->requestType,OAI_REQUEST_LISTIDENTIFIERS)==0) {


		if((*state)->set) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'set' > %s",(*state)->requestType,(*state)->set);
			appendStringInfo(&url_bufffer,"&set=%s",(*state)->set);

		}

		if((*state)->from) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'from' > %s",(*state)->requestType,(*state)->from);
			appendStringInfo(&url_bufffer,"&from=%s",(*state)->from);


		}

		if((*state)->until) {

			elog(DEBUG2,"  %s: appending 'until' > %s",(*state)->requestType,(*state)->until);
			appendStringInfo(&url_bufffer,"&until=%s",(*state)->until);

		}

		if((*state)->metadataPrefix) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'metadataPrefix' > %s",(*state)->requestType,(*state)->metadataPrefix);
			appendStringInfo(&url_bufffer,"&metadataPrefix=%s",(*state)->metadataPrefix);

		}


		if((*state)->resumptionToken) {

			elog(DEBUG2,"  executeOAIRequest (%s): appending 'resumptionToken' > %s",(*state)->requestType,(*state)->resumptionToken);
			resetStringInfo(&url_bufffer);
			appendStringInfo(&url_bufffer,"verb=%s&resumptionToken=%s",(*state)->requestType,(*state)->resumptionToken);

			(*state)->resumptionToken = NULL;

		}

	} else if(strcmp((*state)->requestType,OAI_REQUEST_LISTMETADATAFORMATS)==0) {



	} else {

		return OAI_UNKNOWN_REQUEST;

	}


	elog(DEBUG1,"  executeOAIRequest (%s): url build > %s?%s",(*state)->requestType,(*state)->url,url_bufffer.data);

	curl = curl_easy_init();

	if(curl) {

		init_string(xmlResponse);

		curl_easy_setopt(curl, CURLOPT_URL, (*state)->url);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, url_bufffer.data);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, xmlResponse);
		curl_easy_setopt(curl, CURLOPT_FAILONERROR, true);
//		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &resCode);

		res = curl_easy_perform(curl);

		if (res != CURLE_OK) {

			curl_easy_cleanup(curl);

			ereport(ERROR,
					errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					errmsg("OAI %s request failed. Connection error code %d",(*state)->requestType,res));

		}

	} else {

		elog(DEBUG2, "  => %s: curl failed",(*state)->requestType);
		return OAI_FAIL;

	}

	curl_easy_cleanup(curl);

	elog(DEBUG2, "  => %s: curl http_return > '%d'",(*state)->requestType, res);

	return OAI_SUCCESS;


}


void listRecordsRequest(oai_fdw_state *state) {

	struct string xmlStream;
	int oaiExecuteResponse;

	elog(DEBUG1,"ListRecordsRequest/LoadData called");

	oaiExecuteResponse = executeOAIRequest(&state,&xmlStream);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlDocPtr xmldoc;
		xmlNodePtr recordsList;

		xmlInitParser();

		xmldoc = xmlReadMemory(xmlStream.ptr, strlen(xmlStream.ptr), NULL, NULL, XML_PARSE_SAX1);


		if (!xmldoc || (state->xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {

			xmlFreeDoc(xmldoc);
			xmlCleanupParser();

			ereport(ERROR, errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),	errmsg("Invalid XML response for URL '%s'",state->url));

		}

		for (recordsList = state->xmlroot->children; recordsList != NULL; recordsList = recordsList->next) {

			if (recordsList->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(recordsList->name, (xmlChar*)state->requestType)==0) {

				if (!xmldoc || (state->xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {

					xmlFreeDoc(xmldoc);
					xmlCleanupParser();

				}

			} else if (xmlStrcmp(recordsList->name, (xmlChar*)"error")==0) {

				char *errorMessage = (char*)xmlNodeGetContent(recordsList);
				char *errorCode = (char*) xmlGetProp(recordsList, (xmlChar*) "code");

				if(strcmp(errorCode,OAI_ERROR_ID_DOES_NOT_EXIST)==0 ||
				   strcmp(errorCode,OAI_ERROR_NO_RECORD_MATCH)==0) {

					state->xmlroot = NULL;
					ereport(WARNING,
							errcode(ERRCODE_NO_DATA_FOUND),
							errmsg("OAI %s: %s",errorCode,errorMessage));

				} else {

					ereport(ERROR,
							errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							errmsg("OAI %s: %s",errorCode,errorMessage));

				}


			}

		}

	}

}

static void requestPlanner(oai_fdw_TableOptions *opts, ForeignTable *ft, RelOptInfo *baserel) {

	List *conditions = baserel->baserestrictinfo;
	Relation rel = table_open(ft->relid, NoLock);
	bool hasContentForeignColumn = false;

	/* The default request type is OAI_REQUEST_LISTRECORDS.
	 * This can be altered after depending on the columns
	 * used in the WHERE and SELECT clauses*/
	opts->requestType = OAI_REQUEST_LISTRECORDS;
	opts->numcols = rel->rd_att->natts;
	opts->foreigntableid = ft->relid;

	for (int i = 0; i < rel->rd_att->natts ; i++) {

		List *options = GetForeignColumnOptions(ft->relid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			opts->numfdwcols++;

			DefElem *def = (DefElem*) lfirst(lc);

			if (strcmp(def->defname, OAI_COLUMN_OPTION)==0) {

				char *option_value = defGetString(def);

				if (strcmp(option_value,OAI_ATTRIBUTE_STATUS)==0){

					if (rel->rd_att->attrs[i].atttypid != BOOLOID) {

						ereport(ERROR,
							errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("invalid data type for '%s.%s': %d",
									NameStr(rel->rd_rel->relname),
									NameStr(rel->rd_att->attrs[i].attname),
									rel->rd_att->attrs[i].atttypid),
							errhint("OAI %s must be of type 'boolean'.",
									OAI_ATTRIBUTE_STATUS));
					}

				} else if (strcmp(option_value,OAI_ATTRIBUTE_IDENTIFIER)==0 || strcmp(option_value,OAI_ATTRIBUTE_METADATAPREFIX)==0){

					if (rel->rd_att->attrs[i].atttypid != TEXTOID &&
						rel->rd_att->attrs[i].atttypid != VARCHAROID) {

						ereport(ERROR,
								errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s must be of type 'text' or 'varchar'.",
										option_value));

					}


				} else if (strcmp(option_value,OAI_ATTRIBUTE_CONTENT)==0){

					hasContentForeignColumn = true;

					if (rel->rd_att->attrs[i].atttypid != TEXTOID &&
						rel->rd_att->attrs[i].atttypid != VARCHAROID &&
						rel->rd_att->attrs[i].atttypid != XMLOID) {

						ereport(ERROR,
								errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s expects one of the following types: 'xml', 'text' or 'varchar'.",
										OAI_ATTRIBUTE_CONTENT));
					}


				} else if (strcmp(option_value,OAI_ATTRIBUTE_SETSPEC)==0){

					if (rel->rd_att->attrs[i].atttypid != TEXTARRAYOID &&
					    rel->rd_att->attrs[i].atttypid != VARCHARARRAYOID) {

						ereport(ERROR,
								errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s expects one of the following types: 'text[]', 'varchar[]'.",
										OAI_ATTRIBUTE_SETSPEC));

					}


				}  else if (strcmp(option_value,OAI_ATTRIBUTE_DATESTAMP)==0){

					if (rel->rd_att->attrs[i].atttypid != TIMESTAMPOID) {

						ereport(ERROR,
								errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s expects a 'timestamp'.",
										OAI_ATTRIBUTE_DATESTAMP));

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


	table_close(rel, NoLock);

}


static char *getColumnOption(Oid foreigntableid, int16 attnum) {

	List *options;
	Relation rel = table_open(foreigntableid, NoLock);
	char *optionValue = NULL;

	for (int i = 0; i < rel->rd_att->natts ; i++) {

		ListCell *lc;
		options = GetForeignColumnOptions(foreigntableid, i+1);

		if(rel->rd_att->attrs[i].attnum == attnum){

			foreach (lc, options) {

				DefElem *def = (DefElem*) lfirst(lc);

				if (strcmp(def->defname, OAI_COLUMN_OPTION)==0) {

					optionValue = defGetString(def);

					break;

				}

			}

		}

	}

	table_close(rel, NoLock);

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
		leftargOption = getColumnOption(opts->foreigntableid,varleft->varattnosyn);


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
		leftargOption = getColumnOption(opts->foreigntableid,varleft->varattnosyn);

		if (strcmp(opername, "=") == 0){

			//varright = (Var *) lsecond(oper->args);

			if (strcmp(leftargOption,OAI_ATTRIBUTE_IDENTIFIER) ==0 && (varleft->vartype == TEXTOID || varleft->vartype == VARCHAROID)) {

				Const *constant = (Const*) lsecond(oper->args);

				opts->requestType = OAI_REQUEST_GETRECORD;
				opts->identifier = datumToString(constant->constvalue,constant->consttype);

				elog(DEBUG1,"  deparseExpr: request type set to '%s' with identifier '%s'",OAI_REQUEST_GETRECORD,opts->identifier);

			}


			if (strcmp(leftargOption,OAI_ATTRIBUTE_DATESTAMP) ==0 && (varleft->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->from = deparseTimestamp(constant->constvalue);
				opts->until = deparseTimestamp(constant->constvalue);

			}

			if (strcmp(leftargOption,OAI_ATTRIBUTE_METADATAPREFIX) ==0 && (varleft->vartype == TEXTOID || varleft->vartype == VARCHAROID)) {

				Const *constant = (Const*) lsecond(oper->args);

				opts->metadataPrefix = datumToString(constant->constvalue,constant->consttype);

				elog(DEBUG1,"  deparseExpr: metadataPrefix set to '%s'",opts->metadataPrefix);

			}


		}

		if (strcmp(opername, ">=") == 0 || strcmp(opername, ">") == 0){

			if (strcmp(leftargOption,OAI_ATTRIBUTE_DATESTAMP) ==0 && (varleft->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->from = deparseTimestamp(constant->constvalue);

			}

		}

		if (strcmp(opername, "<=") == 0 || strcmp(opername, "<") == 0){

			if (strcmp(leftargOption,OAI_ATTRIBUTE_DATESTAMP) ==0 && (varleft->vartype == TIMESTAMPOID)) {

				Const *constant = (Const*) lsecond(oper->args);
				opts->until = deparseTimestamp(constant->constvalue);

			}


		}

		if (strcmp(opername, "<@") == 0 || strcmp(opername, "@>") == 0 || strcmp(opername, "&&") == 0 ){

			if (strcmp(leftargOption,OAI_ATTRIBUTE_SETSPEC) ==0 && (varleft->vartype == TEXTARRAYOID || varleft->vartype == VARCHARARRAYOID)) {


				//ListCell   *lc;
				Const *constant = (Const*) lsecond(oper->args);

				ArrayType *array = (ArrayType*) constant->constvalue;

				int numitems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

				if(numitems > 1) {

					elog(WARNING,"The OAI standard requests do not support multiple '%s' attributes. This filter will be applied AFTER the OAI request.",OAI_ATTRIBUTE_SETSPEC);
					elog(DEBUG1,"  deparseExpr: clearing '%s' attribute.",OAI_ATTRIBUTE_SETSPEC);
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

		if(expr->type == T_Var)	{

			variable = (Var *)expr;
			columnOption = getColumnOption(opts->foreigntableid,variable->varattnosyn);

			if(strcmp(columnOption,OAI_ATTRIBUTE_CONTENT)==0) {

				elog(DEBUG1,"  deparseSelectColumns: content OAI option detected");
				hasContentForeignColumn = true;

			}


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
	int start = 0, end = 64; //TODO necessary?

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

		if (strcmp(OAI_ATTRIBUTE_METADATAPREFIX, def->defname) == 0) {

			opts->metadataPrefix = defGetString(def);

		} else if (strcmp(OAI_ATTRIBUTE_SETSPEC, def->defname) == 0) {

			opts->set = defGetString(def);

		} else if (strcmp(OAI_ATTRIBUTE_FROM, def->defname) == 0) {

			opts->from= defGetString(def);

		} else if (strcmp(OAI_ATTRIBUTE_UNTIL, def->defname) == 0) {

			opts->until= defGetString(def);

		}

	}

//	if(!opts->url){
//
//		ereport(ERROR,
//				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
//				 errmsg("'url' not found"),
//				 errhint("'url' is a required option. Check the CREATE FOREIGN TABLE options.")));
//
//	}

//	if(!opts->metadataPrefix){
//
//		ereport(ERROR,
//				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
//				 errmsg("'metadataPrefix' not found"),
//				 errhint("metadataPrefix is a required argument (unless the exclusive argument resumptionToken is used). Check the CREATE FOREIGN TABLE options.")));
//
//	}


	requestPlanner(opts, ft, baserel);

	baserel->rows = end - start; //TODO ??

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

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
			scan_clauses,
			baserel->relid,
			NIL, /* no expressions we will evaluate */
			state, /* pass along our start and end */
			NIL, /* no custom tlist; our scan tuple looks like tlist */
			NIL, /* no quals we will recheck */
			outer_plan);

}

void oai_fdw_BeginForeignScan(ForeignScanState *node, int eflags) {

	ForeignScan *fs = (ForeignScan *) node->ss.ps.plan;
	oai_fdw_state *state = (oai_fdw_state *) fs->fdw_private;

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

				if(xmlGetProp(header, (xmlChar*) OAI_ATTRIBUTE_STATUS)){
					char *status = (char*) xmlGetProp(header, (xmlChar*) OAI_ATTRIBUTE_STATUS);
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

						xmlDocPtr xmldoc; //??
						xmlNodePtr oai_xml_document = metadata->children;
						xmlBufferPtr buffer = xmlBufferCreate();
						//size_t content_size = (size_t) xmlNodeDump(buffer, xmldoc, oai_xml_document, 0, 1);
						size_t content_size = (size_t) xmlNodeDump(buffer, xmldoc, oai_xml_document, 0, 1);

						elog(DEBUG2,"  fetchNextRecord: (%s) XML Buffer size: %ld",state->requestType,content_size);

						oai->content = (char*) buffer->content;
						oai->setsArray = NULL;

						for (header = rec->children; header != NULL; header = header->next) {

							if (header->type != XML_ELEMENT_NODE) continue;
							if (xmlStrcmp(header->name, (xmlChar*) "header") != 0) continue;

							if( xmlGetProp(header, (xmlChar*) OAI_ATTRIBUTE_STATUS)){
								char *status = (char*) xmlGetProp(header, (xmlChar*) OAI_ATTRIBUTE_STATUS);
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

	//return oai;

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

			if (strcmp(def->defname, OAI_COLUMN_OPTION)==0) {

				char * option_value = defGetString(def);

				if (strcmp(option_value,OAI_ATTRIBUTE_STATUS)==0){

					elog(DEBUG2,"  createOAITuple: column %d option '%d'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = oai->isDeleted;


				} else if (strcmp(option_value,OAI_ATTRIBUTE_IDENTIFIER)==0 && oai->identifier){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->identifier);


				} else if (strcmp(option_value,OAI_ATTRIBUTE_METADATAPREFIX)==0 && oai->metadataPrefix){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->metadataPrefix);


				} else if (strcmp(option_value,OAI_ATTRIBUTE_CONTENT)==0 && oai->content){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum((char*)oai->content);

				} else if (strcmp(option_value,OAI_ATTRIBUTE_SETSPEC)==0 && oai->setsArray){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = DatumGetArrayTypeP(oai->setsArray);
					elog(DEBUG2,"    createOAITuple: setspec added to tuple");

				}  else if (strcmp(option_value,OAI_ATTRIBUTE_DATESTAMP)==0 && oai->datestamp){

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
							slot->tts_values[i] = TimestampTzGetDatum(tmsp);

							elog(DEBUG2,"  createOAITuple: datestamp (\"%s\") parsed and decoded.",oai->datestamp);


						} else {


							slot->tts_isnull[i] = true;
							slot->tts_values[i] = NULL;
							elog(WARNING,"  createOAITuple: could not decode datestamp: %s", oai->datestamp);

						}


					} else {


						slot->tts_isnull[i] = true;
						slot->tts_values[i] = NULL;

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
	if(state->numfdwcols == 0) {

		return slot;

	}

	if(state->rowcount == 0 || state->resumptionToken) {

		elog(DEBUG2,"  oai_fdw_IterateForeignScan: loading OAI records (token: \"%s\")", state->resumptionToken);

		loadOAIRecords(state);
		state->current_identifier = NULL;

	}


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

int loadOAIRecords(oai_fdw_state *state) {

	elog(DEBUG1,"loadOAIRecords called: current_row > %d",state->current_row);


	if(state->resumptionToken || state->rowcount == 0) {

		elog(DEBUG2,"  fetchNextOAIRecord: loading page ...");
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
