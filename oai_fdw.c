
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
//#include <libxml2/libxml/tree.h> //TODO: CORRECT PATH
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
//#include "nodes/nodes.h"
//#include "utils/date.h"
//#include "utils/datetime.h"
//#include "utils/timestamp.h"
//#include "catalog/pg_cast.h"
//#include "utils/formatting.h"
//#include "utils/pg_locale.h"
//#define LIBXML_OUTPUT_ENABLED //??
//#define LIBXML_TREE_ENABLED //??

#include "catalog/pg_foreign_table.h"

#define OAI_FDW_VERSION "1.0dev"
#define OAI_REQUEST_LISTRECORDS "ListRecords"
#define OAI_REQUEST_IDENTIFY "Identify"
#define OAI_REQUEST_LISTSETS "ListSets"
#define OAI_XML_ROOT_ELEMENT "OAI-PMH"
#define OAI_ATTRIBUTE_IDENTIFIER "identifier"
#define OAI_ATTRIBUTE_CONTENT "content"
#define OAI_ATTRIBUTE_DATESTAMP "datestamp"
#define OAI_ATTRIBUTE_SETSPEC "setspec"

#define OAI_SUCCESS 0
#define OAI_FAIL 1
#define OAI_UNKNOWN_REQUEST 2
#define ERROR_CODE_MISSING_PREFIX 1

#define OAI_OPT_ATTRIBUTE "oai_attribute"

PG_MODULE_MAGIC;

Datum oai_fdw_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(oai_fdw_handler);

typedef struct oai_fdw_state {
	int current_row;
	int end;

	Oid foreigntableid;
	List *foreignColumns;
	int numcols;
	int  rowcount;
	char *identifier;
	char *document;
	char *date;
	char *set;
	List *list;

	char *url;
	char *from;
	char *until;
	char *metadataPrefix;
	char *resumptionToken;
	int  completeListSize;

} oai_fdw_state;

typedef struct oai_Record {
	int rownumber;

	char *identifier;
	char *content;
	char *datestamp;
	ArrayType *setsArray;

} oai_Record;


typedef struct oai_fdw_TableOptions {

	Oid foreigntableid;
	List *foreignColumns;
	int numcols;
	char *metadataPrefix;
	char *from;
	char *until;
	char *url;
	char * set;

} oai_fdw_TableOptions;


//static const struct OAIFdwOption valid_options[] = {
//
//	{ "identifier",	ForeignTableRelationId },
//	{ "setspec",	ForeignTableRelationId },
//	{ "datestamp",	ForeignTableRelationId },
//	{ "content",	ForeignTableRelationId }
//
//};

struct string {
	char *ptr;
	size_t len;
};

//typedef struct OAIFdwExecState {
//
//	oai_fdw_TableOptions *table;
//}

void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
void oai_fdw_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
ForeignScan *oai_fdw_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
void oai_fdw_BeginForeignScan(ForeignScanState *node, int eflags);
TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node);
void oai_fdw_ReScanForeignScan(ForeignScanState *node);
void oai_fdw_EndForeignScan(ForeignScanState *node);

//void oai_fdw_LoadOAIDocuments(List **list, char *url, char *metadataPrefix, char *set, char *from, char * until);
void loadOAIRecords(oai_fdw_state ** state);
void init_string(struct string *s);
size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s);
int fetchNextOAIRecord(oai_fdw_state *state,oai_Record **oai_record);

void appendTextArray(ArrayType **array, char* text_element) ;
int getHttpResponse(oai_fdw_state **state, char* request, struct string *xmlResponse);

//Datum textToTimestamp(text* value,text* format);

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

int getHttpResponse(oai_fdw_state **state, char* request, struct string *xmlResponse) {

	CURL *curl;
	CURLcode res;
	char* postfields = palloc0(sizeof(char)*5000); //todo: estimate realistic mem size for palloc!

	elog(DEBUG1,"getHttpResponse called");

	if(strcmp(request,OAI_REQUEST_LISTRECORDS)==0) {

		sprintf(postfields,"verb=%s",request);

		if((*state)->set) {

			sprintf(postfields,"%s&set=%s",postfields,(*state)->set);
			elog(DEBUG1,"  getHttpResponse: appending 'set' > %s",(*state)->set);

		}

		if((*state)->from) {

			sprintf(postfields,"%s&from=%s",postfields,(*state)->from);
			elog(DEBUG1,"  getHttpResponse: appending 'from' > %s",(*state)->from);

		}

		if((*state)->until) {

			sprintf(postfields,"%s&until=%s",postfields,(*state)->until);
			elog(DEBUG1,"  getHttpResponse: appending 'until' > %s",(*state)->until);

		}

		if((*state)->metadataPrefix) {

			sprintf(postfields,"%s&metadataPrefix=%s",postfields,(*state)->metadataPrefix);
			elog(DEBUG1,"  getHttpResponse: appending 'metadataPrefix' > %s",(*state)->metadataPrefix);

		}

		if((*state)->resumptionToken) {

			sprintf(postfields,"verb=%s&resumptionToken=%s",request,(*state)->resumptionToken);
			elog(DEBUG1,"  getHttpResponse: appending 'resumptionToken' > %s",(*state)->resumptionToken);
			(*state)->list = NIL;
			(*state)->resumptionToken = NULL;

		}

	} else {

		return OAI_UNKNOWN_REQUEST;

	}

	elog(DEBUG1,"  getHttpResponse: url build > %s?%s",(*state)->url,postfields);

	curl = curl_easy_init();

	if(curl) {

		init_string(xmlResponse);

		curl_easy_setopt(curl, CURLOPT_URL, (*state)->url);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postfields);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, xmlResponse);

		res = curl_easy_perform(curl);

	} else {

		elog(DEBUG1, "  => getHttpResponse: curl failed");
		return OAI_FAIL;

	}

	curl_easy_cleanup(curl);

	elog(DEBUG2, "  => getHttpResponse: curl http_return > '%d'", res);

	//if(res != CURLE_OK) return OAI_REQUEST_FAIL;



	return OAI_SUCCESS;

}

//void isOAIException(char* xmlResponse) {
//
//
//
//}

void loadOAIRecords(oai_fdw_state **state) {

	struct string xmlResponse;
	int record_count = 0;

	int curl_response = getHttpResponse(state,OAI_REQUEST_LISTRECORDS,&xmlResponse);

	elog(DEBUG1,"oai_fdw_LoadOAIRecords: curl response > %d",curl_response);

	if(curl_response == OAI_SUCCESS) {

		xmlNodePtr xmlroot = NULL;
		xmlDocPtr xmldoc;
		xmlNodePtr recordsList;
		xmlNodePtr record;		
		xmlNodePtr header;
		xmlNodePtr metadata;
		xmlNodePtr headerList;
		oai_Record *oai_record;



		xmlInitParser();
		xmldoc = xmlReadMemory(xmlResponse.ptr, strlen(xmlResponse.ptr), NULL, NULL, XML_PARSE_SAX1);

		if (!xmldoc || (xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {

			xmlFreeDoc(xmldoc);
			xmlCleanupParser();

			ereport(ERROR,
					errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					errmsg("Invalid XML response for URL \"%s\"",(*state)->url));

		}

		if (xmlStrcmp(xmlroot->name, (xmlChar*) OAI_XML_ROOT_ELEMENT)) {

			ereport(ERROR,
					errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					errmsg("Invalid XML response for URL \"%s\"",(*state)->url),
					errhint("Make sure that the OAI-PMH service is running or check the URL given in the CREATE FOREIGN TABLE option."));

		}

		elog(DEBUG1,"oai_fdw_LoadOAIRecords: xmldoc parsed (xmlReadMemory)");


		for (recordsList = xmlroot->children; recordsList != NULL; recordsList = recordsList->next) {

			if (recordsList->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(recordsList->name, (xmlChar*)"error")==0) {

				char* oai_error_msg = (char*)xmlNodeGetContent(recordsList);
				char* oai_error_code = (char*)xmlGetProp(recordsList, (xmlChar*) "code");

				if(oai_error_msg && oai_error_code) {

					ereport(ERROR,
							errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
							errmsg("%s (%s)",oai_error_msg,oai_error_code));

				} else {

					ereport(ERROR,
							errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
							errmsg("Unknown OAI Server Error"));

				}


			}

			if (xmlStrcmp(recordsList->name, (xmlChar*)"ListRecords")!=0) continue;

			for (record = recordsList->children; record != NULL; record = record->next) {

				xmlBufferPtr buffer;

				if (record->type != XML_ELEMENT_NODE) continue;

				oai_record = palloc0(sizeof(oai_Record));
				buffer = xmlBufferCreate();

				if (xmlStrcmp(record->name, (xmlChar*) "record") == 0) {

					for (metadata = record->children; metadata != NULL; metadata = metadata->next) {

						if (metadata->type != XML_ELEMENT_NODE) continue;

						if (xmlStrcmp(metadata->name, (xmlChar*) "metadata") == 0) {

							xmlNodePtr oai_xml_document = metadata->children;

							size_t content_size = (size_t) xmlNodeDump(buffer, xmldoc, oai_xml_document, 0, 1);

							oai_record->content = palloc0(sizeof(char)*content_size);
							oai_record->content = (char*)buffer->content;
							oai_record->identifier = NULL;
							oai_record->datestamp = NULL;
							oai_record->rownumber = record_count;

							elog(DEBUG1,"oai_fdw_LoadOAIRecords: buffer size > %ld",content_size);

						}



					}





				} else if (xmlStrcmp(record->name, (xmlChar*) "resumptionToken") == 0) {

					size_t size_token = strlen((char*)xmlNodeGetContent(record));
					char * token = (char*)xmlNodeGetContent(record);

					elog(DEBUG1,"oai_fdw_LoadOAIRecords: resumptionToken found > %s",token);

					(*state)->resumptionToken = palloc0(sizeof(char)*size_token);
					(*state)->resumptionToken = token;

					continue;

				} else {

					continue;

				}

				for (header = record->children; header != NULL; header = header->next) {

					if (header->type != XML_ELEMENT_NODE) continue;
					if (xmlStrcmp(header->name, (xmlChar*) "header") != 0) continue;

					for (headerList = header->children; headerList!= NULL; headerList = headerList->next) {

						size_t size_node;
						char *node_content;

						if (headerList->type != XML_ELEMENT_NODE) continue;

						node_content = (char*)xmlNodeGetContent(headerList);
						size_node = strlen(node_content);

						if (xmlStrcmp(headerList->name, (xmlChar*) "identifier")==0){

							oai_record->identifier = palloc0(sizeof(char)*size_node);
							oai_record->identifier = node_content;

						}

						if (xmlStrcmp(headerList->name, (xmlChar*) "setSpec")==0){

							appendTextArray(&oai_record->setsArray,node_content);

						}

						if (xmlStrcmp(headerList->name, (xmlChar*) "datestamp")==0){

							oai_record->datestamp= palloc0(sizeof(char)*size_node);
							oai_record->datestamp= node_content;

						}

					}

				}

				if((*state)->list == NIL) {
					elog(DEBUG1,"oai_fdw_LoadOAIRecords: first record -> %s",oai_record->identifier);
					(*state)->list = list_make1(oai_record) ;
				} else {
					elog(DEBUG1,"oai_fdw_LoadOAIRecords: appending record -> %s",oai_record->identifier);
					list_concat((*state)->list, list_make1(oai_record));
				}

				record_count++;

				elog(DEBUG1,"oai_fdw_LoadOAIRecords: list size -> %d",(*state)->list->length);

				xmlBufferDetach(buffer);
				xmlBufferFree(buffer);

			}

		}

		xmlFreeDoc(xmldoc);
		xmlCleanupParser();

	} else {

		ereport(ERROR,
				errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				errmsg("Unable to connect to OAI-PMH Service: \"%s\"",(*state)->url),
				errhint("Make sure that the OAI-PMH service is running or check the URL given in the CREATE FOREIGN TABLE option."));

	}

	(*state)->rowcount = record_count;

}


void validateTableStructure(oai_fdw_TableOptions *opts, ForeignTable *ft) {


	Relation rel = table_open(ft->relid, NoLock);
	opts->numcols = rel->rd_att->natts;
	opts->foreigntableid = ft->relid;

	for (int i = 0; i < rel->rd_att->natts ; i++) {

		List * options = GetForeignColumnOptions(ft->relid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			DefElem*    def = (DefElem*) lfirst(lc);

			if (strcmp(def->defname, OAI_OPT_ATTRIBUTE)==0) {

				char * option_value = defGetString(def);

				if (strcmp(option_value,OAI_ATTRIBUTE_IDENTIFIER)==0){

					if (rel->rd_att->attrs[i].atttypid != TEXTOID &&
						rel->rd_att->attrs[i].atttypid != VARCHAROID) {

						//table_close(rel, NoLock);

						ereport(ERROR,
								errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("invalid data type for '%s.%s': %d",
										NameStr(rel->rd_rel->relname),
										NameStr(rel->rd_att->attrs[i].attname),
										rel->rd_att->attrs[i].atttypid),
								errhint("OAI %s must be of type `text` or `varchar`.",
										OAI_ATTRIBUTE_IDENTIFIER));

					}

				} else if (strcmp(option_value,OAI_ATTRIBUTE_CONTENT)==0){

					if (rel->rd_att->attrs[i].atttypid != TEXTOID &&
						rel->rd_att->attrs[i].atttypid != VARCHAROID &&
						rel->rd_att->attrs[i].atttypid != XMLOID) {

						//table_close(rel, NoLock);

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

						//table_close(rel, NoLock);

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

						//table_close(rel, NoLock);

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

				elog(DEBUG1,"createOAITuple: column %d option '%s'",i,option_value);

			}


		}

	}

	table_close(rel, NoLock);

}


void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	//Relation rel = table_open(foreigntableid, NoLock);
	ForeignTable *ft = GetForeignTable(foreigntableid);
	oai_fdw_TableOptions *opts = palloc0(sizeof(oai_fdw_TableOptions));
	ListCell *cell;


	validateTableStructure(opts, ft);


	int start = 0, end = 64; //TODO necessary?

	//GetForeignColumnOptions

	//elog(DEBUG1,">>>>>>>>>> %d",rel->rd_att->attrs[0].attnum);



	//	if (rel->rd_att->natts == 0) {
//		ereport(ERROR,
//				errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
//				errmsg("Foreign Table '%s' has no columns. An OAI foreign table table must contain at least one column.", NameStr(rel->rd_rel->relname)),
//				errhint("Expected columns are:  identifier (text), content (text/xml), set (text) and datestamp (timestamp)."));
//	}

	//TODO: Oid typid = rel->rd_att->attrs[0].atttypid;
/*
	if( (rel->rd_att->attrs[0].atttypid != TEXTOID && rel->rd_att->attrs[0].atttypid != VARCHAROID) ||
		(rel->rd_att->attrs[1].atttypid != TEXTOID && rel->rd_att->attrs[1].atttypid != XMLOID  && rel->rd_att->attrs[1].atttypid != VARCHAROID)||
		(rel->rd_att->attrs[2].atttypid != TEXTARRAYOID && rel->rd_att->attrs[2].atttypid != VARCHARARRAYOID) ||
  		rel->rd_att->attrs[3].atttypid != TIMESTAMPOID )  {

		ereport(ERROR,
				errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				errmsg("Incorrect schema for oai_fdw table \"%s\".", NameStr(rel->rd_rel->relname)),
				errhint("Expected data types are (in this exact order!): text, text/xml, text, timestamp."));

	}


	*/




	foreach(cell, ft->options) {

		DefElem *def = lfirst_node(DefElem, cell);

		if (strcmp("url", def->defname) == 0) {

			opts->url = defGetString(def);

		} else if (strcmp("metadataprefix", def->defname) == 0) {

			opts->metadataPrefix = defGetString(def);

		} else if (strcmp("set", def->defname) == 0) {

			opts->set = defGetString(def);

		} else if (strcmp("from", def->defname) == 0) {

			opts->from= defGetString(def);

		} else if (strcmp("until", def->defname) == 0) {

			opts->until= defGetString(def);

		} else {

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid option \"%s\"", def->defname),
							errhint("Valid table options for oai_fdw are \"url\",\"metadataPrefix\",\"set\",\"from\" and \"until\"")));
		}
	}


	if(!opts->url){

		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("'url' not found"),
						errhint("'url' is a required option. Check the CREATE FOREIGN TABLE options.")));

	}

	if(!opts->metadataPrefix){

		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("'metadataPrefix' not found"),
						errhint("metadataPrefix is a required argument (unless the exclusive argument resumptionToken is used). Check the CREATE FOREIGN TABLE options.")));

	}



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
	oai_fdw_state * state = palloc0(sizeof(oai_fdw_state));

	state->url = opts->url;
	state->set = opts->set;
	state->from = opts->from;
	state->until = opts->until;
	state->metadataPrefix = opts->metadataPrefix;
	state->foreigntableid = opts->foreigntableid;
	state->numcols = opts->numcols;

	//elog(DEBUG1,"GetForeignPlan set = %s",opts->set);

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

void createOAITuple(TupleTableSlot *slot, oai_fdw_state *state, oai_Record *oai_record ) {


	//ForeignTable *ft = GetForeignTable(state->foreigntableid);
//	Relation rel = table_open(state->foreigntableid, NoLock);

	//if( (rel->rd_att->attrs[0].atttypid != TEXTOID && rel->rd_att->attrs[0].atttypid != VARCHAROID) ||

	for (int i = 0; i < state->numcols; i++) {

		List * options = GetForeignColumnOptions(state->foreigntableid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			DefElem*    def = (DefElem*) lfirst(lc);

			if (strcmp(def->defname, OAI_OPT_ATTRIBUTE)==0) {

				char * option_value = defGetString(def);

				if (strcmp(option_value,OAI_ATTRIBUTE_IDENTIFIER)==0){

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai_record->identifier);

				} else if (strcmp(option_value,OAI_ATTRIBUTE_CONTENT)==0){

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai_record->content);

				} else if (strcmp(option_value,OAI_ATTRIBUTE_SETSPEC)==0){

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = DatumGetArrayTypeP(oai_record->setsArray);

				}  else if (strcmp(option_value,OAI_ATTRIBUTE_DATESTAMP)==0){

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai_record->datestamp);

				}

				elog(DEBUG1,"createOAITuple: column %d option '%s'",i,option_value);

			}

		}

	}



//	slot->tts_isnull[0] = false;
//	slot->tts_values[0] = CStringGetTextDatum(oai_record->identifier);

//	slot->tts_isnull[1] = false;
//	slot->tts_values[1] = CStringGetTextDatum(oai_record->content);
//
//	slot->tts_isnull[2] = false;
//	slot->tts_values[2] = DatumGetArrayTypeP(oai_record->setsArray);
//
//	slot->tts_isnull[3] = false;
//	slot->tts_values[3] = CStringGetTextDatum(oai_record->datestamp);

}

TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node) {

	int hasnext;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	oai_fdw_state *state = node->fdw_state;
	oai_Record * oai_record = palloc0(sizeof(oai_Record));;

	ExecClearTuple(slot);

	hasnext = fetchNextOAIRecord(state,&oai_record);

	if(hasnext){

		elog(DEBUG1,"oai_fdw_IterateForeignScan: xml length > %ld",strlen(oai_record->content));
		elog(DEBUG1,"oai_fdw_IterateForeignScan: identifier > %s",oai_record->identifier);
		elog(DEBUG1,"oai_fdw_IterateForeignScan: datestamp  > %s",oai_record->datestamp);

		createOAITuple(slot, state, oai_record);

		//text*format="YYYY-MM-DDThh:mi:ssZ";


		//slot->tts_values[3] = textToTimestamp(oai_record->datestamp,format);
		//slot->tts_values[3] = TimestampGetDatum(GetCurrentTimestamp());
		//slot->tts_values[3] = TimestampGetDatum(castNode(Timestamp, oai_record->set_array));

		ExecStoreVirtualTuple(slot);

		state->current_row= state->current_row+1;

	}

	return slot;

}

int fetchNextOAIRecord(oai_fdw_state *state,oai_Record **oai_record){

	elog(DEBUG1,"fetchNextOAIRecord: current_row > %d",state->current_row);

	/* Loads documents in the "oai_fdw_state" in case it is the first pass or if it reached
	 * the end of the list and there is a resumption token for the next page. */
	if(state->list == NIL || (state->current_row == state->list->length && state->resumptionToken) ) {

		elog(DEBUG1,"fetchNextOAIRecord: loading next page ...");
		loadOAIRecords(&state);
		state->current_row = 0;

	}


	if(state->current_row == state->list->length && !state->resumptionToken ) return 0;

	*oai_record = (oai_Record *) lfirst(list_nth_cell(state->list, state->current_row));

	elog(DEBUG1,"fetchNextOAIEntry: list length > %d",state->list->length);
	elog(DEBUG1,"fetchNextOAIEntry: xml length  > %ld",strlen((*oai_record)->content));
	elog(DEBUG1,"fetchNextOAIEntry: identifier  > %s",(*oai_record)->identifier);

	return 1; //TODO: return size content?

}


void appendTextArray(ArrayType **array, char* text_element) {

	/* Array build variables */
	size_t arr_nelems = 0;
	size_t arr_elems_size = 1;
	Oid elem_type = TEXTOID;
	int16 elem_len;
	bool elem_byval;
	char elem_align;
	Datum *arr_elems = palloc(arr_elems_size*sizeof(Datum));

	elog(DEBUG1,"appendTextArray called with element > %s",text_element);

	get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

	if(*array == NULL) {

		/*Array has no elements */
		arr_elems[arr_nelems] = CStringGetTextDatum(text_element);
		elog(DEBUG1,"  array empty! adding value at arr_elems[%ld]",arr_nelems);
		arr_nelems++;

	} else {

		bool isnull;
		Datum value;
		ArrayIterator iterator = array_create_iterator(*array,0,NULL);
		elog(DEBUG1,"  current array size: %d",ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array)));

		arr_elems_size *= ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array))+1;
		arr_elems = repalloc(arr_elems, arr_elems_size*sizeof(Datum));

		while (array_iterate(iterator, &value, &isnull)) {

			if (isnull) continue;

			elog(DEBUG1,"  re-adding element: arr_elems[%ld]. arr_elems_size > %ld",arr_nelems,arr_elems_size);

			arr_elems[arr_nelems] = value;
			arr_nelems++;

		}
		array_free_iterator(iterator);

		elog(DEBUG1,"  adding new element: arr_elems[%ld]. arr_elems_size > %ld",arr_nelems,arr_elems_size);
		arr_elems[arr_nelems++] = CStringGetTextDatum(text_element);

	}


	elog(DEBUG1,"  => construct_array called: arr_nelems > %ld arr_elems_size %ld",arr_nelems,arr_elems_size);

	*array = construct_array(arr_elems, arr_nelems, elem_type, elem_len, elem_byval, elem_align);

}

//Datum textToTimestamp(text* value,text* format){
//
//	elog(DEBUG1,"textToTimestamp called with value = \"%s\" and format = \"%s\"",(char*)value,(char*)format);
//
//	Oid collid = DEFAULT_COLLATION_OID;
//	Oid typid;
//	int tz = 0;
//	int32 typmod =-1;
//
//
//	text *date = palloc(sizeof(text)*1000);
//	date = cstring_to_text_with_len("2015-07-15T09:38:22Z",strlen("2015-07-15T09:38:22Z"));
//
//	text *date_format = palloc(sizeof(text)*1000);
//	date_format = cstring_to_text_with_len("YYYY-MM-DDThh:mi:ssZ",strlen("YYYY-MM-DDThh:mi:ssZ"));
//
//
//
////
//	struct tm tm;
//	strptime((char*)value, "%Y-%m-%dT%OH:%M:%S%Z", &time);
//	time_t loctime = mktime(&time);  // timestamp in current timezone
//
//	elog(DEBUG1,">>>> %ld",loctime);
//
//
//	//return TimestampGetDatum(pgtm);
//
//	text *dt = "2015-07-15T09:38:22Z";
//	text *frmt = "YYYY-MM-DDThh:mi:ssZ";
//
//	return castNode(Timestamp, dt);
//
//}


void oai_fdw_ReScanForeignScan(ForeignScanState *node) {

	oai_fdw_state *state = node->fdw_state;
	state->current_row = 0;

}

void oai_fdw_EndForeignScan(ForeignScanState *node) {
}
