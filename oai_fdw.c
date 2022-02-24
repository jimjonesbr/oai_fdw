
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
#include "nodes/nodes.h"
//#include "nodes/primnodes.h"
//#include "utils/date.h"
#include "utils/datetime.h"

#include "lib/stringinfo.h"

#include "utils/timestamp.h"
//#include "catalog/pg_cast.h"
#include "utils/formatting.h"


//#include "utils/pg_locale.h"
//#define LIBXML_OUTPUT_ENABLED //??
//#define LIBXML_TREE_ENABLED //??

#include "catalog/pg_foreign_table.h"

#define OAI_FDW_VERSION "1.0.0dev"
#define OAI_REQUEST_LISTRECORDS "ListRecords"
#define OAI_REQUEST_IDENTIFY "Identify"
#define OAI_REQUEST_LISTSETS "ListSets"
#define OAI_REQUEST_GETRECORD "GetRecord"
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

	//xmlNodePtr xmlroot;
	xmlNodePtr xmlroot_listrecords;

	char* current_identifier;

} oai_fdw_state;

typedef struct oai_Record {
	int rownumber;

	char *identifier;
	char *content;
	char *datestamp;
	ArrayType *setsArray;

	xmlBufferPtr doc;
	//StringInfoData xmlContent;

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
int executeOAIRequest(oai_fdw_state **state, char* request, struct string *xmlResponse);
void listRecordsRequest(oai_fdw_state *state);
void createOAITuple(TupleTableSlot *slot, oai_fdw_state *state, oai_Record *oai );
oai_Record *fetchNextRecord(TupleTableSlot *slot, oai_fdw_state *state);
int loadOAIRecords(oai_fdw_state *state);

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

int executeOAIRequest(oai_fdw_state **state, char* request, struct string *xmlResponse) {

	CURL *curl;
	CURLcode res;
	StringInfoData url_bufffer;
	initStringInfo(&url_bufffer);

	elog(DEBUG1,"executeOAIRequest called: url > %s ",(*state)->url);

	appendStringInfo(&url_bufffer,"verb=%s",request);

	if(strcmp(request,OAI_REQUEST_LISTRECORDS)==0) {

		if((*state)->set) {

			elog(DEBUG2,"  %s: appending 'set' > %s",request,(*state)->set);
			appendStringInfo(&url_bufffer,"&set=%s",(*state)->set);

		}

		if((*state)->from) {

			elog(DEBUG2,"  %s: appending 'from' > %s",request,(*state)->from);
			appendStringInfo(&url_bufffer,"&from=%s",(*state)->from);


		}

		if((*state)->until) {

			elog(DEBUG2,"  %s: appending 'until' > %s",request,(*state)->until);
			appendStringInfo(&url_bufffer,"&until=%s",(*state)->until);

		}

		if((*state)->metadataPrefix) {

			elog(DEBUG2,"  %s: appending 'metadataPrefix' > %s",request,(*state)->metadataPrefix);
			appendStringInfo(&url_bufffer,"&metadataPrefix=%s",(*state)->metadataPrefix);

		}

		if((*state)->resumptionToken) {

			elog(DEBUG2,"  %s: appending 'resumptionToken' > %s",request,(*state)->resumptionToken);
			resetStringInfo(&url_bufffer);
			appendStringInfo(&url_bufffer,"verb=%s&resumptionToken=%s",request,(*state)->resumptionToken);

			(*state)->list = NIL;
			(*state)->resumptionToken = NULL;

		}

	} else if(strcmp(request,OAI_REQUEST_GETRECORD)==0) {

		if((*state)->identifier) {

			elog(DEBUG2,"  %s: appending 'identifier' > %s",request,(*state)->identifier);
			appendStringInfo(&url_bufffer,"&identifier=%s",(*state)->identifier);

		}

		if((*state)->metadataPrefix) {

			elog(DEBUG2,"  %s: appending 'metadataPrefix' > %s",request,(*state)->metadataPrefix);
			appendStringInfo(&url_bufffer,"&metadataPrefix=%s",(*state)->metadataPrefix);

		}

	} else {

		return OAI_UNKNOWN_REQUEST;

	}


	elog(DEBUG1,"  %s: url build > %s?%s",request,(*state)->url,url_bufffer.data);

	curl = curl_easy_init();

	if(curl) {

		init_string(xmlResponse);

		curl_easy_setopt(curl, CURLOPT_URL, (*state)->url);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, url_bufffer.data);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, xmlResponse);

		res = curl_easy_perform(curl);

	} else {

		elog(DEBUG2, "  => %s: curl failed",request);
		return OAI_FAIL;

	}

	curl_easy_cleanup(curl);

	elog(DEBUG2, "  => %s: curl http_return > '%d'",request, res);

	return OAI_SUCCESS;


}

void listRecordsRequest(oai_fdw_state *state) {

	struct string xmlStream;
	int oaiExecuteResponse;

	elog(DEBUG1,"listRecordsRequest called");

	oaiExecuteResponse = executeOAIRequest(&state,OAI_REQUEST_LISTRECORDS,&xmlStream);

	if(oaiExecuteResponse == OAI_SUCCESS) {

		xmlDocPtr xmldoc;
		xmlNodePtr recordsList;

		xmlInitParser();

		xmldoc = xmlReadMemory(xmlStream.ptr, strlen(xmlStream.ptr), NULL, NULL, XML_PARSE_SAX1);


		if (!xmldoc || (state->xmlroot_listrecords = xmlDocGetRootElement(xmldoc)) == NULL) {

			xmlFreeDoc(xmldoc);
			xmlCleanupParser();

			ereport(ERROR, errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),	errmsg("Invalid XML response for URL \"%s\"",state->url));

		}

		for (recordsList = state->xmlroot_listrecords->children; recordsList != NULL; recordsList = recordsList->next) {

			if (recordsList->type != XML_ELEMENT_NODE) continue;

			if (xmlStrcmp(recordsList->name, (xmlChar*)"ListRecords")==0) {

				if (!xmldoc || (state->xmlroot_listrecords = xmlDocGetRootElement(xmldoc)) == NULL) {

					xmlFreeDoc(xmldoc);
					xmlCleanupParser();

				}

			}

		}

	}

}

void validateTableStructure(oai_fdw_TableOptions *opts, ForeignTable *ft) {

	Relation rel = table_open(ft->relid, NoLock);
	opts->numcols = rel->rd_att->natts;
	opts->foreigntableid = ft->relid;

//	elog(DEBUG1,"%s",rel->rd_rel->);

	for (int i = 0; i < rel->rd_att->natts ; i++) {

		List *options = GetForeignColumnOptions(ft->relid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			DefElem *def = (DefElem*) lfirst(lc);

			if (strcmp(def->defname, OAI_OPT_ATTRIBUTE)==0) {

				char *option_value = defGetString(def);

				if (strcmp(option_value,OAI_ATTRIBUTE_IDENTIFIER)==0){

					if (rel->rd_att->attrs[i].atttypid != TEXTOID &&
						rel->rd_att->attrs[i].atttypid != VARCHAROID) {

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

				elog(DEBUG1,"createOAITuple: column %d option '%s'",i,option_value);

			}

		}

	}

	table_close(rel, NoLock);

}

void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	//Relation rel = table_open(foreigntableid, NoLock);
	ForeignTable *ft = GetForeignTable(foreigntableid);
	oai_fdw_TableOptions *opts = (oai_fdw_TableOptions *)palloc0(sizeof(oai_fdw_TableOptions));
	ListCell *cell;


	validateTableStructure(opts, ft);




//	List* conditions = baserel->baserestrictinfo;
//	ListCell *cell2;
//
//	conditions = extract_actual_clauses(conditions, false);
//
//	foreach(cell2, conditions){
//
//		//Expr *expr =((RestrictInfo*)cell)->clause;
//
//		elog(DEBUG1,"GOT HERE!");
//
//		Expr  *expr = (Expr *) lfirst(cell2);
//
//				/* extract clause from RestrictInfo, if required */
//				if (IsA(expr, RestrictInfo))
//				{
//					RestrictInfo *ri = (RestrictInfo *) expr;
//					expr = ri->clause;
//				}
//
//		//elog(ERROR,"GOT HERE!: %s", clause->type);
//		//elog(ERROR,"GOT HERE!: %s", ((RestrictInfo*)cell)->clause->type);
//
//	}



	int start = 0, end = 64; //TODO necessary?




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
	oai_fdw_state *state = (oai_fdw_state *)palloc0(sizeof(oai_fdw_state));

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

oai_Record *fetchNextRecord(TupleTableSlot *slot, oai_fdw_state *state) {


	xmlDocPtr xmldoc;
	xmlNodePtr headerList;
	xmlNodePtr header;
	xmlNodePtr recordsList;
	xmlNodePtr rec;
	xmlNodePtr metadata;

//	char *identifier;
//	char *datestamp;
//	char *specSets;

	oai_Record *oai = (oai_Record*) palloc0(sizeof(oai_Record));

	bool getNext = false;

	xmlInitParser(); //????????????????

	for (recordsList = state->xmlroot_listrecords->children; recordsList!= NULL; recordsList = recordsList->next) {

		if (recordsList->type != XML_ELEMENT_NODE) continue;

		if (xmlStrcmp(recordsList->name, (xmlChar*)"ListRecords")!=0) continue;

		for (rec = recordsList->children; rec != NULL; rec = rec->next) {

			if (rec->type != XML_ELEMENT_NODE) continue;

			if(rec->next) {

				if (xmlStrcmp(rec->next->name, (xmlChar*)"resumptionToken")==0) {

					char *token = (char*)xmlNodeGetContent(rec->next);

					if(strlen(token) != 0) {

						state->resumptionToken = token;
						elog(DEBUG1,"FETCHNEXTRECORD_NEW: Token detected in current page > %s",token);

					}
				}

			}



			for (metadata = rec->children; metadata != NULL; metadata = metadata->next) {

				if (metadata->type != XML_ELEMENT_NODE) continue;

				if (xmlStrcmp(metadata->name, (xmlChar*) "metadata") == 0) {

					xmlNodePtr oai_xml_document = metadata->children;
					xmlBufferPtr buffer = xmlBufferCreate();
					//size_t content_size = (size_t) xmlNodeDump(buffer, xmldoc, oai_xml_document, 0, 1);
					size_t content_size = (size_t) xmlNodeDump(buffer, xmldoc, oai_xml_document, 0, 1);

					elog(DEBUG2,"XML Buffer size: %ld",content_size);

					oai->content = (char*) buffer->content;
					oai->setsArray = NULL;

					for (header = rec->children; header != NULL; header = header->next) {


						if (header->type != XML_ELEMENT_NODE) continue;
						if (xmlStrcmp(header->name, (xmlChar*) "header") != 0) continue;


						for (headerList = header->children; headerList!= NULL; headerList = headerList->next) {

							//size_t size_node;
							char *node_content;

							if (headerList->type != XML_ELEMENT_NODE) continue;

							node_content = (char*)xmlNodeGetContent(headerList);
							//size_node = strlen(node_content);

							if (xmlStrcmp(headerList->name, (xmlChar*) "identifier")==0){

								//oai_record->identifier = (char*)palloc(sizeof(char)*size_node+1);
								oai->identifier = node_content;

							}

							if (xmlStrcmp(headerList->name, (xmlChar*) "setSpec")==0){

								//oai_record->setsArray = (ArrayType*)palloc0(sizeof(ArrayType));
								//						oai->setsArray = (ArrayType*)palloc0(sizeof(ArrayType));

								//						appendTextArray(&oai->setsArray,node_content);

								appendTextArray(&oai->setsArray,node_content);


							}

							if (xmlStrcmp(headerList->name, (xmlChar*) "datestamp")==0){

								//oai_record->datestamp= (char*)palloc(sizeof(char)*size_node+1);
								oai->datestamp= node_content;

							}

						}

					}

				}

			}



			if(!state->current_identifier) {

				state->current_identifier = oai->identifier;
				elog(DEBUG1,"setting first current identifier > %s",state->current_identifier );
				return oai;

			}

			if (strcmp(state->current_identifier,oai->identifier)==0){

				elog(DEBUG1,"there is a match! > %s",oai->identifier);
				getNext = true;

			} else if(getNext == true) {

				state->current_identifier = oai->identifier;
				elog(DEBUG1,"setting new current identifier > %s",state->current_identifier);
				return oai;

			}

//			if(rec->next) {
//
//				if (xmlStrcmp(rec->next->name, (xmlChar*)"resumptionToken")==0) {
//
//					elog(DEBUG1,"REALODING OAI RECORDS > %s",xmlNodeGetContent(rec->next));
//
//					state->resumptionToken = xmlNodeGetContent(rec->next);
//					//state->current_identifier = NULL;
//					state->rowcount = 0;
//
//					//loadOAIRecords_NEW(state);
//
//				}
//
//			}

			if(!rec->next && !state->resumptionToken) 	return NULL;

		}

	}


	//return oai;

}

void createOAITuple(TupleTableSlot *slot, oai_fdw_state *state, oai_Record *oai ) {

	elog(DEBUG2,"createOAITuple called: numcols = %d",state->numcols);
	elog(DEBUG2,"createOAITuple called: \n\n "
			"- identifier > %s\n "
			"- datestamp: %s\n "
			"- content: %s\n",oai->identifier,oai->datestamp,oai->content);

		for (int i = 0; i < state->numcols; i++) {

			List * options = GetForeignColumnOptions(state->foreigntableid, i+1);
			ListCell *lc;

			foreach (lc, options) {

				DefElem *def = (DefElem*) lfirst(lc);

				if (strcmp(def->defname, OAI_OPT_ATTRIBUTE)==0) {

					char * option_value = defGetString(def);

					if (strcmp(option_value,OAI_ATTRIBUTE_IDENTIFIER)==0){

						elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

						slot->tts_isnull[i] = false;
						slot->tts_values[i] = CStringGetTextDatum(oai->identifier);


					} else if (strcmp(option_value,OAI_ATTRIBUTE_CONTENT)==0){

						elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);
						//elog(DEBUG1,"  createOAITuple: StringInfo data > '%s'",oai_record->xmlContent->data);
						//elog(DEBUG1,"    createOAITuple: content size > '%d'",strlen(oai_record->content));
						slot->tts_isnull[i] = false;
						slot->tts_values[i] = CStringGetTextDatum((char*)oai->content);
						//slot->tts_values[i] = CStringGetTextDatum(pstrdup(oai_record->xmlContent.data));
						//elog(DEBUG1,"    createOAITuple: content added to tuple > '%s'",oai_record->xmlContent.data);
						//elog(DEBUG1,"    createOAITuple: content added to tuple > '%d'",strlen(oai_record->xmlContent.data));

					} else if (strcmp(option_value,OAI_ATTRIBUTE_SETSPEC)==0){

						elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);
						//elog(DEBUG1,"  createOAITuple: array ndim %d",oai_record->setsArray->ndim);

						slot->tts_isnull[i] = false;
						slot->tts_values[i] = DatumGetArrayTypeP(oai->setsArray);
						elog(DEBUG2,"    createOAITuple: setspec added to tuple");




					}  else if (strcmp(option_value,OAI_ATTRIBUTE_DATESTAMP)==0){

						char workBuffer[MAXDATELEN + 1];
						char *fieldArray[MAXDATEFIELDS];
						int fieldTypeArray[MAXDATEFIELDS];
						int fieldCount = 0;
						int parseError = ParseDateTime(oai->datestamp, workBuffer, sizeof(workBuffer), fieldArray, fieldTypeArray, MAXDATEFIELDS, &fieldCount);

						elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

						//elog(DEBUG2,"    createOAITuple: parsing datestamp \"%s\"",oai->datestamp);



						//elog(DEBUG2,"    createOAITuple: datestamp parser OK");

						if (parseError == 0) {

							int dateType = 0;
							struct pg_tm tm;
							fsec_t fsec = 0;
							int timezone = 0;

							int decodeError = DecodeDateTime(fieldArray, fieldTypeArray, fieldCount, &dateType, &tm, &fsec, &timezone);

							Timestamp tmsp;
							tm2timestamp(&tm, fsec, fieldTypeArray, &tmsp);


							if (decodeError == 0) {

								elog(DEBUG2,"    createOAITuple: datestamp can be decoded");

								slot->tts_isnull[i] = false;
								slot->tts_values[i] = DatumGetTimestamp(tmsp);

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
		elog(DEBUG2,"  createOAITuple finished");

}

TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node) {

	int hasnext;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	oai_fdw_state *state = node->fdw_state;
	oai_Record *oai = (oai_Record *)palloc0(sizeof(oai_Record));


	elog(DEBUG1,"oai_fdw_IterateForeignScan called");

	ExecClearTuple(slot);

	if(state->rowcount == 0 || state->resumptionToken) {

		elog(DEBUG2,"  oai_fdw_IterateForeignScan: loading OAI records (token: \"%s\")",state->resumptionToken);

		loadOAIRecords(state);
		state->current_identifier = NULL;

	}


	oai = fetchNextRecord(slot, state);

	if(oai) {

		createOAITuple(slot, state, oai);

		ExecStoreVirtualTuple(slot);

	}

	elog(DEBUG2,"  => oai_fdw_IterateForeignScan: returning tuple (rowcount: %d)",state->rowcount);

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
