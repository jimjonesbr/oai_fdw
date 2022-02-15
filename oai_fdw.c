
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
#include "commands/explain.h"
#include <curl/curl.h>
#include <libxml2/libxml/tree.h> //TODO: CORRECT PATH

#include <funcapi.h> //TODO:NECESSARY?

#define LIBXML_OUTPUT_ENABLED
#define LIBXML_TREE_ENABLED

PG_MODULE_MAGIC;

Datum oai_fdw_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(oai_fdw_handler);

typedef struct oai_fdw_state {
	int current_row;
	int end;

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
	ArrayType *set_array;

} oai_Record;


typedef struct oai_fdw_TableOptions {

	char *metadataPrefix;
	char *from;
	char *until;
	char *url;
	char * set;

} oai_fdw_TableOptions;


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

//void oai_fdw_LoadOAIDocuments(List **list, char *url, char *metadataPrefix, char *set, char *from, char * until);
void loadOAIRecords(oai_fdw_state ** state);
void init_string(struct string *s);
size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s);
int fetchNextOAIRecord(oai_fdw_state *state,oai_Record **entry);

void appendTextArray(ArrayType **array, char* text_element) ;


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

void loadOAIRecords(oai_fdw_state **state) {


	char *postfields;
	int record_count = 0;

	postfields = palloc0(sizeof(char)*5000); //todo: calculate real size for malloc!

	if(!(*state)->resumptionToken) {

		sprintf(postfields,"verb=ListRecords&from=%s&until=%s&metadataPrefix=%s&set=%s",(*state)->from,(*state)->until,(*state)->metadataPrefix,(*state)->set);

	} else {

		sprintf(postfields,"verb=ListRecords&resumptionToken=%s",(*state)->resumptionToken);
		(*state)->list = NIL;
		(*state)->resumptionToken = NULL;

	}

	elog(DEBUG1,"%s?%s",(*state)->url,postfields);

	CURL *curl;
	CURLcode res;

	curl = curl_easy_init();

	if(curl) {

		struct string s;
		init_string(&s);
		//appendBinaryStringInfo(&s, VARDATA(content_text), content_size);

		curl_easy_setopt(curl, CURLOPT_URL, (*state)->url);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, postfields);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writefunc);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &s);

		res = curl_easy_perform(curl);

		elog(DEBUG2, "http_return '%d'", res);



		xmlNodePtr xmlroot = NULL;
		xmlDocPtr xmldoc;
		int xml_size;
		char *xml;

		xmlInitParser();
		xmldoc = xmlReadMemory(s.ptr, strlen(s.ptr), NULL, NULL, XML_PARSE_SAX1);

		if (!xmldoc || (xmlroot = xmlDocGetRootElement(xmldoc)) == NULL) {

			xmlFreeDoc(xmldoc);
			xmlCleanupParser();

			elog(ERROR,"invalid XML response.");

		}

		xmlNodePtr list_records;

		for (list_records = xmlroot->children; list_records != NULL; list_records = list_records->next) {

			if (list_records->type != XML_ELEMENT_NODE) continue;
			if (xmlStrcmp(list_records->name, (xmlChar*)"ListRecords")!=0) continue;

			xmlNodePtr record;

			for (record = list_records->children; record != NULL; record = record->next) {

				if (record->type != XML_ELEMENT_NODE) continue;

				oai_Record *doc = palloc0(sizeof(oai_Record));
				xmlBufferPtr buffer = xmlBufferCreate();

				if (xmlStrcmp(record->name, (xmlChar*) "record") == 0) {

					size_t content_size = (size_t) xmlNodeDump(buffer, xmldoc, record, 0, 1);

					elog(DEBUG1,"oai_fdw_LoadOAIRecords: buffer size > %ld",content_size);

					doc->content = palloc0(sizeof(char)*content_size);
					doc->content = (char*)buffer->content;
					doc->identifier = NULL;
					//doc->setspec = NULL;
					doc->datestamp = NULL;
					doc->rownumber = record_count;

				} else if (xmlStrcmp(record->name, (xmlChar*) "resumptionToken") == 0){

					size_t size_token = strlen(xmlNodeGetContent(record));
					char * token = xmlNodeGetContent(record);

					elog(DEBUG1,"oai_fdw_LoadOAIRecords: resumptionToken found > %s",token);

					(*state)->resumptionToken = palloc0(sizeof(char)*size_token);
					(*state)->resumptionToken = token;

					continue;

				} else {

					continue;

				}


				xmlNodePtr header;

				for (header = record->children; header != NULL; header = header->next) {

					if (header->type != XML_ELEMENT_NODE) continue;
					if (xmlStrcmp(header->name, (xmlChar*) "header") != 0) continue;

					xmlNodePtr header_attributes;

					for (header_attributes = header->children; header_attributes!= NULL; header_attributes = header_attributes->next) {

						size_t size_node;

						if (header_attributes->type != XML_ELEMENT_NODE) continue;

						 char * node_content = xmlNodeGetContent(header_attributes);
						 size_node = strlen(node_content);

						if (xmlStrcmp(header_attributes->name, (xmlChar*) "identifier")==0){

							doc->identifier = palloc0(sizeof(char)*size_node);
							//doc->identifier = (char*) xmlNodeGetContent(header_attributes);
							doc->identifier = node_content;

						}

						if (xmlStrcmp(header_attributes->name, (xmlChar*) "setSpec")==0){


							appendTextArray(&doc->set_array,node_content);

//							if(!doc->setspec) {
//
//								doc->setspec= palloc0(sizeof(char)*size_node);
//								doc->setspec= node_content;
//
//							} else {

								//TODO: create array as result set!


//								char *current_sets = palloc0(sizeof(char)*strlen(doc->setspec));
//								size_t size_current_sets = strlen(current_sets);
//								snprintf(current_sets,strlen(doc->setspec),"%s",doc->setspec);
//
//								doc->setspec= palloc0(sizeof(char)*(size_node + size_current_sets));
//								snprintf(doc->setspec,size_node + size_current_sets + 2,"%s, %s",current_sets, node_content);


							//}

							//doc->setspec= (char*) xmlNodeGetContent(header_attributes);


						}

						if (xmlStrcmp(header_attributes->name, (xmlChar*) "datestamp")==0){

							doc->datestamp= palloc0(sizeof(char)*size_node);
							//doc->datestamp= (char*) xmlNodeGetContent(header_attributes);
							doc->datestamp= node_content;

						}


					}

				}

				if((*state)->list == NIL) {
					elog(DEBUG1,"oai_fdw_LoadOAIRecords: first record -> %s",doc->identifier);
					(*state)->list = list_make1(doc) ;
				} else {
					elog(DEBUG1,"oai_fdw_LoadOAIRecords: appending record -> %s",doc->identifier);
					list_concat((*state)->list, list_make1(doc));
				}

				record_count++;

				elog(DEBUG1,"oai_fdw_LoadOAIRecords: list size -> %d",(*state)->list->length);

				xmlBufferDetach(buffer);
				xmlBufferFree(buffer);

			}

		}

		xmlFreeDoc(xmldoc);
		xmlCleanupParser();

	}

	curl_easy_cleanup(curl);

	(*state)->rowcount = record_count;

}



void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	Relation rel = table_open(foreigntableid, NoLock);

	if (rel->rd_att->natts != 4) {
		ereport(ERROR,
				errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
				errmsg("Incorrect schema for oai_fdw table \"%s\". An OAI foreign table table must have exactly four columns.", NameStr(rel->rd_rel->relname)),
				errhint("Expected columns are:  identifier (text), content (text/xml), set (text) and datestamp (timestamp)."));
	}

	Oid typid = rel->rd_att->attrs[0].atttypid;
//	if (typid != INT4OID) {
//		ereport(ERROR,
//				errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
//				errmsg("incorrect schema for oai_fdw table %s: table column must have type int", NameStr(rel->rd_rel->relname)));
//	}

	table_close(rel, NoLock);

	int start = 0, end = 64;

	ForeignTable *ft = GetForeignTable(foreigntableid);
	oai_fdw_TableOptions *opts = palloc0(sizeof(oai_fdw_TableOptions));

	ListCell *cell;
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


//	if (start < 0) {
//		ereport(ERROR,
//				errcode(ERRCODE_FDW_ERROR),
//				errmsg("invalid value for option \"start\": must be non-negative"));
//	}
//	if (end < 0) {
//		ereport(ERROR,
//				errcode(ERRCODE_FDW_ERROR),
//				errmsg("invalid value for option \"end\": must be non-negative"));
//	}
//	if (end < start) {
//		ereport(ERROR,
//				errcode(ERRCODE_FDW_ERROR),
//				errmsg("invalid values for option \"start\" and \"end\": end must be >= start"));
//	}


	baserel->rows = end - start;


//	opts->start = start;
//	opts->end = end;
//
	//opts->from = from;
	//opts->until = until;
	//opts->url = url;
	//opts->set = set;
	//opts->metadataPrefix = metadataPrefix;

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

	if(eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

	oai_fdw_state *state = (oai_fdw_state *) fs->fdw_private;
	state->current_row = 0;

	node->fdw_state = state;

}


TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node) {

	int hasnext;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	oai_fdw_state *state = node->fdw_state;
	oai_Record * entry = palloc0(sizeof(oai_Record));;

	ExecClearTuple(slot);

	hasnext =  fetchNextOAIRecord(state,&entry);

	if(hasnext){

		elog(DEBUG1,"oai_fdw_IterateForeignScan: xml length > %ld",strlen(entry->content));
		elog(DEBUG1,"oai_fdw_IterateForeignScan: identifier > %s",entry->identifier);
		//elog(DEBUG1,"oai_fdw_IterateForeignScan: setSpec    > %s",entry->setspec);
		elog(DEBUG1,"oai_fdw_IterateForeignScan: datestamp  > %s",entry->datestamp);

//		appendTextArray(&entry->set_array,"zemeter5");




		slot->tts_isnull[0] = false;
		slot->tts_values[0] = CStringGetTextDatum(entry->identifier);

		slot->tts_isnull[1] = false;
		slot->tts_values[1] = CStringGetTextDatum(entry->content);

		slot->tts_isnull[2] = false;
		slot->tts_values[2] = DatumGetArrayTypeP(entry->set_array);
		//slot->tts_values[2] = DatumGetArrayTypeP(arr);
		//slot->tts_values[2] = CStringGetTextDatum(entry->setspec);

		slot->tts_isnull[3] = false;
		slot->tts_values[3] = CStringGetTextDatum(entry->datestamp);

		ExecStoreVirtualTuple(slot);

		state->current_row= state->current_row+1;

	}

	return slot;

}

int fetchNextOAIRecord(oai_fdw_state *state,oai_Record **entry){

	elog(DEBUG1,"fetchNextOAIRecord: current_row > %d",state->current_row);

	/* Loads documents in the "oai_fdw_state" in case it is the first pass or if it reached
	 * the end of the list and there is a resumption token for the next page. */
	if(state->list == NIL || (state->current_row == state->list->length && state->resumptionToken) ) {

		elog(DEBUG1,"fetchNextOAIRecord: RELOADING");
		loadOAIRecords(&state);
		state->current_row = 0;

	}


	if(state->current_row == state->list->length && !state->resumptionToken ) return 0;


	*entry = (oai_Record *) lfirst(list_nth_cell(state->list, state->current_row));

	elog(DEBUG1,"fetchNextOAIEntry: list length > %d",state->list->length);
	elog(DEBUG1,"fetchNextOAIEntry: xml length  > %ld",strlen((*entry)->content));
	elog(DEBUG1,"fetchNextOAIEntry: identifier  > %s",(*entry)->identifier);

	return 1; //TODO: return size content?

}


void appendTextArray(ArrayType **array, char* text_element) {

	size_t arr_nelems = 0;
	size_t arr_elems_size = 1;
	Datum *arr_elems = palloc(arr_elems_size*sizeof(Datum));

	Oid elem_type = TEXTOID;
	int16 elem_len;
	bool elem_byval;
	char elem_align;

	//elog(DEBUG1,"array elemen %d",ARR_SIZE((*array)));

	//if(!*array) {

	  // elog(DEBUG1,"array elemen before %d",ARR_SIZE(*array));
	get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

		if(*array == NULL) {

			arr_elems[arr_nelems] = CStringGetTextDatum(text_element);
			elog(DEBUG1,"array empty! adding value at arr_elems[%d]",arr_nelems);
			arr_nelems++;
		} else {

			ArrayIterator iterator = array_create_iterator(*array,0,NULL);
			elog(DEBUG1,"## current array size: %d",ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array)));

			arr_elems_size *= ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array))+1;
			arr_elems = repalloc(arr_elems, arr_elems_size*sizeof(Datum));

			bool isnull;
			Datum value;

			while (array_iterate(iterator, &value, &isnull)) {

				if (isnull) continue;



				elog(DEBUG1,"  adding element: arr_elems[%d]. arr_elems_size > %d",arr_nelems,arr_elems_size);

				arr_elems[arr_nelems] = value;
				arr_nelems++;

			}
			array_free_iterator(iterator);

			arr_elems[arr_nelems++] = CStringGetTextDatum(text_element);



			//arr_elems[0] = CStringGetTextDatum(text_element);
			//arr_elems[1] = CStringGetTextDatum(text_element);




			//arr_nelems=2;

//			get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);
//			arr_elems[0] = CStringGetTextDatum(text_element);
//			arr_elems[1] = CStringGetTextDatum(text_element);
//			arr_elems[2] = CStringGetTextDatum(text_element);

		}


		elog(DEBUG1,"construct_array called: arr_nelems > %d arr_elems_size %d",arr_nelems,arr_elems_size);

		*array = construct_array(arr_elems, arr_nelems, elem_type, elem_len, elem_byval, elem_align);
		//elog(DEBUG1,"ARR_DIMS %d",ArrayGetNItems(ARR_NDIM(*array), ARR_DIMS(*array)));

	//} else {



//
	//}




	//elog(DEBUG1,"dataoffset after > %d",(*array)->dataoffset);
}


void oai_fdw_ReScanForeignScan(ForeignScanState *node) {

	oai_fdw_state *state = node->fdw_state;
	state->current_row = 0;

}

void oai_fdw_EndForeignScan(ForeignScanState *node) {
}
