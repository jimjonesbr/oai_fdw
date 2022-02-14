
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
#include "commands/explain.h"
#include <curl/curl.h>
#include <string.h> //TODO: REMNOVE
#include <libxml2/libxml/tree.h> //TODO: CORRECT PATH

#define LIBXML_OUTPUT_ENABLED
#define LIBXML_TREE_ENABLED

PG_MODULE_MAGIC;

Datum oai_fdw_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(oai_fdw_handler);

typedef struct oai_fdw_state {
	int current;
	int end;

	int rowcount;
	char *identifier;
	char *document;
	char *date;
	char *set;
	List *list;

	char*url;
	char*from;
	char*until;
	char *metadataPrefix;

} oai_fdw_state;

struct oai_entry {
	int rownumber;

	char *identifier;
	char *content;
	char *datestamp;
	char *setspec;

} oai_entry;


typedef struct oai_fdw_TableOptions {
	int start, end;
	char *metadataPrefix;
	char *from;
	char *until;
	char *url;
	char* set;
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
void oai_fdw_LoadOAIDocuments(oai_fdw_state ** state);

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

//void oai_fdw_LoadOAIDocuments(List **list, char *url, char *metadataPrefix, char *set, char *from, char * until) {
void oai_fdw_LoadOAIDocuments(oai_fdw_state **state) {


	char *postfields;
	int rowcount = 0;

	postfields = malloc(5000); //todo: calculate real size for malloc!

	sprintf(postfields,"verb=ListRecords&from=%s&until=%s&metadataPrefix=%s&set=%s",(*state)->from,(*state)->until,(*state)->metadataPrefix,(*state)->set);

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
			if (xmlStrcmp(list_records->name,"ListRecords")!=0) continue;

			xmlNodePtr record;

			for (record = list_records->children; record != NULL; record = record->next) {

				if (record->type != XML_ELEMENT_NODE) continue;
				if (xmlStrcmp(record->name, (xmlChar*) "record") != 0) continue;

				xmlBufferPtr buffer = xmlBufferCreate();

				int size = xmlNodeDump(buffer, xmldoc, record, 0, 1);

				elog(DEBUG1,"buffer size -> %d",size);
				//elog(DEBUG1,"buffer content -> %s",buffer->content);

				struct oai_entry *doc = palloc(sizeof(oai_entry));

				doc->content = palloc(strlen((char*)buffer->content));
				doc->content = (char*)buffer->content;
				//doc->identifier ="id42";
				doc->setspec ="speck";
				doc->datestamp ="2021-04-16";
				doc->rownumber = rowcount;

				xmlNodePtr header;

				for (header = record->children; header != NULL; header = header->next) {

					if (header->type != XML_ELEMENT_NODE) continue;
					if (xmlStrcmp(header->name, (xmlChar*) "header") != 0) continue;

					xmlNodePtr header_attributes;

					for (header_attributes = header->children; header_attributes!= NULL; header_attributes = header_attributes->next) {

						if (header_attributes->type != XML_ELEMENT_NODE) continue;

						if (xmlStrcmp(header_attributes->name, (xmlChar*) "identifier")==0){

							//elog(DEBUG1,"header_attributes %s",(char*) xmlNodeGetContent(header_attributes));
							doc->identifier = palloc(strlen((char*)xmlNodeGetContent(header_attributes)));
							doc->identifier = (char*) xmlNodeGetContent(header_attributes);

						}

						if (xmlStrcmp(header_attributes->name, (xmlChar*) "setSpec")==0){

							doc->setspec= palloc(strlen((char*)xmlNodeGetContent(header_attributes)));
							doc->setspec= (char*) xmlNodeGetContent(header_attributes);

						}


					}

				}

				if((*state)->list == NIL) {
					(*state)->list = list_make1(doc) ;
				} else {
					list_concat((*state)->list, list_make1(doc));
				}

				rowcount++;

				elog(DEBUG1,"list size -> %d",(*state)->list->length);

				xmlBufferDetach(buffer);
				xmlBufferFree(buffer);



			}

		}


		xmlFreeDoc(xmldoc);
		xmlCleanupParser();

	}

	curl_easy_cleanup(curl);

	(*state)->rowcount = rowcount;

}





void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	char *url;
	char *metadataPrefix;
	char *set;
	char *from;
	char *until;

	Relation rel = table_open(foreigntableid, NoLock);

	if (rel->rd_att->natts >1024) {
		ereport(ERROR,
				errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
				errmsg(">>>> incorrect schema for oai_fdw table %s: table must have exactly one column", NameStr(rel->rd_rel->relname)));
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

		if (strcmp("url", def->defname) == 0) {

			//char *val = defGetString(def);
			//sscanf(val, "%s", url);
			url = defGetString(def);

		} else if (strcmp("metadataprefix", def->defname) == 0) {

//			char *val = defGetString(def);
//			sscanf(val, "%s", metadataPrefix);
			metadataPrefix = defGetString(def);

		} else if (strcmp("set", def->defname) == 0) {

			set = defGetString(def);
			elog(DEBUG1,"FDW set = %s",set);
			//char *val = defGetString(def);
			//sscanf(val, "%s", set);

		} else if (strcmp("from", def->defname) == 0) {

//			char *val = defGetString(def);
//			sscanf(val, "%s", from);

			from = defGetString(def);

		} else if (strcmp("until", def->defname) == 0) {

//			char *val = defGetString(def);
//			sscanf(val, "%s", until);

			until = defGetString(def);

		} else {
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid option \"%s\"", def->defname),
							errhint("Valid table options for oai_fdw are \"url\",\"metadataPrefix\",\"setl\",\"from\" and \"until\"")));
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

	opts->from = from;
	opts->until = until;
	opts->url = url;
	opts->set = set;
	opts->metadataPrefix = metadataPrefix;

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

//	List *fdw_private = list_make1(makeString(opts->url));
//
//	list_concat(fdw_private, list_make1(makeString(opts->metadataPrefix)));
//	list_concat(fdw_private, list_make1(makeString(opts->set)));
//	list_concat(fdw_private, list_make1(makeString(opts->from)));
//	list_concat(fdw_private, list_make1(makeString(opts->until)));

	oai_fdw_state * state = palloc0(sizeof(oai_fdw_state));

	state->url = opts->url;
	state->set = opts->set;
	state->from = opts->from;
	state->until = opts->until;
	state->metadataPrefix = opts->metadataPrefix;

	elog(DEBUG1,"GetForeignPlan set = %s",opts->set);

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

	//oai_fdw_state *state = palloc0(sizeof(oai_fdw_state));
	ForeignScan *fs = (ForeignScan *) node->ss.ps.plan;
//	List *list = NIL;

	if(eflags & EXEC_FLAG_EXPLAIN_ONLY) return;

//	if(list == NIL) list = list_make1(makeString("<head>"));

//	list_concat(list, list_make1(makeString("<doc>1</doc>")));
//	list_concat(list, list_make1(makeString("<doc>2</doc>")));
//	list_concat(list, list_make1(makeString("<doc>3</doc>")));

	oai_fdw_state *state = (oai_fdw_state *) fs->fdw_private;


//	char *postfields;
//	char *url = "https://services.dnb.de/oai/repository";
//	char *metadata_prefix = "MARC21-xml";
//	char *from = "2020-04-01";
//	char *until = "2020-05-01";
//	char *set = "dnb-all:online:dissertations:sg300";

//	oai_fdw_LoadOAIDocuments(
//			&list,
//			strVal(lfirst(list_nth_cell(fs->fdw_private, 0))),  // url
//			strVal(lfirst(list_nth_cell(fs->fdw_private, 1))),  // metadataPrefix
//			strVal(lfirst(list_nth_cell(fs->fdw_private, 2))),  // set
//			strVal(lfirst(list_nth_cell(fs->fdw_private, 3))),  // from
//			strVal(lfirst(list_nth_cell(fs->fdw_private, 4)))); // until

//	oai_fdw_LoadOAIDocuments(
//			&list,
//			state->url,  // url
//			state->metadataPrefix,  // metadataPrefix
//			state->set,  // set
//			state->from,  // from
//			state->until); // until

	//oai_fdw_LoadOAIDocuments(&state);

//	state->list = list;
	//state->identifier = "ID000042";
	state->current = 0;
	//state->end = intVal(lsecond(fs->fdw_private));
	//state->set = "set";



	node->fdw_state = state;

}

//TODO: CRIAR FUNCAO GETNEXT PARA A LISTA CRIADA PELA FUNCAO LOADOAIDOCUMENTS
//      CRIAR TIPO OAIDOCUMENT COM CONTENT E HEADER. COLOCAR ESSE TIPO COMO TIPO DA LISTA EM STATE

int fetchNextOAIEntry(oai_fdw_state *state,struct oai_entry **entry){


	ListCell *cell;

	//elog(DEBUG1," nextOAIEntry list size > %d",state->list->length);
	elog(DEBUG1," nextOAIEntry current > %d",state->current);

	if(state->current < state->rowcount){

		//oai_entry * entry = palloc0(sizeof(oai_entry));
		*entry = (struct oai_entry *) lfirst(list_nth_cell(state->list, state->current));

		elog(DEBUG1,"fetchNextOAIEntry: xml length > %d",strlen((*entry)->content));
		elog(DEBUG1,"fetchNextOAIEntry: identifier > %s",(*entry)->identifier);

		return 0;
	}

	return 1;

}


TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node) {

	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	oai_fdw_state *state = node->fdw_state;
	struct oai_entry * entry;

	int hasnext = 1;

	if(state->current == 0) {

		oai_fdw_LoadOAIDocuments(&state);

	}

	ExecClearTuple(slot);

	hasnext =  fetchNextOAIEntry(state,&entry);


	if(hasnext == 0){

		char *xml = entry->content;
		//char* identifier = palloc(sizeof(char)*strlen(entry->identifier));
		//snprintf(identifier,strlen(entry->identifier)+1,"%s",entry->identifier);
		//identifier = entry->identifier;

		elog(DEBUG1,"oai_fdw_IterateForeignScan: xml length > %d",strlen(xml));
		elog(DEBUG1,"oai_fdw_IterateForeignScan: identifier > %s",entry->identifier);
		elog(DEBUG1,"oai_fdw_IterateForeignScan: setSpec    > %s",entry->setspec);

		slot->tts_isnull[0] = false;
		slot->tts_values[0] = Int32GetDatum(entry->rownumber);

		slot->tts_isnull[1] = false;
		slot->tts_values[1] = TextDatumGetCString(entry->identifier);
		//slot->tts_values[1] = CStringGetTextDatum("identifier");

		slot->tts_isnull[2] = false;
		//TextDatumGetCString
		slot->tts_values[2] = CStringGetTextDatum(entry->content);
		//slot->tts_values[2] = CStringGetTextDatum("xml");

		slot->tts_isnull[3] = false;
		slot->tts_values[3] = CStringGetTextDatum(entry->setspec);
		//slot->tts_values[3] = CStringGetTextDatum("set");


		ExecStoreVirtualTuple(slot);

		state->current= state->current+1;

	}



	return slot;


//	if (state->current < state->list->length) {
//
//		//TODO: parsear identifier e date pra retornar como tupla.
//		oai_entry *entry = (oai_entry *) list_nth_cell(state->list, state->current);
//
//		//char * xml = strVal(lfirst(list_nth_cell(state->list, state->current)));
//		char * xml = "xx";
//
//		slot->tts_isnull[0] = false;
//		slot->tts_values[0] = Int32GetDatum(state->current);
//
//		slot->tts_isnull[1] = false;
//		slot->tts_values[1] = CStringGetTextDatum("identifi");
//
//		slot->tts_isnull[2] = false;
//		slot->tts_values[2] = CStringGetTextDatum(xml);
//
//		slot->tts_isnull[3] = false;
//		slot->tts_values[3] = CStringGetTextDatum("xset");
//
//
//		ExecStoreVirtualTuple(slot);
//
//
//	}

}

void oai_fdw_ReScanForeignScan(ForeignScanState *node) {

	oai_fdw_state *state = node->fdw_state;
	state->current = 0;

}

void oai_fdw_EndForeignScan(ForeignScanState *node) {
}
