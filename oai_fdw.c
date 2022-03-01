
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
#include "lib/stringinfo.h"

#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"
#include "utils/formatting.h"

#include "catalog/pg_operator.h"
#include "utils/syscache.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
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

#define OAI_XML_ROOT_ELEMENT "OAI-PMH"
#define OAI_ATTRIBUTE_IDENTIFIER "identifier"
#define OAI_ATTRIBUTE_CONTENT "content"
#define OAI_ATTRIBUTE_DATESTAMP "datestamp"
#define OAI_ATTRIBUTE_SETSPEC "setspec"
#define OAI_ATTRIBUTE_METADATAPREFIX "metadataPrefix"

#define OAI_SUCCESS 0
#define OAI_FAIL 1
#define OAI_UNKNOWN_REQUEST 2
#define ERROR_CODE_MISSING_PREFIX 1

#define OAI_FDW_OPTION "oai_attribute"

PG_MODULE_MAGIC;

Datum oai_fdw_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(oai_fdw_handler);

typedef struct oai_fdw_state {

	int end;


	int current_row;
	int numcols;
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
	ArrayType *setsArray;

} oai_Record;


typedef struct oai_fdw_TableOptions {

	Oid foreigntableid;
	List *columnlist;

	int numcols;
	char *metadataPrefix;
	char *from;
	char *until;
	char *url;
	char * set;
	char* identifier;
	char* requestType;

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

void init_string(struct string *s);
size_t writefunc(void *ptr, size_t size, size_t nmemb, struct string *s);
void appendTextArray(ArrayType **array, char* text_element) ;
int executeOAIRequest(oai_fdw_state **state, struct string *xmlResponse);
void listRecordsRequest(oai_fdw_state *state);
void createOAITuple(TupleTableSlot *slot, oai_fdw_state *state, oai_Record *oai );
oai_Record *fetchNextRecord(TupleTableSlot *slot, oai_fdw_state *state);
int loadOAIRecords(oai_fdw_state *state);
void* deparseExpr(Expr *expr, oai_fdw_TableOptions *opts);
static char *datumToString(Datum datum, Oid type);

char *getColumnOption(Oid foreigntableid, int16 attnum);
void deparseWhereClause(List *conditions, oai_fdw_TableOptions *opts);
void deparseSelectColumns(oai_fdw_TableOptions *opts);
void requestPlanner(oai_fdw_TableOptions *opts, ForeignTable *ft, RelOptInfo *baserel);

char *deparseTimestamp(Datum datum);

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

//		elog(ERROR,"res = %d",res);

		if (res != CURLE_OK) {

			curl_easy_cleanup(curl);

			ereport(ERROR,
					errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					errmsg("Connection error code %d: could not resolve \"%s\"",res,(*state)->url));

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

			ereport(ERROR, errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),	errmsg("Invalid XML response for URL \"%s\"",state->url));

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
				ereport(ERROR,
						errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("OAI %s: %s",errorCode,errorMessage));

			}

		}

	}

}

void requestPlanner(oai_fdw_TableOptions *opts, ForeignTable *ft, RelOptInfo *baserel) {

	List *conditions = baserel->baserestrictinfo;

	bool hasContentForeignColumn = false;
	Relation rel = table_open(ft->relid, NoLock);

	opts->numcols = rel->rd_att->natts;

	opts->foreigntableid = ft->relid;
	/* The default request type is OAI_REQUEST_LISTRECORDS.
	 * This can be altered after depending on the columns
	 * used in the WHERE and SELECT clauses*/
	opts->requestType = OAI_REQUEST_LISTRECORDS;




	for (int i = 0; i < rel->rd_att->natts ; i++) {

		List *options = GetForeignColumnOptions(ft->relid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			DefElem *def = (DefElem*) lfirst(lc);

			if (strcmp(def->defname, OAI_FDW_OPTION)==0) {

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

				//elog(DEBUG1,"requestPlanner: column %d option '%s'",i,option_value);

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

	opts->columnlist = baserel->reltarget->exprs;
	deparseSelectColumns(opts);

	table_close(rel, NoLock);

}


char *getColumnOption(Oid foreigntableid, int16 attnum) {

	Relation rel = table_open(foreigntableid, NoLock);
	List *options;
	char *option_value;

	for (int i = 0; i < rel->rd_att->natts ; i++) {

		options = GetForeignColumnOptions(foreigntableid, i+1);
		ListCell *lc;

		if(rel->rd_att->attrs[i].attnum == attnum){

			foreach (lc, options) {

				DefElem *def = (DefElem*) lfirst(lc);

				if (strcmp(def->defname, OAI_FDW_OPTION)==0) {

					option_value = defGetString(def);

					break;


				}

			}

		}

	}

	table_close(rel, NoLock);

	return option_value;

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



void* deparseExpr(Expr *expr, oai_fdw_TableOptions *opts){

	OpExpr *oper;
	Var *varleft;
	Var *varright;
	HeapTuple tuple;
	char *opername, *left, *right, *arg, oprkind;
	Oid leftargtype, rightargtype;

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
		oprkind = ((Form_pg_operator)GETSTRUCT(tuple))->oprkind;
		leftargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprleft;
		rightargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprright;
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

				elog(DEBUG1,"  deparseExpr: request Type set to '%s' with identifier '%s'",OAI_REQUEST_GETRECORD,opts->identifier);

			}

			//leftargOption = getColumnOption(opts,varright->varattnosyn);


			//elog(DEBUG1,">>>>>>>>>> %u",((Expr *)lsecond(oper->args))->type);
			//111 = T_Const
//			deparseExpr(linitial(oper->args),opts);

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


		break;

	default:
		elog(DEBUG1,"NONE");
		break;

	}

}


void deparseSelectColumns(oai_fdw_TableOptions *opts){

	ListCell *cell;
	Var *variable;
	bool hasContentForeignColumn = false;
	char *columnOption;

	elog(DEBUG1,"deparseSelectColumns called");

	foreach(cell, opts->columnlist) {

		//opts->numcols++;

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


char *deparseTimestamp(Datum datum) {

	struct pg_tm datetime_tm;
	int32 tzoffset;
	fsec_t datetime_fsec;
	StringInfoData s;

	/* get the parts */
	tzoffset = 0;
	(void)timestamp2tm(DatumGetTimestampTz(datum),
				false ? &tzoffset : NULL,
				&datetime_tm,
				&datetime_fsec,
				NULL,
				NULL);

	initStringInfo(&s);
	appendStringInfo(&s, "%04d-%02d-%02dT%02d:%02d:%02dZ",
		datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
		datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
		datetime_tm.tm_min, (int32)datetime_fsec);

	return s.data;

}


void deparseWhereClause(List *conditions, oai_fdw_TableOptions *opts){

	ListCell *cell;
	char *opername, *left, *right, *arg, oprkind;
	char* operator;

	Oid leftargtype, rightargtype, schema;

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
	oai_fdw_TableOptions *opts = (oai_fdw_TableOptions *)palloc0(sizeof(oai_fdw_TableOptions));
	ListCell *cell;

//	requestPlanner(opts, ft, baserel);

	elog(DEBUG1,"oai_fdw_GetForeignRelSize: requestType > %s", opts->requestType);

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

	oai->metadataPrefix = state->metadataPrefix;

	elog(DEBUG1,"fetchNextRecord for %s",state->requestType);


	bool getNext = false;

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

							elog(DEBUG1,"  fetchNextRecord: empty token in '%s' (EOF) %s",state->requestType);

						}
					}


					if (xmlStrcmp(header->name, (xmlChar*) "header") != 0) continue;


					for (headerList = header->children; headerList!= NULL; headerList = headerList->next) {

						char *node_content;

						if (headerList->type != XML_ELEMENT_NODE) continue;

						node_content = (char*)xmlNodeGetContent(headerList);

						if (xmlStrcmp(headerList->name, (xmlChar*) "identifier")==0)  oai->identifier = node_content;

						if (xmlStrcmp(headerList->name, (xmlChar*) "setSpec")==0) appendTextArray(&oai->setsArray,node_content);

						if (xmlStrcmp(headerList->name, (xmlChar*) "datestamp")==0)	oai->datestamp= node_content;

					}

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


				if(!rec->next && !state->resumptionToken) 	return NULL;

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

	/*

	for (int i = 0; i < state->numcols; i++) {

		ListCell *cell;
		int columnIndex = 0;
		List * options = GetForeignColumnOptions(state->foreigntableid, i+1);

		foreach (cell, options) {

			Expr * expr = (Expr *) lfirst(cell);

			if(expr->type == T_Var) {

				char* columnOption ;
				Var *variable = (Var *)expr;
				columnOption = getColumnOption(state->foreigntableid,variable->varattnosyn);

				if (strcmp(columnOption,OAI_ATTRIBUTE_IDENTIFIER)==0){

					elog(DEBUG2,"    createOAITuple: index '%d' value '%s'",columnIndex,oai->identifier);

					slot->tts_isnull[columnIndex] = false;
					slot->tts_values[columnIndex] = CStringGetTextDatum(oai->identifier);


				} else if (strcmp(columnOption,OAI_ATTRIBUTE_METADATAPREFIX)==0){

					elog(DEBUG2,"    createOAITuple: index '%d' value '%s'",columnIndex,oai->metadataPrefix);

					slot->tts_isnull[columnIndex] = false;
					slot->tts_values[columnIndex] = CStringGetTextDatum(oai->metadataPrefix);


				} else if (strcmp(columnOption,OAI_ATTRIBUTE_CONTENT)==0){

					elog(DEBUG2,"    createOAITuple: index '%d' value '%s'",columnIndex,oai->content);

					slot->tts_isnull[columnIndex] = false;
					slot->tts_values[columnIndex] = CStringGetTextDatum((char*)oai->content);

				} else if (strcmp(columnOption,OAI_ATTRIBUTE_SETSPEC)==0){

					elog(DEBUG2,"    createOAITuple: index '%d' value '%u'",columnIndex,oai->identifier);

					slot->tts_isnull[columnIndex] = false;
					slot->tts_values[columnIndex] = DatumGetArrayTypeP(oai->setsArray);


				} else if (strcmp(columnOption,OAI_ATTRIBUTE_DATESTAMP)==0){

					elog(DEBUG2,"    createOAITuple: index '%d' value '%s'",columnIndex,oai->datestamp);

					char workBuffer[MAXDATELEN + 1];
					char *fieldArray[MAXDATEFIELDS];
					int fieldTypeArray[MAXDATEFIELDS];
					int fieldCount = 0;
					int parseError = ParseDateTime(oai->datestamp, workBuffer, sizeof(workBuffer), fieldArray, fieldTypeArray, MAXDATEFIELDS, &fieldCount);

					//elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);


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

							slot->tts_isnull[columnIndex] = false;
							slot->tts_values[columnIndex] = DatumGetTimestamp(tmsp);

							elog(DEBUG2,"    createOAITuple: datestamp (\"%s\") parsed and decoded.",oai->datestamp);


						} else {


							slot->tts_isnull[columnIndex] = true;
							slot->tts_values[columnIndex] = NULL;
							elog(WARNING,"    createOAITuple: could not decode datestamp: %s", oai->datestamp);

						}


					} else {


						slot->tts_isnull[columnIndex] = true;
						slot->tts_values[columnIndex] = NULL;

						elog(WARNING,"    createOAITuple: could not parse datestamp: %s", oai->datestamp);


					}

				}

			}

			columnIndex++;
		}

	}

	*/

	for (int i = 0; i < state->numcols; i++) {

		List * options = GetForeignColumnOptions(state->foreigntableid, i+1);
		ListCell *lc;

		foreach (lc, options) {

			DefElem *def = (DefElem*) lfirst(lc);

			if (strcmp(def->defname, OAI_FDW_OPTION)==0) {

				char * option_value = defGetString(def);

				if (strcmp(option_value,OAI_ATTRIBUTE_IDENTIFIER)==0 && oai->identifier){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->identifier);


				} else if (strcmp(option_value,OAI_ATTRIBUTE_METADATAPREFIX)==0 && oai->metadataPrefix){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum(oai->metadataPrefix);


				} else if (strcmp(option_value,OAI_ATTRIBUTE_CONTENT)==0 && oai->content){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);
					//elog(DEBUG1,"  createOAITuple: StringInfo data > '%s'",oai_record->xmlContent->data);
					//elog(DEBUG1,"    createOAITuple: content size > '%d'",strlen(oai_record->content));
					slot->tts_isnull[i] = false;
					slot->tts_values[i] = CStringGetTextDatum((char*)oai->content);
					//slot->tts_values[i] = CStringGetTextDatum(pstrdup(oai_record->xmlContent.data));
					//elog(DEBUG1,"    createOAITuple: content added to tuple > '%s'",oai_record->xmlContent.data);
					//elog(DEBUG1,"    createOAITuple: content added to tuple > '%d'",strlen(oai_record->xmlContent.data));

				} else if (strcmp(option_value,OAI_ATTRIBUTE_SETSPEC)==0 && oai->setsArray){

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);
					//elog(DEBUG1,"  createOAITuple: array ndim %d",oai_record->setsArray->ndim);

					slot->tts_isnull[i] = false;
					slot->tts_values[i] = DatumGetArrayTypeP(oai->setsArray);
					elog(DEBUG2,"    createOAITuple: setspec added to tuple");

				}  else if (strcmp(option_value,OAI_ATTRIBUTE_DATESTAMP)==0 && oai->datestamp){

					char workBuffer[MAXDATELEN + 1];
					char *fieldArray[MAXDATEFIELDS];
					int fieldTypeArray[MAXDATEFIELDS];
					int fieldCount = 0;
					int parseError = ParseDateTime(oai->datestamp, workBuffer, sizeof(workBuffer), fieldArray, fieldTypeArray, MAXDATEFIELDS, &fieldCount);

					elog(DEBUG2,"  createOAITuple: column %d option '%s'",i,option_value);


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
	elog(DEBUG1,"  createOAITuple finished (rowcount: %d)",state->rowcount);

}

TupleTableSlot *oai_fdw_IterateForeignScan(ForeignScanState *node) {

	int hasnext;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	oai_fdw_state *state = node->fdw_state;
	oai_Record *oai = (oai_Record *)palloc0(sizeof(oai_Record));


	elog(DEBUG1,"oai_fdw_IterateForeignScan called");

	ExecClearTuple(slot);

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



