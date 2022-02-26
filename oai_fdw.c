
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

	char* requestType;

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

	char* identifier;
	char* requestType;

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
void* deparseExpr(Expr *expr, oai_fdw_TableOptions *opts);
static char *datumToString(Datum datum, Oid type);

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

	appendStringInfo(&url_bufffer,"verb=%s",(*state)->requestType);

	//if(strcmp(request,OAI_REQUEST_LISTRECORDS)==0) {
	if(strcmp((*state)->requestType,OAI_REQUEST_LISTRECORDS)==0) {

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

//	} else if(strcmp(request,OAI_REQUEST_GETRECORD)==0) {
	} else if(strcmp((*state)->requestType,OAI_REQUEST_GETRECORD)==0) {

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

//void getRecordRequest(oai_fdw_state *state) {
//
//	struct string xmlStream;
//	int oaiExecuteResponse;
//
//	elog(DEBUG1,"GetRecordsRequest called");
//
//	oaiExecuteResponse = executeOAIRequest(&state,OAI_REQUEST_GETRECORD,&xmlStream);
//
//	if(oaiExecuteResponse == OAI_SUCCESS) {
//
//		xmlDocPtr xmldoc;
//		xmlNodePtr recordsList;
//
//		xmlInitParser();
//
//		xmldoc = xmlReadMemory(xmlStream.ptr, strlen(xmlStream.ptr), NULL, NULL, XML_PARSE_SAX1);
//
//
//		if (!xmldoc || (state->xmlroot_listrecords = xmlDocGetRootElement(xmldoc)) == NULL) {
//
//			xmlFreeDoc(xmldoc);
//			xmlCleanupParser();
//
//			ereport(ERROR, errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),	errmsg("Invalid XML response for URL \"%s\"",state->url));
//
//		}
//
//		for (recordsList = state->xmlroot_listrecords->children; recordsList != NULL; recordsList = recordsList->next) {
//
//			if (recordsList->type != XML_ELEMENT_NODE) continue;
//
//			if (xmlStrcmp(recordsList->name, (xmlChar*)"GetRecord")==0) {
//
//				if (!xmldoc || (state->xmlroot_listrecords = xmlDocGetRootElement(xmldoc)) == NULL) {
//
//					xmlFreeDoc(xmldoc);
//					xmlCleanupParser();
//
//				}
//
//			}
//
//		}
//
//	}
//
//
//}

void listRecordsRequest(oai_fdw_state *state) {

	struct string xmlStream;
	int oaiExecuteResponse;

	elog(DEBUG1,"ListRecordsRequest called");

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

			if (xmlStrcmp(recordsList->name, (xmlChar*)state->requestType)==0) {

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


char *getColumnOption(oai_fdw_TableOptions *opts, int16 attnum) {

	elog(DEBUG1,"####################");

	Relation rel = table_open(opts->foreigntableid, NoLock);



	for (int i = 0; i < rel->rd_att->natts ; i++) {

		List *options = GetForeignColumnOptions(opts->foreigntableid, i+1);
		ListCell *lc;

		if(rel->rd_att->attrs[i].attnum == attnum){

			foreach (lc, options) {

				DefElem *def = (DefElem*) lfirst(lc);

				elog(DEBUG1,"	----------------");

				if (strcmp(def->defname, OAI_OPT_ATTRIBUTE)==0) {

					char *option_value = defGetString(def);
					return option_value;

				}

			}


		}





	}

	table_close(rel, NoLock);
}


static char *datumToString(Datum datum, Oid type) {

	StringInfoData result;
	regproc typoutput;
	HeapTuple tuple;
	char *str, *p;

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
	Oid leftargtype, rightargtype, schema;

	char* leftargOption;

	elog(DEBUG1,"deparseExpr: expr->type %u",expr->type);

	switch (expr->type)	{

	case T_Var:

		elog(DEBUG1,"  deparseExpr: T_Var");
		varleft = (Var *)expr;
		//varattno = att num of col in table
		elog(DEBUG1,"  variable NUMBER %u", varleft->varattnosyn);

		elog(DEBUG1,"  result getColumn %s", getColumnOption(opts,varleft->varattnosyn));
		//TEXTOID=25

		leftargOption = getColumnOption(opts,varleft->varattnosyn);

		if (strcmp(leftargOption,OAI_ATTRIBUTE_IDENTIFIER) ==0 ) {

			elog(DEBUG1,"  deparseExpr: GetIdentifier Request > %s",leftargOption);

		}


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
		schema = ((Form_pg_operator)GETSTRUCT(tuple))->oprnamespace;

//		return opername;


		ReleaseSysCache(tuple);

		if (strcmp(opername, "=") == 0){

			elog(DEBUG1,"  deparseExpr: opername > %s",opername);


			varleft  = (Var *) linitial(oper->args);
			varright = (Var *) lsecond(oper->args);

			leftargOption = getColumnOption(opts,varleft->varattnosyn);


			if (strcmp(leftargOption,OAI_ATTRIBUTE_IDENTIFIER) ==0 && (varleft->vartype == TEXTOID || varleft->vartype == VARCHAROID)) {

				opts->requestType = OAI_REQUEST_GETRECORD;

				Const *constant = (Const*) lsecond(oper->args);

				opts->identifier = datumToString(constant->constvalue,constant->consttype);

				elog(DEBUG1,"  GetRecord: identifier: %s",opts->identifier);
				//elog(DEBUG1,"  GetIdentifier Request > %s",pstrdup(tuple->t_data));

			}

			//leftargOption = getColumnOption(opts,varright->varattnosyn);


			elog(DEBUG1,">>>>>>>>>> %u",((Expr *)lsecond(oper->args))->type);
			//111 = T_Const
//			deparseExpr(linitial(oper->args),opts);

		}


		//elog(DEBUG1,"  leftargtype  : %u",leftargtype);

			//elog(DEBUG1,"  rightargtype : %u",rightargtype);



		//}
		//elog(ERROR,"TEST: %s",opername);
		//mysql_deparse_op_expr((OpExpr *) node, context);
		break;

	default:
		elog(DEBUG1,"NONE");
		break;

	}

}

void deparseWhereClause(List *conditions, oai_fdw_TableOptions *opts){

//	StringInfo buf;
//	Form_pg_operator opform;
//
//
//	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));

//	List *conditions = baserel->baserestrictinfo;



	ListCell *cell;
	//OpExpr *oper;
	//HeapTuple tuple;
	char *opername, *left, *right, *arg, oprkind;

	char* operator;
	Oid leftargtype, rightargtype, schema;

	int i = 0;

	foreach(cell, conditions) {

		Expr  *expr = (Expr *) lfirst(cell);

		elog(DEBUG1,"round %d, expr->type %u",++i,expr->type);


		/* extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo)) {

			elog(DEBUG1,"  IsA RestrictInfo");

			RestrictInfo *ri = (RestrictInfo *) expr;
			expr = ri->clause;

		}

//		if (expr->type == T_OpExpr){
//
//			OpExpr *oper = (OpExpr *)expr;
//			HeapTuple tuple;
//
//			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper->opno));
//
//			operator = deparseExpr(expr,opts);
//
//		}

//		deparseExpr(expr,opts);

		deparseExpr(expr,opts);

	}


	//elog(ERROR,"LEAVING FOR - Operator: %s",operator);

}

void oai_fdw_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {

	//Relation rel = table_open(foreigntableid, NoLock);
	ForeignTable *ft = GetForeignTable(foreigntableid);
	oai_fdw_TableOptions *opts = (oai_fdw_TableOptions *)palloc0(sizeof(oai_fdw_TableOptions));
	List *conditions = baserel->baserestrictinfo;
	ListCell *cell;

	opts->foreigntableid = ft->relid;

	validateTableStructure(opts, ft);


	/* Default request type: OAI_REQUEST_LISTRECORDS
	 * It can be altered after depending on the WHERE clause*/
	opts->requestType = OAI_REQUEST_LISTRECORDS;

	deparseWhereClause(conditions,opts);



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
	state->requestType = opts->requestType;
	state->identifier = opts->identifier;
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

		if (xmlStrcmp(recordsList->name, (xmlChar*)state->requestType)!=0) continue;

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



















//
///*
// * Context for deparseExpr
// */
//typedef struct deparse_expr_cxt
//{
//	PlannerInfo *root;			/* global planner state */
//	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
//	RelOptInfo *scanrel;		/* the underlying scan relation. Same as
//								 * foreignrel, when that represents a join or
//								 * a base relation. */
//	StringInfo	buf;			/* output buffer to append to */
//	List	  **params_list;	/* exprs that will become remote Params */
//} deparse_expr_cxt;
//
//
//
// void
//deparseExpr(Expr *node, deparse_expr_cxt *context) {
//	if (node == NULL)
//		return;
//
//	switch (nodeTag(node))
//	{
//		case T_Var:
//			//mysql_deparse_var((Var *) node, context);
//			break;
//		case T_Const:
//			//mysql_deparse_const((Const *) node, context);
//			break;
//		case T_Param:
//			//mysql_deparse_param((Param *) node, context);
//			break;
//#if PG_VERSION_NUM < 120000
//		case T_ArrayRef:
//			//mysql_deparse_array_ref((ArrayRef *) node, context);
//#else
//		case T_SubscriptingRef:
//			//mysql_deparse_array_ref((SubscriptingRef *) node, context);
//#endif
//			break;
//		case T_FuncExpr:
//			//mysql_deparse_func_expr((FuncExpr *) node, context);
//			break;
//		case T_OpExpr:
//			mysql_deparse_op_expr((OpExpr *) node, context);
//			break;
//		case T_DistinctExpr:
//			//mysql_deparse_distinct_expr((DistinctExpr *) node, context);
//			break;
//		case T_ScalarArrayOpExpr:
//			//mysql_deparse_scalar_array_op_expr((ScalarArrayOpExpr *) node, context);
//			break;
//		case T_RelabelType:
//			//mysql_deparse_relabel_type((RelabelType *) node, context);
//			break;
//		case T_BoolExpr:
//			//mysql_deparse_bool_expr((BoolExpr *) node, context);
//			break;
//		case T_NullTest:
//			//mysql_deparse_null_test((NullTest *) node, context);
//			break;
//		case T_ArrayExpr:
//			//mysql_deparse_array_expr((ArrayExpr *) node, context);
//			break;
//		case T_Aggref:
//			//mysql_deparse_aggref((Aggref *) node, context);
//			break;
//		default:
//			elog(ERROR, "unsupported expression type for deparse: %d", (int) nodeTag(node));
//			break;
//	}
//}
//
//
//
// void
//mysql_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context)
//{
//	StringInfo	buf = context->buf;
//	HeapTuple	tuple;
//	Form_pg_operator form;
//	char		oprkind;
//	ListCell   *arg;
//
//	/* Retrieve information about the operator from system catalog. */
//	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
//	if (!HeapTupleIsValid(tuple))
//		elog(ERROR, "cache lookup failed for operator %u", node->opno);
//
//	form = (Form_pg_operator) GETSTRUCT(tuple);
//	oprkind = form->oprkind;
//
//	/* Sanity check. */
//	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
//		   (oprkind == 'l' && list_length(node->args) == 1) ||
//		   (oprkind == 'b' && list_length(node->args) == 2));
//
//	/* Always parenthesize the expression. */
//	appendStringInfoChar(buf, '(');
//
//	/* Deparse left operand. */
//	if (oprkind == 'r' || oprkind == 'b')
//	{
//		arg = list_head(node->args);
//		deparseExpr(lfirst(arg), context);
//		appendStringInfoChar(buf, ' ');
//	}
//
//	/* Deparse operator name. */
//	mysql_deparse_operator_name(buf, form);
//
//	/* Deparse right operand. */
//	if (oprkind == 'l' || oprkind == 'b')
//	{
//		arg = list_tail(node->args);
//		appendStringInfoChar(buf, ' ');
//		deparseExpr(lfirst(arg), context);
//	}
//
//	appendStringInfoChar(buf, ')');
//
//	ReleaseSysCache(tuple);
//}
//
// void
//mysql_deparse_operator_name(StringInfo buf, Form_pg_operator opform)
//{
//	/* opname is not a SQL identifier, so we should not quote it. */
//	char *cur_opname = NameStr(opform->oprname);
//
//	/* Print schema name only if it's not pg_catalog */
//	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
//	{
//		const char *opnspname;
//
//		opnspname = get_namespace_name(opform->oprnamespace);
//		/* Print fully qualified operator name. */
//		appendStringInfo(buf, "OPERATOR(%s.%s)",
//						 mysql_quote_identifier(opnspname, '`'), cur_opname);
//	}
//	else
//	{
//		if (strcmp(cur_opname, "~~") == 0)
//			appendStringInfoString(buf, "LIKE BINARY");
//		else if (strcmp(cur_opname, "~~*") == 0)
//			appendStringInfoString(buf, "LIKE");
//		else if (strcmp(cur_opname, "!~~") == 0)
//			appendStringInfoString(buf, "NOT LIKE BINARY");
//		else if (strcmp(cur_opname, "!~~*") == 0)
//			appendStringInfoString(buf, "NOT LIKE");
//		else if (strcmp(cur_opname, "~") == 0)
//			appendStringInfoString(buf, "REGEXP BINARY");
//		else if (strcmp(cur_opname, "~*") == 0)
//			appendStringInfoString(buf, "REGEXP");
//		else if (strcmp(cur_opname, "!~") == 0)
//			appendStringInfoString(buf, "NOT REGEXP BINARY");
//		else if (strcmp(cur_opname, "!~*") == 0)
//			appendStringInfoString(buf, "NOT REGEXP");
//		else
//			appendStringInfoString(buf, cur_opname);
//	}
//}
