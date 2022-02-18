# Software still under construction!
---------------------------------------------
# PostgreSQL Foreign Data Wrapper for OAI-PMH (oai_fdw)


## Requirements

* [libxml2](http://www.xmlsoft.org/)
* [libcurl](https://curl.se/libcurl/)

## Install 

```sql
CREATE EXTENSION oai_fdw;
```

## How to use 

In order to access the `oai_fdw` features you first need to create a `SERVER` 

```sql
CREATE SERVER oai_server FOREIGN DATA WRAPPER oai_fdw;
```

The next step is to create a `FOREIGN TABLE` and map its columns to the supported `oai_attributes`. The attributes must be mapped using the `OPTION` clause of each column, as shown in the exaple bellow. The `oai_attribute` values expected are:

* `identifier`: the unique identifier of an item in a repository (OAI Header);
* `setspec`: the set membership of the item for the purpose of selective harvesting. (OAI Header)
* `datestamp`: the date of creation, modification or deletion of the record for the purpose of selective harvesting. (OAI Header)
* `content`: the XML document representing the retrieved recored (OAI Record)


We still need to tell the server where the OAI-PMH server is and which information we're looking for. These information can be attached to the table using the `SERVER` clause and, similar to the he column mapping, also has a `OPTION` clause. These options allow to limit harvest requests to portions of the metadata available from a repository. Expected options are:

* `url` OAI-PMH Werbserice URL. 
* `metadataPrefix` specifies the metadataPrefix of the format that should be included in the metadata part of the returned records. Records should be included only for items from which the metadata format matching the metadataPrefix can be disseminated. The metadata formats supported by a repository and for a particular item can be retrieved using the [ListMetadataFormats](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats) request. 
* `from` an *optional* argument with a UTCdatetime value, which specifies a lower bound for datestamp-based selective harvesting. 
* `until` an *optional* argument with a UTCdatetime value, which specifies a upper bound for datestamp-based selective harvesting. 
* `set` an *optional* argument with a setSpec value , which specifies set criteria for selective harvesting. 

####  Example:

```sql
CREATE FOREIGN TABLE oai_ulb_ulbmshs (
  id text                OPTIONS (oai_attribute 'identifier'), 
  xmldoc text            OPTIONS (oai_attribute 'content'), 
  sets text[]            OPTIONS (oai_attribute 'setspec'), 
  updatedate timestamp   OPTIONS (oai_attribute 'datestamp')) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           set 'ulbmshs', 
                           metadataPrefix 'oai_dc', 
                           from '2015-07-15', 
                           until '2015-07-15');
```


