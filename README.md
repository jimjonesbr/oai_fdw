
---------------------------------------------
# PostgreSQL Foreign Data Wrapper for OAI-PMH (oai_fdw)

A PostgreSQL Foreign Data Wrapper to access OAI-PMH repositories (Open Archives Initiative Protocol for Metadata Harvesting). This wrapper supports the [OAI-PMH 2.0 Protocol](http://www.openarchives.org/OAI/openarchivesprotocol.html).

![CI](https://github.com/jimjonesbr/oai_fdw/actions/workflows/ci.yml/badge.svg)

**Note**: This software is still unstable and under constant development, and therefore still **NOT** production ready.

## Index

- [Requirements](#requirements)
- [Build & Install](#build-and-install)
- [Usage](#usage)
  - [CREATE SERVER](#create-server)
  - [IMPORT FOREIGN SCHEMA](#import-foreign-schema)
  - [CREATE FOREIGN TABLE](#create-foreign-table)
  - [Examples](#examples)
- [Support Functions](#support-functions)
  - [OAI_Identify](#oai_identify)
  - [OAI_ListMetadataFormats](#oai_listmetadataformats)
  - [OAI_ListSets](#oai_listsets)  
  - [OAI_Version](#oai_version)
   
## [Requirements](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#requirements)

* [libxml2](http://www.xmlsoft.org/): version 2.5.0 or higher.
* [libcurl](https://curl.se/libcurl/): version 7.74.0 or higher.
* [PostgreSQL](https://www.postgresql.org): version 11 or higher.

## [Build and Install](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#build_and_install)

To compile the source code you need to ensure the [pg_config](https://www.postgresql.org/docs/current/app-pgconfig.html) executable is properly set when you run `make` - this executable is typically in your PostgreSQL installation's bin directory. After that, just run `make` in the root directory:

```bash
$ cd oai_fdw
$ make
```

After compilation, just run `make install` to install the OAI-PMH Foreign Data Wrapper:

```bash
$ make install
```

After building and installing the extension you're ready to create the extension in a PostgreSQL database with `CREATE EXTENSION`:

```sql
CREATE EXTENSION oai_fdw;
```

To run the predefined regression tests run `make installcheck` with the user `postgres`:

```bash
$ make PGUSER=postgres installcheck
```

## Usage

To use the OAI-PMH Foreign Data Wrapper you must first create a `SERVER` to connect to an OAI-PMH repository. After that, you can either automatically generate foreign tables using `IMPORT FOREIGN SCHEMA` or create them manually using `CREATE FOREIGN TABLE`.

### [CREATE SERVER](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#create_server)

The SQL command [CREATE SERVER](https://www.postgresql.org/docs/current/sql-createserver.html) defines a new foreign server. The user who defines the server becomes its owner. An OAI Foreign Server requires an `url`, so that the Foreign Data Wrapper knows where to sent the http requests.

The following example creates a `SERVER` that connects to the OAI-PMH repository of the Münster University Library:

```sql
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai'); 
```

**Server Options**:

| Server Option | Requirement          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `url`  | mandatory        | URL address of the OAI-PMH repository.  

### [IMPORT FOREIGN SCHEMA](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#import-foreign-schema)

The [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html) command creates `FOREIGN TABLES` that represent [sets](http://www.openarchives.org/OAI/openarchivesprotocol.html#Set) existing on an OAI foreign server. The OAI Foreign Data Wrapper offers the following `FOREIGN SCHEMAS` to automatically gernate `FOREIGN TABLES`:

 | Foreign Schema | Description          |
|---------------|--------------------------|
| `oai_repository`  | Creates a single foreign table that can access all sets in the OAI repository.        
| `oai_sets`  | Creates a foreign table for each `set` in the OAI repository

OAI-PMH repositories publish data sets in many different customizable data formats, such as [MARC21/XML](https://www.loc.gov/standards/marcxml/) and [Dublin Core](http://www.openarchives.org/OAI/openarchivesprotocol.html#dublincore). So, in order to retrieve documents from OAI-PMH repositories it is necessary to tell which format is supposed to be returned from the server. The same goes for the OAI Foreign Data Wrapper, using the option `metadataprefix` in the `OPTION` clause as shown in the following examples. To see which formats are supported in an OAI repository, see [OAI_ListMetadataFormats](#oai_listmetadataformats).

**Foreign Schema Options**:

| Server Option | Requirement          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `metadataprefix`  | mandatory        | A string that specifies the metadata format in OAI-PMH requests issued to the repository.  

#### Examples

1. Import a single foreign table to access all available OAI Sets using the schema `oai_repository`

```sql

CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');

IMPORT FOREIGN SCHEMA oai_repository 
FROM SERVER oai_server_ulb 
INTO ulb_schema OPTIONS (metadataprefix 'oai_dc');

-- Table created
SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables;

 foreign_table_schema |    foreign_table_name     
----------------------+---------------------------
 ulb_schema           | oai_server_ulb_repository
(1 row)
```

The created foreign table is named with the server name and the suffix `_repository`. In this case `oai_server_ulb_repository`:

```
                          Foreign table "ulb_schema.oai_server_ulb_repository"
   Column   |            Type             | Collation | Nullable | Default |         FDW options         
------------+-----------------------------+-----------+----------+---------+-----------------------------
 id         | text                        |           |          |         | (oai_node 'identifier')
 xmldoc     | xml                         |           |          |         | (oai_node 'content')
 sets       | text[]                      |           |          |         | (oai_node 'setspec')
 updatedate | timestamp without time zone |           |          |         | (oai_node 'datestamp')
 format     | text                        |           |          |         | (oai_node 'metadataprefix')
 status     | boolean                     |           |          |         | (oai_node 'status')
Server: oai_server_ulb
FDW options: (metadataprefix 'oai_dc')
```
        
 2. Import a foreign table for each OAI Set using the schema `oai_sets`
  
```sql
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');   

IMPORT FOREIGN SCHEMA oai_sets 
FROM SERVER oai_server_ulb 
INTO ulb_schema OPTIONS (metadataprefix 'oai_dc');    

-- Tables created
SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables;

 foreign_table_schema | foreign_table_name 
----------------------+--------------------
 ulb_schema           | ulbmshbw
 ulb_schema           | ulbmshs
 ulb_schema           | ulbmssp
 ulb_schema           | ulbmsuo
 ulb_schema           | ulbmsz
 ulb_schema           | ulbmsum
 ulb_schema           | ulbmsob
 ulb_schema           | ulbmshd
 ulb_schema           | ulbmsh
 ulb_schema           | fremdbestand_hbz
(10 rows)

``` 

The created foreign tables are named with `set` name, for instance the set `ulbmshbw`:

```
                                   Foreign table "ulb_schema.ulbmshbw"
   Column   |            Type             | Collation | Nullable | Default |         FDW options         
------------+-----------------------------+-----------+----------+---------+-----------------------------
 id         | text                        |           |          |         | (oai_node 'identifier')
 xmldoc     | xml                         |           |          |         | (oai_node 'content')
 sets       | text[]                      |           |          |         | (oai_node 'setspec')
 updatedate | timestamp without time zone |           |          |         | (oai_node 'datestamp')
 format     | text                        |           |          |         | (oai_node 'metadataprefix')
 status     | boolean                     |           |          |         | (oai_node 'status')
Server: oai_server_ulb
FDW options: (metadataprefix 'oai_dc', setspec 'ulbmshbw')
```

 3. Import a foreign table for each OAI Set using the schema `oai_sets`, excluding specific sets using `EXCEPT`.
 
 ```sql
CREATE SCHEMA ulb_schema;

CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');   

IMPORT FOREIGN SCHEMA oai_sets EXCEPT (ulbmshbw,ulbmshs) 
FROM SERVER oai_server_ulb 
INTO ulb_schema OPTIONS (metadataprefix 'oai_dc');  

-- Tables created
SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables;

 foreign_table_schema | foreign_table_name 
----------------------+--------------------
 ulb_schema           | ulbmssp
 ulb_schema           | ulbmsuo
 ulb_schema           | ulbmsz
 ulb_schema           | ulbmsum
 ulb_schema           | ulbmsob
 ulb_schema           | ulbmshd
 ulb_schema           | ulbmsh
 ulb_schema           | fremdbestand_hbz
(8 rows)
 ```
 
 4. Import foreign tables for specific OAI Sets using `LIMIT TO`. 

```sql
CREATE SCHEMA ulb_schema;

CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');  

IMPORT FOREIGN SCHEMA oai_sets LIMIT TO (ulbmshbw,ulbmshs) 
FROM SERVER oai_server_ulb 
INTO ulb_schema OPTIONS (metadataprefix 'oai_dc');

-- Tables created      
SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables;

 foreign_table_schema | foreign_table_name 
----------------------+--------------------
 ulb_schema           | ulbmshbw
 ulb_schema           | ulbmshs
(2 rows)
```

### [CREATE FOREIGN TABLE](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#create_foreign_table)

Foreign Tables from the OAI Foreign Data Wrapper work as a proxy between PostgreSQL clients and OAI-PMH Repositories. Each `FOREIGN TABLE` column must be mapped to an `oai_node`, so that PostgreSQL knows where to display the OAI documents and header data. It is mandatory to set a `metadataprefix` to the `SERVER` clause of the `CREATE FOREIGN TABLE` statement, so that the OAI-PMH repository knows which XML format is supposed to be returned (see [OAI_ListMetadataFormats](#oai_listmetadataformats)). Optionally, it is possible to constraint a `FOREIGN TABLE` to specific OAI sets using the `setspec` option from the `SERVER` clause - omitting this option means that every SQL query will harvest *all sets* in the OAI repository.

The following example creates a `FOREIGN TABLE` connected to the `SERVER` created in the previous section. Queries executed against this table will harvest the set `dnb:reiheC` and will return the documents encoded as `oai_dc`. Each column is set with an `oai_node` in the `OPTION` clause:


```sql
CREATE FOREIGN TABLE dnb_maps_marc21 (
  id text              OPTIONS (oai_node 'identifier'),
  content text         OPTIONS (oai_node 'content'),
  setspec text[]       OPTIONS (oai_node 'setspec'),
  datestamp timestamp  OPTIONS (oai_node 'datestamp'),
  meta text            OPTIONS (oai_node 'metadataprefix')
)
  SERVER oai_server OPTIONS (setspec 'dnb:reiheC',
                             metadataprefix 'oai_dc');
```

**Column Options**:

| oai_node | PostgreSQL type          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `identifier`  | `text`, `varchar`        | The unique identifier of an item in a repository (OAI Header).                                                     |
| `setspec`     | `text[]`, `varchar[]`    | The set membership of the item for the purpose of selective harvesting. (OAI Header)                               |
| `datestamp`   | `timestamp`              | The date of creation, modification or deletion of the record for the purpose of selective harvesting. (OAI Header) |
| `content`     | `text`, `varchar`, `xml` | The XML document representing the retrieved recored (OAI Record)                                                   |
| `metadataprefix`     | `text`, `varchar` | A string that specifies the metadata format in OAI-PMH requests issued to the repository      |


**Server Options**

| Server Option | Requirement          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `metadataprefix`  | mandatory        | an argument that specifies the metadataPrefix of the format that should be included in the metadata part of the returned records. Records should be included only for items from which the metadata format matching the metadataPrefix can be disseminated. The metadata formats supported by a repository and for a particular item can be retrieved using the [ListMetadataFormats](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats) request.  
| `from`  | optional        | an argument with a UTCdatetime value, which specifies a lower bound for datestamp-based selective harvesting.  
| `until`  | optional        | an argument with a UTCdatetime value, which specifies a upper bound for datestamp-based selective harvesting.  
| `setspec`  | optional        | an argument with a setSpec value , which specifies set criteria for selective harvesting. 

#### [Examples](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#examples)

1. Create a `SERVER` and `FOREIGN TABLE` to harvest the [OAI-PMH repository](https://sammlungen.ulb.uni-muenster.de/oai) of the Münster University Library with records encoded as `oai_dc`:

```sql
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');

CREATE FOREIGN TABLE ulb_oai_dc (
  id text                OPTIONS (oai_node 'identifier'), 
  xmldoc xml             OPTIONS (oai_node 'content'), 
  sets text[]            OPTIONS (oai_node 'setspec'), 
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_ulb OPTIONS (metadataprefix 'oai_dc');
 
```

2. Create a `SERVER` and a `FOREIGN TABLE` to harvest the [OAI-PMH repository](https://sammlungen.ulb.uni-muenster.de/oai) of the Münster University Library with records encoded as `oai_dc` in the set `ulbmsuo`:

```sql
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');

CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (
  id text                OPTIONS (oai_node 'identifier'), 
  xmldoc xml             OPTIONS (oai_node 'content'), 
  sets text[]            OPTIONS (oai_node 'setspec'), 
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_ulb OPTIONS (metadataprefix 'oai_dc',
                                  setspec 'ulbmsuo');
                                  
SELECT * FROM ulb_ulbmsuo_oai_dc;

```

3. Create a `SERVER` and a `FOREIGN TABLE` to harvest the [OAI-PMH repository](https://services.dnb.de/oai/repository) of the German National Library with records encoded as `oai_dc` in the set `zdb` created between `2022-01-31` and `2022-02-01` (YYYY-MM-DD).


```sql
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository');

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text             OPTIONS (oai_node 'identifier'), 
  content text        OPTIONS (oai_node 'content'), 
  setspec text[]      OPTIONS (oai_node 'setspec'), 
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text           OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                                  metadataprefix 'oai_dc',
                                  from '2022-01-31', 
                                  until '2022-02-01');

SELECT * FROM dnb_zdb_oai_dc;

```

4. It is possible to set (or even overwrite) the pre-configured `SERVER OPTION` values by filtering the records in the SQL `WHERE` clause. The following example shows how to set the harvesting `metadataprefix`, `setspec`, and time interval (`from` and `until`) values in query time, overwriting the values defined in the `CREATE FOREIGN TABLE` statement:

```sql
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://services.dnb.de/oai/repository');

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text             OPTIONS (oai_node 'identifier'), 
  content text        OPTIONS (oai_node 'content'), 
  setspec text[]      OPTIONS (oai_node 'setspec'), 
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text           OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                                  metadataprefix 'oai_dc',
                                  from '2022-01-31', 
                                  until '2022-02-01');
						   
SELECT * FROM dnb_zdb_oai_dc 
WHERE 
  meta = 'MARC21-xml' AND
  datestamp BETWEEN '2022-03-01' AND '2022-03-02' AND
  setspec <@ ARRAY['dnb:reiheC'];
  
```

## Support Functions

These support functions help to retrieve additional information from an OAI Server to allow harvesters to limit harvest requests to portions of the metadata available from a repository.

### [OAI_Identify](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#oai_identify)

**Synopsis**

*SETOF OAI_IdentityNode* **OAI_Identify**(server_name *text*);

`server_name`: Name of a previously created OAI Foreign Data Wrapper Server

**Description**

This function is used to retrieve information about a repository. Some of the information returned is required as part of the OAI-PMH. Repositories may also employ the Identify verb to return additional descriptive information.

OAI Request: [Identify](http://www.openarchives.org/OAI/openarchivesprotocol.html#Identify)

**Usage**

```sql
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');   

SELECT * FROM OAI_Identify('oai_server_ulb');

       name        |                             description                              
-------------------+----------------------------------------------------------------------
 repositoryName    | Visual Library Server der Universitäts- und Landesbibliothek Münster
 baseURL           | http://sammlungen.ulb.uni-muenster.de/oai/
 protocolVersion   | 2.0
 adminEmail        | vl-support@semantics.de
 earliestDatestamp | 2011-02-22T15:12:26Z
 deletedRecord     | no
 granularity       | YYYY-MM-DDThh:mm:ssZ
(7 rows)
```

### [OAI_ListMetadataFormats](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#oai_listmetadataformats)

**Synopsis**

*SETOF OAI_MetadataFormat* **OAI_ListMetadataFormats**(server_name *text*);

`server_name`: Name of a previously created OAI Foreign Data Wrapper Server

**Description**

This function is used to retrieve the metadata formats available from a repository. An optional argument restricts the request to the formats available for a specific item.

OAI Request: [ListMetadataFormats](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats)

**Usage**

```sql
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');   

SELECT * FROM OAI_ListMetadataFormats('oai_server_ulb');

 metadataprefix |                               schema                               |              metadatanamespace              
----------------+--------------------------------------------------------------------+---------------------------------------------
 oai_dc         | http://www.openarchives.org/OAI/2.0/oai_dc.xsd                     | http://www.openarchives.org/OAI/2.0/oai_dc/
 mets           | http://www.loc.gov/standards/mets/mets.xsd                         | http://www.loc.gov/METS/
 mods           | http://www.loc.gov/standards/mods/v3/mods-3-0.xsd                  | http://www.loc.gov/mods/v3
 rawmods        | http://www.loc.gov/standards/mods/v3/mods-3-0.xsd                  | http://www.loc.gov/mods/v3
 epicur         | http://www.persistent-identifier.de/xepicur/version1.0/xepicur.xsd | urn:nbn:de:1111-2004033116
(5 rows)
```


### [OAI_ListSets](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#oai_listsets)

**Synopsis**

*SETOF OAI_Set* **OAI_ListSets**(server_name *text*);

`server_name`: Name of a previously created OAI Foreign Data Wrapper Server

**Description**

This function is used to retrieve the set structure of a repository, useful for [selective harvesting](http://www.openarchives.org/OAI/openarchivesprotocol.html#SelectiveHarvesting).

OAI Request: [ListSets](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListSets)

**Usage**

```sql
CREATE SERVER oai_server_ulb FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai');   

SELECT * FROM OAI_ListSets('oai_server_ulb');

     setspec      |                            setname                            
------------------+---------------------------------------------------------------
 ulbmshbw         | Historische Bestände in Westfalen (allegro)
 ulbmshs          | Schöpper (HANS)
 ulbmssp          | Schulprogramme (Verbundkatalog)
 ulbmsuo          | Historische Drucke mit URN (Verbundkatalog ohne Lokalbestand)
 ulbmsz           | ulbmsz
 ulbmsum          | Historische Drucke mit URN (Verbundkatalog mit Lokalbestand)
 ulbmsob          | Historische Drucke (Verbundkatalog ohne Lokalbestand)
 ulbmshd          | Historische Drucke (Verbundkatalog)
 ulbmsh           | Handschriften, Nachlässe, Autographen, Sammlungen (HANS)
 fremdbestand_hbz | Fremdbestand (Verbundkatalog)
(10 rows)
```

### [OAI_Version](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#oai_version)

**Synopsis**

*text* **OAI_Version**();

**Description**

Shows the version of the installed OAI FDW and its main libraries.

**Usage**

```sql
SELECT OAI_Version();
                                                                                      oai_version                                                                                    
  
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 oai fdw = 1.0.0dev, libxml = 2.9.10, libcurl = libcurl/7.74.0 NSS/3.61 zlib/1.2.11 brotli/1.0.9 libidn2/2.3.0 libpsl/0.21.0 (+libidn2/2.3.0) libssh2/1.9.0 nghttp2/1.43.0 librtmp/2.3
```
