
---------------------------------------------
# PostgreSQL Foreign Data Wrapper for OAI-PMH (oai_fdw)

A PostgreSQL Foreign Data Wrapper to access OAI-PMH repositories (Open Archives Initiative Protocol for Metadata Harvesting). This wrapper supports the [OAI-PMH 2.0 Protocol](http://www.openarchives.org/OAI/openarchivesprotocol.html).

![CI](https://github.com/jimjonesbr/oai_fdw/actions/workflows/ci.yml/badge.svg)

## Index
  - [Requirements](#requirements)
  - [Build and Install](#build-and-install)
  - [Update](#update)
  - [Usage](#usage)
    - [CREATE SERVER](#create-server)
    - [CREATE USER MAPPING](#create-user-mapping)
    - [IMPORT FOREIGN SCHEMA](#import-foreign-schema)
      - [IMPORT FOREIGN SCHEMA Examples](#import-foreign-schema-examples)
    - [CREATE FOREIGN TABLE](#create-foreign-table)
      - [Examples](#examples)
  - [Support Functions](#support-functions)
    - [OAI\_Identify](#oai_identify)
    - [OAI\_ListMetadataFormats](#oai_listmetadataformats)
    - [OAI\_ListSets](#oai_listsets)
    - [OAI\_Version](#oai_version)
    - [OAI\_HarvestTable](#oai_harvesttable)
  - [Deploy with Docker](#deploy-with-docker)
  - [Error Handling](#error-handling)
  - [Limitations](#limitations)
   
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

To install an specific version add the full version number in the `WITH VERSION` clause

```sql
CREATE EXTENSION oai_fdw WITH VERSION '1.11';
```

To run the predefined regression tests run `make installcheck` with the user `postgres`:

```bash
$ make PGUSER=postgres installcheck
```

## [Update](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#update)

To update the oai_fdw's version you must first build and install the binaries and then run `ALTER EXTENSION`:


```sql
ALTER EXTENSION oai_fdw UPDATE;
```

To update to an specific version use `UPDATE TO` and the full version number

```sql
ALTER EXTENSION oai_fdw UPDATE TO '1.11';
```

## [Usage](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#usage)

To use the OAI Foreign Data Wrapper you must first create a `SERVER` to connect to an OAI-PMH repository. After that, you can either automatically generate foreign tables using `IMPORT FOREIGN SCHEMA` or create them manually using `CREATE FOREIGN TABLE`.

### [CREATE SERVER](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#create_server)

The SQL command [CREATE SERVER](https://www.postgresql.org/docs/current/sql-createserver.html) defines a new foreign server. The user who defines the server becomes its owner. An OAI Foreign repository requires an `url`, so that the Foreign Data Wrapper knows where to sent the http requests.

The following example creates a `SERVER` that connects to the OAI-PMH repository of the Münster University Library:

```sql
CREATE SERVER oai_server_ulb 
FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai'); 
```

**Server Options**:

| Server Option | Type          | Description                                                                                                        |
|---------------|----------------------|--------------------------------------------------------------------------------------------------------------------|
| `url`         | **required**            | URL address of the OAI-PMH repository.
| `http_proxy` | optional            | Proxy for HTTP requests.
| `proxy_user` | optional            | User for proxy server authentication.
| `proxy_user_password` | optional            | Password for proxy server authentication.
| `connect_timeout`         | optional            | Connection timeout for HTTP requests in seconds (default 300 seconds).
| `connect_retry`         | optional            | Number of attempts to retry a request in case of failure (default 3 times).
| `request_redirect`         | optional            | Enables URL redirect issued by the server (default 'false').
| `request_max_redirect`         | optional            | Limit of how many times the URL redirection may occur. If that many redirections have been followed, the next redirect will cause an error. Not setting this parameter or setting it to `0` will allow an infinite number of redirects.

### [CREATE USER MAPPING](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#create-user-mapping)

Availability: **1.9**

[CREATE USER MAPPING](https://www.postgresql.org/docs/current/sql-createusermapping.html) defines a mapping of a PostgreSQL user to an user in the target OAI repository. For instance, to map the PostgreSQL user `postgres` to the user `admin` in the `SERVER` named `my_protected_oai`:

```sql
CREATE SERVER my_protected_oai
FOREIGN DATA WRAPPER oai_fdw 
OPTIONS (url 'https://my.proteceted.oai.de/oai'); 

CREATE USER MAPPING FOR postgres
SERVER my_protected_oai OPTIONS (user 'admin', password 'secret');
```

| Option | Type | Description |
|---|---|---|
| `user` | **required** | name of the user for authentication |
| `password` | optional |   password of the user set in the option `user` |

The `oai_fdw` will try to authenticate the user using HTTP Basic Authentication - no other authentication method is currently supported. This feature can be ignored if the OAI repository does not require user authentication.

### [IMPORT FOREIGN SCHEMA](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#import-foreign-schema)

The [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html) command creates `FOREIGN TABLES` that represent [sets](http://www.openarchives.org/OAI/openarchivesprotocol.html#Set) existing on an OAI [SERVER](#create_server). The OAI Foreign Data Wrapper offers the following `FOREIGN SCHEMAS` to automatically generate `FOREIGN TABLES`:

 | Foreign Schema | Description          |
|---------------|--------------------------|
| `oai_repository`  | Creates a single foreign table that can access all sets in the OAI repository.        
| `oai_sets`  | Creates a foreign table for each `set` in the OAI repository

OAI-PMH repositories publish data sets in many different customizable data formats, such as [MARC21/XML](https://www.loc.gov/standards/marcxml/) and [Dublin Core](http://www.openarchives.org/OAI/openarchivesprotocol.html#dublincore). So, in order to retrieve documents from OAI-PMH repositories it is necessary to tell which format is supposed to be returned from the server. The same goes for the OAI Foreign Data Wrapper, using the option `metadataprefix` in the `OPTION` clause as shown in the following examples. To see which formats are supported in an OAI repository, see [OAI_ListMetadataFormats](#oai_listmetadataformats).

**Foreign Schema Options**:

| Server Option | Type          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `metadataprefix`  | **required**        | A string that specifies the metadata format in OAI-PMH requests issued to the repository.  

#### IMPORT FOREIGN SCHEMA Examples

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

The following example creates a `FOREIGN TABLE` connected to the server `oai_server_dnb`. Queries executed against this table will harvest the set `dnb:reiheC` and will return the documents encoded as `oai_dc`. Each column is set with an `oai_node` in the `OPTION` clause:


```sql
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository');

CREATE FOREIGN TABLE dnb_maps (
  id text              OPTIONS (oai_node 'identifier'),
  content text         OPTIONS (oai_node 'content'),
  setspec text[]       OPTIONS (oai_node 'setspec'),
  datestamp timestamp  OPTIONS (oai_node 'datestamp'),
  meta text            OPTIONS (oai_node 'metadataprefix')
)
  SERVER oai_server_dnb OPTIONS (setspec 'dnb:reiheC',
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

| Server Option | Type          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `metadataprefix`  | **required**        | an argument that specifies the metadataPrefix of the format that should be included in the metadata part of the returned records. Records should be included only for items from which the metadata format matching the metadataPrefix can be disseminated. The metadata formats supported by a repository and for a particular item can be retrieved using the [ListMetadataFormats](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats) request.  
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

 SELECT * FROM ulb_oai_dc;
 
                   id                    |                                                           xmldoc                                                            |       sets        |     updatedate      | format 
-----------------------------------------+-----------------------------------------------------------------------------------------------------------------------------+-------------------+---------------------+--------
 oai:digital.ulb.uni-muenster.de:4849615 | <oai_dc:dc xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">+| {ulbmshd,article} | 2017-11-15 12:39:37 | oai_dc
                                         |   <dc:title>Karaktertrekken.</dc:title>                                                                                    +|                   |                     | 
                                         |   <dc:creator>Voght, P.F. de</dc:creator>                                                                                  +|                   |                     | 
                                         |   <dc:publisher>Univ.- und Landesbibliothek</dc:publisher>                                                                 +|                   |                     | 
                                         |   <dc:date>2017</dc:date>                                                                                                  +|                   |                     | 
                                         |   <dc:type>Text</dc:type>                                                                                                  +|                   |                     | 
                                         |   <dc:type>Article</dc:type>                                                                                               +|                   |                     | 
                                         |   <dc:format>3 Seiten</dc:format>                                                                                          +|                   |                     | 
                                         |   <dc:relation>urn:nbn:de:hbz:6:1-375650</dc:relation>                                                                     +|                   |                     | 
                                         |   <dc:rights>pdm</dc:rights>                                                                                               +|                   |                     | 
                                         | </oai_dc:dc>                                                                                                                |                   |                     | 

(...) 
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

                   id                    |                                                                        xmldoc                                                                         |      sets      |     updatedate      | format 
-----------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------+----------------+---------------------+--------
 oai:digital.ulb.uni-muenster.de:1188819 | <oai_dc:dc xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">                          +| {ulbmsuo,book} | 2012-08-08 08:03:33 | oai_dc
                                         |   <dc:title>Die Lütticher Revolution im Jahr 1789 und das Benehmen Sr. Königl. Majestät von Preussen bey derselben</dc:title>                        +|                |                     | 
                                         |   <dc:creator>Dohm, Christian Conrad Wilhelm von</dc:creator>                                                                                        +|                |                     | 
                                         |   <dc:subject>Intervention</dc:subject>                                                                                                              +|                |                     | 
                                         |   <dc:subject>Lüttich / Revolution &lt;1789&gt;</dc:subject>                                                                                         +|                |                     | 
                                         |   <dc:subject>Friedrich Wilhelm &lt;II., Preußen, König&gt;</dc:subject>                                                                             +|                |                     | 
                                         |   <dc:subject>Heiliges Römisches Reich / Reichskammergericht</dc:subject>                                                                            +|                |                     | 
                                         |   <dc:description>dargestellt von ... Ihrem Clevischen Geheimen Kreiß-Directorialrath ... Christian Wilhelm von Dohm</dc:description>                +|                |                     | 
                                         |   <dc:description>Vorlageform des Erscheinungsvermerks: gedruckt bey George Jacob Decker und Sohn, Königl. Geh. Ober-Hofbuchdruckern</dc:description>+|                |                     | 
                                         |   <dc:publisher>Decker</dc:publisher>                                                                                                                +|                |                     | 
                                         |   <dc:publisher>Univ.- und Landesbibliothek</dc:publisher>                                                                                           +|                |                     | 
                                         |   <dc:date>1790</dc:date>                                                                                                                            +|                |                     | 
                                         |   <dc:type>Text</dc:type>                                                                                                                            +|                |                     | 
                                         |   <dc:type>Book</dc:type>                                                                                                                            +|                |                     | 
                                         |   <dc:format>186 S., [1] Bl. ; 8</dc:format>                                                                                                         +|                |                     | 
                                         |   <dc:identifier>eki:HBZHT000092708</dc:identifier>                                                                                                  +|                |                     | 
                                         |   <dc:identifier>hbz-idn:CT005003896</dc:identifier>                                                                                                 +|                |                     | 
                                         |   <dc:identifier>urn:nbn:de:hbz:6-85659547872</dc:identifier>                                                                                        +|                |                     | 
                                         |   <dc:identifier>https://nbn-resolving.org/urn:nbn:de:hbz:6-85659547872</dc:identifier>                                                              +|                |                     | 
                                         |   <dc:identifier>system:HT000092708</dc:identifier>                                                                                                  +|                |                     | 
                                         |   <dc:relation>vignette : http://sammlungen.ulb.uni-muenster.de/titlepage/urn/urn:nbn:de:hbz:6-85659547872/128</dc:relation>                         +|                |                     | 
                                         |   <dc:language>ger</dc:language>                                                                                                                     +|                |                     | 
                                         |   <dc:coverage>Fürstentum Lüttich</dc:coverage>                                                                                                      +|                |                     | 
                                         |   <dc:coverage>Geschichte 1789</dc:coverage>                                                                                                         +|                |                     | 
                                         |   <dc:coverage>Berlin</dc:coverage>                                                                                                                  +|                |                     | 
                                         |   <dc:rights>reserved</dc:rights>                                                                                                                    +|                |                     | 
                                         | </oai_dc:dc>                                                                                                                                          |                |                     | 

(...)

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

            id             |                                                                      content                                                                       | setspec |      datestamp      |  meta  
---------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+---------+---------------------+--------
 oai:dnb.de/zdb/1250800153 | <dc xmlns:dnb="http://d-nb.de/standards/dnbterms" xmlns="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">+| {zdb}   | 2022-02-01 15:39:05 | oai_dc
                           |   <dc:title>Jahrbuch Deutsch als Fremdsprache</dc:title>                                                                                          +|         |                     | 
                           |   <dc:publisher>München : Iudicium Verlag</dc:publisher>                                                                                          +|         |                     | 
                           |   <dc:date>2022</dc:date>                                                                                                                         +|         |                     | 
                           |   <dc:language>ger</dc:language>                                                                                                                  +|         |                     | 
                           |   <dc:identifier xsi:type="dnb:IDN">1250800153</dc:identifier>                                                                                    +|         |                     | 
                           |   <dc:identifier xsi:type="dnb:ZDBID">3108310-9</dc:identifier>                                                                                   +|         |                     | 
                           |   <dc:type>Online-Ressource</dc:type>                                                                                                             +|         |                     | 
                           |   <dc:relation>http://d-nb.info/011071060</dc:relation>                                                                                           +|         |                     | 
                           | </dc>                                                                                                                                              |         |                     | 

(...)

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
  
                id                |                                                                        content                                                                         |   setspec    |      datestamp      |    meta    
----------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+---------------------+------------
 oai:dnb.de/dnb:reiheC/1131642694 | <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">                                                                                  +| {dnb:reiheC} | 2022-03-01 13:54:09 | MARC21-xml
                                  |     <leader>00000pem a2200000 c 4500</leader>                                                                                                         +|              |                     | 
                                  |     <controlfield tag="001">1131642694</controlfield>                                                                                                 +|              |                     | 
                                  |     <controlfield tag="003">DE-101</controlfield>                                                                                                     +|              |                     | 
                                  |     <controlfield tag="005">20220301145409.0</controlfield>                                                                                           +|              |                     | 
                                  |     <controlfield tag="007">a|||||||</controlfield>                                                                                                   +|              |                     | 
                                  |     <controlfield tag="008">170509s2017    au |||||||a||   ||||ger  </controlfield>                                                                   +|              |                     | 
                                  |     <datafield tag="015" ind1=" " ind2=" ">                                                                                                           +|              |                     | 
                                  |       <subfield code="a">17,C04</subfield>                                                                                                            +|              |                     | 
                                  |       <subfield code="z">17,N20</subfield>                                                                                                            +|              |                     | 
                                  |       <subfield code="2">dnb</subfield>                                                                                                               +|              |                     | 
                                  |     </datafield>                                                                                                                                      +|              |                     | 
                                  |     <datafield tag="016" ind1="7" ind2=" ">                                                                                                           +|              |                     | 
                                  |       <subfield code="2">DE-101</subfield>                                                                                                            +|              |                     | 
                                  |       <subfield code="a">1131642694</subfield>                                                                                                        +|              |                     | 
                                  |     </datafield>                                                                                                                                      +|              |                     | 
                                  |     <datafield tag="020" ind1=" " ind2=" ">                                                                                                           +|              |                     | 
                                  |       <subfield code="a">9783990442807</subfield>                                                                                                     +|              |                     | 
                                  |       <subfield code="c">: EUR 11.99 (DE), EUR 11.99 (AT), CHF 18.50 (freier Preis)</subfield>                                                        +|              |                     | 
                                  |       <subfield code="9">978-3-99044-280-7</subfield>                                                                                                 +|              |                     | 
                                  |     </datafield>                                                                                                                                      +|              |                     | 
                                  |     <datafield tag="020" ind1=" " ind2=" ">                                                                                                           +|              |                     | 
                                  |       <subfield code="a">3990442805</subfield>                                                                                                        +|              |                     | 
                                  |       <subfield code="9">3-99044-280-5</subfield>                                                                                                     +|              |                     | 
                                  |     </datafield>                                                                                                                                      +|              |                     | 
                                  |     <datafield tag="024" ind1="3" ind2=" ">                                                                                                           +|              |                     | 
                                  |       <subfield code="a">9783990442807</subfield>                                                                                                     +|              |                     | 
                                  |     </datafield>                                                                                                                                      +|              |                     | 
                                  |     <datafield tag="034" ind1="0" ind2=" ">                                                                                                           +|              |                     | 
                                  |       <subfield code="a">a</subfield>                                                                                                                 +|              |                     | 
                                  |       <subfield code="d">E 010 08 09</subfield>                                                                                                       +|              |                     | 
                                  |       <subfield code="e">E 010 49 02</subfield>                                                                                                       +|              |                     | 
                                  |       <subfield code="f">N 047 25 32</subfield>                                                                                                       +|              |                     | 
                                  |       <subfield code="g">N 047 06 15</subfield>                                                                                                       +|              |                     | 
                                  |       <subfield code="9">A:acx</subfield>                                                                                                             +|              |                     | 
                                  |     </datafield>                                                                                                                                      +|              |                     | 
                                  |     <datafield tag="034" ind1="0" ind2=" ">                                                                                                           +|              |                     | 
                                  |       <subfield code="a">a</subfield>                                                                                                                 +|              |                     | 
                                  |       <subfield code="d">E010.135833</subfield>                                                                                                       +|              |                     | 
                                  |       <subfield code="e">E010.817222</subfield>                                                                                                       +|              |                     | 
                                  |       <subfield code="f">N047.425555</subfield>                                                                                                       +|              |                     | 
                                  |       <subfield code="g">N047.104166</subfield>                                                                                                       +|              |                     | 
                                  |       <subfield code="9">A:dcx</subfield>                                                                                                             +|              |                     | 
                                  |     </datafield>                                                                                                       
(...)
                                  |     <datafield tag="926" ind1="2" ind2=" ">                                                                                                           +|              |                     | 
                                  |       <subfield code="a">1DZ</subfield>                                                                                                               +|              |                     | 
                                  |       <subfield code="o">94</subfield>                                                                                                                +|              |                     | 
                                  |       <subfield code="q">Publisher</subfield>                                                                                                         +|              |                     | 
                                  |       <subfield code="v">1.2</subfield>                                                                                                               +|              |                     | 
                                  |       <subfield code="x">Europa, physisch</subfield>                                                                                                  +|              |                     | 
                                  |     </datafield>                                                                                                                                      +|              |                     | 
                                  |   </record>  
  
```

5. Create a `SERVER` and a `FOREIGN TABLE` to harvest the [OAI-PMH repository](https://services.dnb.de/oai/repository) of the German National Library with records encoded as `oai_dc` in the set `zdb` created between `2022-01-31` and `2022-02-01` (YYYY-MM-DD), and parse `title` and `date` from the XML document using XPATH.

```
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository');

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text             OPTIONS (oai_node 'identifier'), 
  content xml        OPTIONS (oai_node 'content'), 
  setspec text[]      OPTIONS (oai_node 'setspec'), 
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text           OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_dnb OPTIONS (setspec 'zdb', 
                                  metadataprefix 'oai_dc',
                                  from '2022-01-31', 
                                  until '2022-02-01');
                                  

SELECT  
  (xpath('//dc:title/text()',content::xml,ARRAY[ARRAY['dc','http://purl.org/dc/elements/1.1/']]))[1] AS title,
  (xpath('//dc:date/text()',content::xml,ARRAY[ARRAY['dc','http://purl.org/dc/elements/1.1/']]))[1] AS date
FROM dnb_zdb_oai_dc;


                                               title                                                | date 
----------------------------------------------------------------------------------------------------+------
 Jahrbuch Deutsch als Fremdsprache                                                                  | 2022
 Jahrbuch Noßwitz                                                                                   | 2022
 [Lokalanzeiger] ; Lokalanzeiger : mein Südhessen                                                   | 2022
 Südstadtgemeinde aktuell / Herausgeber: Der Kirchenvorstand der Ev.-luth. Südstadt-Kirchengemeinde | 2022
 Nomos eLibrary; Sportwissenschaft; [E-Journals]                                                    | 2022
 Nomos eLibrary; Geschichte; [E-Journals]                                                           | 2022
 Nomos eLibrary; Mediation; [E-Journals]                                                            | 2022
 Psychology of human-animal intergroup relations : PHAIR                                            | 2022
 Global environmental psychology                                                                    | 2022
 Journal of integrative and complementary medicine                                                  | 2022
(...)

                                  
```

## Support Functions

These support functions help to retrieve additional information from an OAI Server to allow harvesters to limit harvest requests to portions of the metadata available from a repository.

### [OAI_Identify](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#oai_identify)

**Synopsis**

*SETOF OAI_IdentityNode* **OAI_Identify**(server_name *text*);


`server_name`: Name of a previously created OAI Foreign Data Wrapper `SERVER`

-------

**Availability**: 1.0.0

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

-------

**Availability**: 1.0.0

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

`server_name`: Name of a previously created OAI Foreign Data Wrapper `SERVER``

-------

**Availability**: 1.0.0

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

-------

**Availability**: 1.0.0

**Description**

Shows the version of the installed OAI FDW and its main libraries.

**Usage**

```sql
SELECT OAI_Version();
                                                                                                    oai_version                                                                                                     
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 oai fdw = 1.12, libxml = 2.9.14, libcurl = libcurl/8.14.1 OpenSSL/3.5.4 zlib/1.3.1 brotli/1.1.0 zstd/1.5.7 libidn2/2.3.8 libpsl/0.21.2 libssh2/1.11.1 nghttp2/1.64.0 nghttp3/1.8.0 librtmp/2.3 OpenLDAP/2.6.10
(1 row)
```

### [OAI_HarvestTable](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#oai_harvesttable)

**Synopsis**

*void* **OAI_HarvestTable**(oai_table *text*, target_table *text*, page_size *interval*, start_date *timestamp*);

*void* **OAI_HarvestTable**(oai_table *text*, target_table *text*, page_size *interval*, start_date *timestamp*, end_date *timestamp*);

*void* **OAI_HarvestTable**(oai_table *text*, target_table *text*, page_size *interval*, start_date *timestamp*, end_date *timestamp*, create_table *boolean*);

*void* **OAI_HarvestTable**(oai_table *text*, target_table *text*, page_size *interval*, start_date *timestamp*, end_date *timestamp*, create_table *boolean*, verbose *boolean*);


`oai_table`: OAI foreign table

`target_table`: Local table where the data from the OAI foreign table will be imported to. If the `target_table` does not exist, a new table with the given name will be automatically created - unless explicitly configured otherwise in the parameter `create_table`. The `target_table` will be appended if it already exists. If the `oai_table`, and consequently the `target_table`, have an `identifier` column, the system will ensure that records are not duplicated in the `target_table` by updating the records in case of a conflict (upsert). 

`page_size`: Page size (time interval) in which the OAI Foreign Data Wrapper will request data from the OAI repository. For instance, setting this parameter to `1 day` within a time window from `2022-01-01` until `2022-01-10` will be translated into 10 distinct requests to the OAI repository.

`start_date`:  Start date from the time window.

`end_date` (optional): End date from the time window. Default **CURRENT_TIMESTAMP**.

`create_table` (optional): Set this parameter to `false` in case the target table already exists. Default **TRUE**.

`verbose` (optional): Set this parameter to `true` for more comprehensive output messages. Default **FALSE**.

-------

**Availability**: 1.2.0

**Description**

Often it is the case that a OAI repository contains so much data, that a requests over large time intervals become just too expensive and end up being denied by the server. This stored procedure addresses this issue by internally partitioning a single request into several small ones using the a given [time interval](https://www.postgresql.org/docs/current/datatype-datetime.html) as partition unit - parameter `page_size`. 

For instance, an OAI ListRecords request for all records from the year 2021 (`2021-01-01` to `2021-12-31`) can be split into 12 smaller requests by setting the `page_size` parameter to `interval '1 month'`. Although in the end the result sets from both approaches are pretty much the same, both client and server may significantly profit from having  smaller result sets instead of single large one.


**Usage**

```sql
-- Cloning records from foreign table 'ulb_oai_dc' to a new table called 'clone_ulb_oai_dc'

CALL OAI_HarvestTable('ulb_oai_dc','clone_ulb_oai_dc', interval '5 days', '2022-10-01 09:00:00', '2022-10-17 17:00:00');

INFO:  target table "public.clone_ulb_oai_dc" successfully created.
INFO:  90 records from "public.ulb_oai_dc" successfully inserted into "public.clone_ulb_oai_dc" [2022-10-01 09:00:00 - 2022-10-17 17:00:00].
```

## [Deploy with Docker](#deploy-with-docker)

To deploy oai_fdw with docker just pick one of the supported PostgreSQL versions, install the [requirements](#requirements) and [compile](#build-and-install) the [source code](https://github.com/jimjonesbr/oai_fdw/releases). For instance, a oai_fdw `Dockerfile` for PostgreSQL 16 should look like this (minimal example):

```docker
FROM postgres:16

RUN apt-get update && \
    apt-get install -y make gcc postgresql-server-dev-16 libxml2-dev libcurl4-openssl-dev

RUN tar xvzf oai_fdw-1.8.0.tar.gz && \
    cd oai_fdw-1.8.0 && \
    make -j && \
    make install
```

To build the image save it in a `Dockerfile` and  run the following command in the root directory - this will create an image called `oai_fdw_image`.:
 
```bash
 $ docker build -t oai_fdw_image .
```

After successfully building the image you're ready to `run` or `create` the container ..
 
```bash
$ docker run --name my_oai_container -e POSTGRES_HOST_AUTH_METHOD=trust oai_fdw_image
```

.. and then finally you're able to create and use the extension!

```bash
$ docker exec -u postgres my_oai_container psql -d mydatabase -c "CREATE EXTENSION oai_fdw"
```
## [Error Handling](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#error-handling)

If there is a network error or other condition that results in the loss of an incomplete list response, the OAI Foreign Data Wrapper will re-issue the most recent call, including the last resumptionToken to continue the list request sequence. The number of attempts and their interval are defined by the `connect_retry` and `connect_timeout` defined at the [CREATE SERVER](#create-server) statement.

If the OAI Foreign Data Wrapper receives a `badResumptionToken` error during a sequence of incomplete list requests it will assume that the `resumptionToken` has either expired or is invalid in some other way. There is no way to resume the list request sequence in this case; the user must start the list request again.

If a harvester receives some other error then there is an unrecoverable problem with the list request sequence; the user must start the list request again.

## [Limitations](https://github.com/jimjonesbr/oai_fdw/blob/master/README.md#limitations)

* **PostgreSQL**: The OAI Foreign Data Wrapper currently supports only PostgreSQL 11 or higher.
* **Aggregate and Join Push-down**: Aggregate functions and joins are not pushed down to the OAI repository, as such features are not foreseen by the OAI-PMH protocol. This means that aggregate and join operations will pull all necessary data from the server and then will perform the operations on the client side. The same applies for [Aggregate Expressions](https://www.postgresql.org/docs/14/sql-expressions.html#SYNTAX-AGGREGATES) and [Window Functions](https://www.postgresql.org/docs/current/tutorial-window.html).
* **Data from OAI Requests are always pulled entirely**: The OAI Foreign Data Wrapper sort of translates SQL Queries to standard OAI-PMH HTTP requests in order access the data sets, which is basically limited to [ListRecords](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListRecords) or [ListIdentifiers](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListIdentifiers) requests (in case the node `content` isn't listed in the `SELECT` clause). These OAI requests cannot be altered to only partially retrieve information, so the requests result sets will always be downloaded entirely - even if not used in the `SELECT` clause. 
* **Operators**: The OAI-PMH supports [selective harvesting](http://www.openarchives.org/OAI/openarchivesprotocol.html#SelectiveHarvesting) with only a few attributes and operators and `oai_nodes`:


| oai_node     | operator                     |
|--------------|------------------------------|
| `datestamp`  | `=`,`>`,`>=`,`<`,`<=`, `BETWEEN` |
| `setspec`    | `<@`,`@>`                    |
| `identifier` | `=`                          |
| `metadataprefix`       | `=`                          |
|              |                              |
* **Response Compression**: Response compression from OAI-PMH servers is currently not supported. 

Note that all operators supported in PostgreSQL can be used to filter result sets, but only the supported operators listed above will be used in the OAI-PMH requests. In other words, non supported filters will be performed **locally** in the client.