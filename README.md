
---------------------------------------------
# PostgreSQL Foreign Data Wrapper for OAI-PMH (oai_fdw)

A PostgreSQL Foreign Data Wrapper to access OAI-PMH Werbservices (Open Archives Initiative Protocol for Metadata Harvesting). It supports the [OAI-PMH 2.0 Protocol](http://www.openarchives.org/OAI/openarchivesprotocol.html). 

## Requirements

* [LibXML2](http://www.xmlsoft.org/): version 2.5.0 or higher.
* [LibcURL](https://curl.se/libcurl/): version 7.74.0 or higher.

## Install 

Compile and install the code.

```bash
make
make install
```

Create the extension after compiling and installing the code.

```sql
CREATE EXTENSION oai_fdw;
```

## Usage

To use the OAI Foreign Data Wrapper you must first create a `SERVER` to connect to an OAI-PMH endpoint. After that create `FOREIGN TABLE`s to enable access to OAI-PMH documents using SQL queries. Each `FOREIGN TABLE` column must be mapped to a `oai_node`, so that PostgreSQL knows where the deliver the OAI data.

### CREATE SERVER

The following example creates a `SERVER` that connects to the OAI-PMH repository of the German National Library.

```sql
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository');
```

**Server Options**:

| Server Option | Requirement          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `url`  | mandatory        | URL address of the OAI-PMH endpoint.  

### CREATE FOREIGN TABLE

The following example creates a `FOREIGN TABLE` connected to the `SERVER` created above, and each table column is mapped to a `oai_node` using the `OPTION` clause. It is mandatory to set a `metadataprefix` to the `SERVER` clause of the `CREATE FOREIGN TABLE` statement, so that the OAI-PMH repository knows which XML format is supposed to be returned. Optionally, it is possible to constraint a `FOREIGN TABLE` to specific OAI sets using the `setspec` option from the `SERVER` clause. Omitting this option means that every SQL query will harvest all sets in the OAI repository.


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
| `metadataPrefix`     | `text`, `varchar` | The metadataPrefix - a string that specifies the metadata format in OAI-PMH requests issued to the repository      |


**Server Options**

| Server Option | Requirement          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `metadataprefix`  | mandatory        | an argument that specifies the metadataPrefix of the format that should be included in the metadata part of the returned records. Records should be included only for items from which the metadata format matching the metadataPrefix can be disseminated. The metadata formats supported by a repository and for a particular item can be retrieved using the [ListMetadataFormats](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats) request.  
| `from`  | optional        | an argument with a UTCdatetime value, which specifies a lower bound for datestamp-based selective harvesting.  
| `until`  | optional        | an argument with a UTCdatetime value, which specifies a upper bound for datestamp-based selective harvesting.  
| `setspec`  | optional        | an argument with a setSpec value , which specifies set criteria for selective harvesting. 

####  Further Examples:

1. Create a `SERVER` and `FOREIGN TABLE` to harvest the [OAI-PMH Endpoint](https://sammlungen.ulb.uni-muenster.de/oai) of the Münster University Library with records encoded as `oai_dc`

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

2. Create a `SERVER` and a `FOREIGN TABLE` to harvest the [OAI-PMH Endpoint](https://sammlungen.ulb.uni-muenster.de/oai) of the Münster University Library with records encoded as `oai_dc` in the set `ulbmsuo`.

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

3. Create a `SERVER` and a `FOREIGN TABLE` to harvest the [OAI-PMH Endpoint](https://services.dnb.de/oai/repository) of the German National Library with records encoded as `oai_dc` in the set `zdb` created between `2022-01-31` and `2022-02-01` (YYYY-MM-DD).


```sql
-- German National Library
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

4. It is possible to set or overwrite the pre-configured `SERVER OPTION` values by filtering the records with the SQL `WHERE` clause. The following example shows how to set the harvesting `metadataPrefix`, `setspec`, and time interval (`from` and `until`) values in query time:

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

These support functions helps to retrieve additional information from an OAI Server, so that queries can be narrowed down.

### OAI_Identify

**Synopsis**

*SETOF OAI_IdentityNode* **OAI_Identify**(server_name *text*);

`server_name`: Name of a previously created OAI Foreign Data Wrapper Server

**Description**

This function is used to retrieve information about a repository. Some of the information returned is required as part of the OAI-PMH. Repositories may also employ the Identify verb to return additional descriptive information.

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

### OAI_ListMetadataFormats

**Synopsis**

*SETOF OAI_MetadataFormat* **OAI_ListMetadataFormats**(server_name *text*);

`server_name`: Name of a previously created OAI Foreign Data Wrapper Server

**Description**

This function is used to retrieve the metadata formats available from a repository. An optional argument restricts the request to the formats available for a specific item.

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


### OAI_ListSets

**Synopsis**

*SETOF OAI_Set* **OAI_ListSets**(server_name *text*);

`server_name`: Name of a previously created OAI Foreign Data Wrapper Server

**Description**

This function is used to retrieve the set structure of a repository, useful for selective harvesting.

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

### OAI_Version

**Synopsis**

*text* **OAI_Version**();

**Description**

Shows the OAI FDW installed version and its main libraries.

**Usage**

```sql
SELECT OAI_Version();
                                                                                      oai_version                                                                                    
  
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
 oai fdw = 1.0.0dev, libxml = 2.9.10, libcurl = libcurl/7.74.0 NSS/3.61 zlib/1.2.11 brotli/1.0.9 libidn2/2.3.0 libpsl/0.21.0 (+libidn2/2.3.0) libssh2/1.9.0 nghttp2/1.43.0 librtmp/2.
3
```