
---------------------------------------------
# PostgreSQL Foreign Data Wrapper for OAI-PMH (oai_fdw)

A PostgreSQL Foreign Data Wrapper to access OAI-PMH Werbservices (Open Archives Initiative Protocol for Metadata Harvesting). It supports the [OAI-PMH 2.0 Protocol](http://www.openarchives.org/OAI/openarchivesprotocol.html). 

## Requirements

* [libxml2](http://www.xmlsoft.org/) (2.9.10+)
* [libcurl](https://curl.se/libcurl/) (7.74.0+)

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

### CREATE SERVER
In order to access the `oai_fdw` features you first need to create a `SERVER` and tell it which OAI-PMH endpoint it should connect to. 

```sql
CREATE SERVER oai_server_dnb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://services.dnb.de/oai/repository');
```

**Server Options**:
| Server Option | Requirement          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `url`  | mandatory        | URL address of the OAI-PMH endpoint.  

### CREATE FOREIGN TABLE
After creating the `SERVER`, the next step is to create a `FOREIGN TABLE` and map its columns to the supported `oai_node`'s. The nodes must be mapped using the `OPTION` clause of each column, as shown in the exaple bellow. 

```sql
CREATE FOREIGN TABLE dnb_maps_marc21 (
  id text OPTIONS (oai_attribute 'identifier'),
  content text OPTIONS (oai_attribute 'content'),
  setspec text[] OPTIONS (oai_attribute 'setspec'),
  datestamp timestamp OPTIONS (oai_attribute 'datestamp'),
  meta text OPTIONS (oai_attribute 'metadataprefix')
)
  SERVER oai_server OPTIONS (setspec 'dnb:reiheC',
                             metadataPrefix 'oai_dc');
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
Now we just need to tell the `oai_fdw` server which information we're looking for and in which format the data should be delivered. These information are also attached to the `FOREIGN TABLE` using the `SERVER` clause, which similar to the he column mapping also has an `OPTION` clause. These options allow to limit harvest requests to portions of the metadata available from a repository. Expected options are:

| Server Option | Requirement          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `metadataprefix`  | mandatory        | an argument that specifies the metadataPrefix of the format that should be included in the metadata part of the returned records. Records should be included only for items from which the metadata format matching the metadataPrefix can be disseminated. The metadata formats supported by a repository and for a particular item can be retrieved using the [ListMetadataFormats](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats) request.  
| `from`  | optional        | an argument with a UTCdatetime value, which specifies a lower bound for datestamp-based selective harvesting.  
| `until`  | optional        | an argument with a UTCdatetime value, which specifies a upper bound for datestamp-based selective harvesting.  
| `setspec`  | optional        | an argument with a setSpec value , which specifies set criteria for selective harvesting. 

####  Examples:

1. Create a `SERVER` and `FOREIGN TABLE` to harvest the [OAI-PMH Endpoint](https://sammlungen.ulb.uni-muenster.de/oai) of the Münster University Library with records encoded as `oai_dc`

```sql
-- Münster University Library
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
```

2. Create a `SERVER` and a `FOREIGN TABLE` to harvest the [OAI-PMH Endpoint](https://sammlungen.ulb.uni-muenster.de/oai) of the Münster University Library with records encoded as `oai_dc` in the set `ulbmsuo`.

```sql
CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (
  id text                OPTIONS (oai_node 'identifier'), 
  xmldoc xml             OPTIONS (oai_node 'content'), 
  sets text[]            OPTIONS (oai_node 'setspec'), 
  updatedate timestamp   OPTIONS (oai_node 'datestamp'),
  format text            OPTIONS (oai_node 'metadataprefix')
 ) SERVER oai_server_ulb OPTIONS (metadataprefix 'oai_dc',
                                  setspec 'ulbmsuo');
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
						   
SELECT * FROM dnb_zdb_oai_dc LIMIT 1;

```

4. It is possible to set or overwrite the pre-configured `SERVER OPTION` values by filtering the records with the SQL `WHERE` clause. The following example shows how to set the harversting metadataPrefix, set, and time interval values in query time:

```sql
CREATE SERVER oai_server_dnb FOREIGN  DATA WRAPPER oai_fdw OPTIONS (url 'https://services.dnb.de/oai/repository');

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text             OPTIONS (oai_node 'identifier'), 
  content text        OPTIONS (oai_node 'content'), 
  setspec text[]      OPTIONS (oai_node 'setspec'), 
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  meta text           OPTIONS (oai_node 'metadataPrefix')
 ) SERVER oai_dnb_server OPTIONS (metadataPrefix 'oai_dc');
						   
SELECT * FROM dnb_zdb_oai_dc 
WHERE 
  meta = 'MARC21-xml' AND
  datestamp BETWEEN '2022-02-01' AND '2022-02-02' AND
  setspec <@ ARRAY['dnb:reiheC']
LIMIT 1;

                id                |                                                           content                                                            |   setspec    |      datestamp      |    meta    
----------------------------------+------------------------------------------------------------------------------------------------------------------------------+--------------+---------------------+------------
 oai:dnb.de/dnb:reiheC/1246033399 | <record xmlns="http://www.loc.gov/MARC21/slim" type="Bibliographic">                                                        +| {dnb:reiheC} | 2022-02-01 21:30:31 | MARC21-xml
                                  |     <leader>00000nem a2200000 c 4500</leader>                                                                               +|              |                     | 
                                  |     <controlfield tag="001">1246033399</controlfield>                                                                       +|              |                     | 
                                  |     <controlfield tag="003">DE-101</controlfield>                                                                           +|              |                     | 
                                  |     <controlfield tag="005">20220201223031.0</controlfield>                                                                 +|              |                     | 
                                  |     <controlfield tag="007">a|||||||</controlfield>                                                                         +|              |                     | 
                                  |     <controlfield tag="008">211118s2021    gw |||||||a||   ||||ger  </controlfield>                                         +|              |                     | 
                                  |     <datafield tag="015" ind1=" " ind2=" ">                                                                                 +|              |                     | 
                                  |       <subfield code="a">22,C01</subfield>                                                                                  +|              |                     | 
                                  |       <subfield code="2">dnb</subfield>                                                                                     +|              |                     | 
                                  |     </datafield>                                                                                                            +|              |                     | 
                                  |     <datafield tag="016" ind1="7" ind2=" ">                                                                                 +|              |                     | 
                                  |       <subfield code="2">DE-101</subfield>                                                                                  +|              |                     | 
                                  |       <subfield code="a">1246033399</subfield>                                                                              +|              |                     | 
                                  |     </datafield>                                                                                                            +|              |                     | 
                                  |     <datafield tag="020" ind1=" " ind2=" ">                                                                                 +|              |                     | 
                                  |       <subfield code="a">9783866361010</subfield>                                                                           +|              |                     | 
                                  |       <subfield code="c">keine Bindung</subfield>                                                                           +|              |                     | 
                                  |       <subfield code="9">978-3-86636-101-0</subfield>                                                                       +|              |                     | 
                                  |     </datafield>                                                                                                            +|              |                     | 
                                  |     <datafield tag="034" ind1="0" ind2=" ">                                                                                 +|              |                     | 
                                  |       <subfield code="a">a</subfield>                                                                                       +|              |                     | 
                                  |       <subfield code="d">E 012 11 35</subfield>                                                                             +|              |                     | 
                                  |       <subfield code="e">E 012 26 51</subfield>                                                                             +|              |                     | 
                                  |       <subfield code="f">N 053 33 39</subfield>                                                                             +|              |                     | 
                                  |       <subfield code="g">N 053 22 44</subfield>                                                                             +|              |                     | 
                                  |       <subfield code="9">A:agx</subfield>                                                                                   +|              |                     | 
                                  |     </datafield>                                                                                                            +|              |                     | 
                                  |     <datafield tag="034" ind1="0" ind2=" ">                                                                                 +|              |                     | 
                                  |       <subfield code="a">a</subfield>                                                                                       +|              |                     | 
                                  |       <subfield code="d">E012.193055</subfield>                                                                             +|              |                     | 
                                  |       <subfield code="e">E012.447500</subfield>                                                                             +|              |                     | 
                                  |       <subfield code="f">N053.560833</subfield>                                                                             +|              |                     | 
                                  |       <subfield code="g">N053.378888</subfield>                                                                             +|              |                     | 
                                  |       <subfield code="9">A:dgx</subfield>                                                                                   +|              |                     | 
                                  |     </datafield>                                                                                                            +|              |                     | 
.... 

```

