# Software still under construction!
---------------------------------------------
# PostgreSQL Foreign Data Wrapper for OAI-PMH (oai_fdw)

A PostgreSQL Foreign Data Wrapper to access OAI-PMH Werbservices (Open Archives Initiative Protocol for Metadata Harvesting). It supports the [OAI-PMH 2.0 Protocol](http://www.openarchives.org/OAI/openarchivesprotocol.html). 

## Requirements

* [libxml2](http://www.xmlsoft.org/)
* [libcurl](https://curl.se/libcurl/)

## Install 

```sql
CREATE EXTENSION oai_fdw;
```

## How to use 

In order to access the `oai_fdw` features you first need to create a `SERVER`. 

```sql
CREATE SERVER my_oai_server FOREIGN DATA WRAPPER oai_fdw;
```

The next step is to create a `FOREIGN TABLE` and map its columns to the supported `oai_attributes`. The attributes must be mapped using the `OPTION` clause of each column, as shown in the exaple bellow. The `oai_attribute` values expected are:


| oai_attribute | PostgreSQL type          | Description                                                                                                        |
|---------------|--------------------------|--------------------------------------------------------------------------------------------------------------------|
| `identifier`  | `text`, `varchar`        | The unique identifier of an item in a repository (OAI Header).                                                     |
| `setspec`     | `text[]`, `varchar[]`    | The set membership of the item for the purpose of selective harvesting. (OAI Header)                               |
| `datestamp`   | `timestamp`              | The date of creation, modification or deletion of the record for the purpose of selective harvesting. (OAI Header) |
| `content`     | `text`, `varchar`, `xml` | The XML document representing the retrieved recored (OAI Record)                                                   |
| `metadataPrefix`     | `text`, `varchar` | The metadataPrefix - a string that specifies the metadata format in OAI-PMH requests issued to the repository      |

Now we just need to tell the `oai_fdw` server where the OAI-PMH server is, and which information we're looking for. These information are also attached to the `FOREIGN TABLE` using the `SERVER` clause, which similar to the he column mapping also has an `OPTION` clause. These options allow to limit harvest requests to portions of the metadata available from a repository. Expected options are:

| oai_fdw option   | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`            | URL of the OAI-PMH Werbserice                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `metadataPrefix` | an argument that specifies the metadataPrefix of the format that should be included in the metadata part of the returned records. Records should be included only for items from which the metadata format matching the metadataPrefix can be disseminated. The metadata formats supported by a repository and for a particular item can be retrieved using the [ListMetadataFormats](http://www.openarchives.org/OAI/openarchivesprotocol.html#ListMetadataFormats) request. |
| `from`           | an *optional* argument with a UTCdatetime value, which specifies a lower bound for datestamp-based selective harvesting.|
| `until`          | an *optional* argument with a UTCdatetime value, which specifies a upper bound for datestamp-based selective harvesting.|
| `set`            | an *optional* argument with a setSpec value , which specifies set criteria for selective harvesting.|


####  Examples:

1. Create a `SERVER` and `FOREIGN TABLE` to harvest the [OAI-PMH Endpoint](https://sammlungen.ulb.uni-muenster.de/oai) of the Münster University Library with records encoded as `oai_dc`

```sql
CREATE SERVER ulb_oai_server FOREIGN DATA WRAPPER oai_fdw;

CREATE FOREIGN TABLE ulb_oai_dc (
  id text                OPTIONS (oai_attribute 'identifier'), 
  xmldoc xml             OPTIONS (oai_attribute 'content'), 
  sets text[]            OPTIONS (oai_attribute 'setspec'), 
  updatedate timestamp   OPTIONS (oai_attribute 'datestamp'),
  format text            OPTIONS (oai_attribute 'metadataPrefix')
 ) SERVER ulb_oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                                  metadataPrefix 'oai_dc');
                                                      
SELECT * FROM ulb_oai_dc LIMIT 1;
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
(1 row)

```

2. Create a `SERVER` and a `FOREIGN TABLE` to harvest the [OAI-PMH Endpoint](https://sammlungen.ulb.uni-muenster.de/oai) of the Münster University Library with records encoded as `oai_dc` in the set `ulbmsuo`.

```sql
CREATE FOREIGN TABLE ulb_ulbmsuo_oai_dc (
  id text                OPTIONS (oai_attribute 'identifier'), 
  xmldoc xml             OPTIONS (oai_attribute 'content'), 
  sets text[]            OPTIONS (oai_attribute 'setspec'), 
  updatedate timestamp   OPTIONS (oai_attribute 'datestamp'),
  format text            OPTIONS (oai_attribute 'metadataPrefix')
 ) SERVER ulb_oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                                  metadataPrefix 'oai_dc',
                                  set 'ulbmsuo');
								  
SELECT * FROM ulb_ulbmsuo_oai_dc LIMIT 1;

                   id                    |                                                                                                                                             xmldoc                                                                                                                                              |              sets               |     updatedate      | format 
-----------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------+---------------------+--------
 oai:digital.ulb.uni-muenster.de:1188818 | <oai_dc:dc xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">                                                                                                                                                                    +| {ulbmsuo,book,nuenning,benelux} | 2012-08-08 07:50:15 | oai_dc
                                         |   <dc:title>De Vryheyt Vertoont in den Staat der Vereenigde Nederlanden : Zijnde een Vertoogh van des zelfs maniere van Regeeringe, veelvondige Middelen en Oorlogs-Lasten, Met de Autentyke Stukken, zo van de Munstersche als andere Vredens Tractaten, Unie en Verbonden voorsien</dc:title>+|                                 |                     | 
                                         |   <dc:description>[U. E.]</dc:description>                                                                                                                                                                                                                                                     +|                                 |                     | 
                                         |   <dc:description>Vorlageform des Erscheinungsvermerks: By Engelbregt Boucquet, Boekverkooper ...</dc:description>                                                                                                                                                                             +|                                 |                     | 
                                         |   <dc:publisher>Boucquet</dc:publisher>                                                                                                                                                                                                                                                        +|                                 |                     | 
                                         |   <dc:publisher>Univ.- und Landesbibliothek</dc:publisher>                                                                                                                                                                                                                                     +|                                 |                     | 
                                         |   <dc:date>1697</dc:date>                                                                                                                                                                                                                                                                      +|                                 |                     | 
                                         |   <dc:type>Text</dc:type>                                                                                                                                                                                                                                                                      +|                                 |                     | 
                                         |   <dc:type>Book</dc:type>                                                                                                                                                                                                                                                                      +|                                 |                     | 
                                         |   <dc:format>[4] Bl., 400 S. ; 12-o</dc:format>                                                                                                                                                                                                                                                +|                                 |                     | 
                                         |   <dc:identifier>eki:HBZHT007586425</dc:identifier>                                                                                                                                                                                                                                            +|                                 |                     | 
                                         |   <dc:identifier>hbz-idn:CT005003895</dc:identifier>                                                                                                                                                                                                                                           +|                                 |                     | 
                                         |   <dc:identifier>urn:nbn:de:hbz:6-85659548630</dc:identifier>                                                                                                                                                                                                                                  +|                                 |                     | 
                                         |   <dc:identifier>https://nbn-resolving.de/urn:nbn:de:hbz:6-85659548630</dc:identifier>                                                                                                                                                                                                         +|                                 |                     | 
                                         |   <dc:identifier>system:HT007586425</dc:identifier>                                                                                                                                                                                                                                            +|                                 |                     | 
                                         |   <dc:relation>vignette : http://sammlungen.ulb.uni-muenster.de/titlepage/urn/urn:nbn:de:hbz:6-85659548630/128</dc:relation>                                                                                                                                                                   +|                                 |                     | 
                                         |   <dc:coverage>Den Haag</dc:coverage>                                                                                                                                                                                                                                                          +|                                 |                     | 
                                         |   <dc:rights>reserved</dc:rights>                                                                                                                                                                                                                                                              +|                                 |                     | 
                                         | </oai_dc:dc>                                                                                                                                                                                                                                                                                    |                                 |                     | 
(1 row)
```

3. Create a `SERVER` and a `FOREIGN TABLE` to harvest the [OAI-PMH Endpoint](https://services.dnb.de/oai/repository) of the German National Library with records encoded as `oai_dc` in the set `zdb` created between `2022-01-31` and `2022-02-01` (YYYY-MM-DD).


```sql
CREATE SERVER oai_dnb_server FOREIGN DATA WRAPPER oai_fdw;

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_attribute 'identifier'), 
  content text OPTIONS (oai_attribute 'content'), 
  setspec text[] OPTIONS (oai_attribute 'setspec'), 
  datestamp timestamp OPTIONS (oai_attribute 'datestamp'),
  meta text OPTIONS (oai_attribute 'metadataPrefix')
 ) 
SERVER oai_dnb_server OPTIONS (url 'https://services.dnb.de/oai/repository', 
                               set 'zdb', 
                               metadataPrefix 'oai_dc',
                               from '2022-01-31', 
                               until '2022-02-01');
						   
SELECT * FROM dnb_zdb_oai_dc LIMIT 1;

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
(1 row)
```

4. It is possible to set or overwrite the pre-configured `SERVER OPTION` values by filtering the records with the SQL `WHERE` clause. The following example shows how to set the harversting metadataPrefix, set, and time interval values in query time:

```sql
CREATE SERVER oai_dnb_server FOREIGN DATA WRAPPER oai_fdw;

CREATE FOREIGN TABLE dnb_zdb_oai_dc (
  id text OPTIONS (oai_attribute 'identifier'), 
  content text OPTIONS (oai_attribute 'content'), 
  setspec text[] OPTIONS (oai_attribute 'setspec'), 
  datestamp timestamp OPTIONS (oai_attribute 'datestamp'),
  meta text OPTIONS (oai_attribute 'metadataPrefix')
 ) 
SERVER oai_dnb_server OPTIONS (url 'https://services.dnb.de/oai/repository', 
                               metadataPrefix 'oai_dc');
						   
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

