# PostgreSQL Foreign Data Wrapper for OAI-PMH (oai_fdw)

## Usage

Create a `oai_fdw` server
```sql
CREATE SERVER oai_server FOREIGN DATA WRAPPER oai_fdw;
```
Create the foreign table and add the necessary information to the OAI-PMH Webservice in `OPTIONS`

```sql
CREATE FOREIGN TABLE oai_ulb_ulbmshs (identifier text, content xml, setpec text[], datestamp timestamp) 
SERVER oai_server OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', 
                           set 'ulbmshs', 
                           metadataPrefix 'oai_dc', 
                           from '2015-07-15', 
                           until '2015-07-15');
```
* `url` OAI-PMH Werbserice URL. **required**
* `metadataPrefix` specifies the metadataPrefix of the format that should be included in the metadata part of the returned records. Records should be included only for items from which the metadata format  matching the metadataPrefix can be disseminated. The metadata formats supported by a repository and for a particular item can be retrieved using the ListMetadataFormats request. **required**
* `from` an optional argument with a UTCdatetime value, which specifies a lower bound for datestamp-based selective harvesting. 
* `until` an optional argument with a UTCdatetime value, which specifies a upper bound for datestamp-based selective harvesting.
* `set` an optional argument with a setSpec value , which specifies set criteria for selective harvesting.