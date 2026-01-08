### oai_fdw 1.12
YYYY-MM-DD

* Bug Fixes
  - Added URL encoding for resumption tokens in ListRecords and ListIdentifiers requests using curl_easy_escape to properly handle special characters like '&' and '='.
  - Ensured xmldoc is only freed if successfully initialized to prevent crashes from freeing uninitialized memory.
  - Set User-Agent and Accept headers in requests for better server compatibility and to mimic standard HTTP client behavior.

### oai_fdw 1.11
2025-09-26

* New Features

  PostgreSQL 18 support.

* Bug fixes

  A safeguard has been introduced to handle cases where xmlDocGetRootElement fails to parse the root node of an XML document. Instead of proceeding with an empty set, an error message is now displayed to inform the user of the issue.

### oai_fdw 1.10
2024-10-14

 * New Features
   
   PostgreSQL 17 support.

### oai_fdw 1.9
2024-03-21

 * New Features *
   - This feature defines a mapping of a PostgreSQL user to an user in the target 
     triplestore - `user` and `password`, so that the user can be authenticated.

 * Bug Fixes *
   - This fix a fixed string length limit in the support functions (512 bytes). I
     mistakenly assumed that it was defined like that in the OAI-PMH Standard.

### oai_fdw 1.8
2023-11-15

 * Performance improvements
   -  This intoduces a new pagination logic to go through the result
      sets from OAI requests. It no longer iterates over the xml document
      to retrieve the OAI records for building the tuples. Instead, it
      now parses all records from a result set into a list right after
      loading the xml response, so that they can accessed later on for
      building the tuples.

### oai_fdw 1.7
2023-09-22

 * New Features *
   -  Adds PostgreSQL 16 support

### oai_fdw 1.6
2023-04-11

 * New Features *
   -  Adds `request_redirect` and `request_max_redirect` options (CREATE SERVER).
      This allows users to enable or disable URL redirects issued by the server
      and also how many times a redirection may occur in a single http request.

 * Bug Fixes *
   - This bug fix implements a http response validation to avoid a server crash
     when a support function receives an invalid OAI document from the repository.

### oai_fdw 1.5.1
2023-04-11

 * Bug Fixes *
   - This bug fix implements a http response validation to avoid a server crash
     when a support function receives an invalid OAI document from the repository.

### oai_fdw 1.5.0
2022-11-15

 * Enhancements *
   - add number of inserted and updated records in the harvester log messages
    
 * Bug Fixes *
   - clean up parser in support function calls (libxml2)
   - fix wrong initial page size in the logs for debug1 sessions
   - add missing memory contexts for oai loader and parser.


### oai_fdw 1.4.0
2022-10-27
 
 * New Features *
   - add update path script for oai_fdw upgrades: 1.1 -> 1.4, 1.2 -> 1.4
     and 1.3 -> 1.4 
    
 * Bug Fixes *
   - fix memory leak in xmldoc for requests with resumption tokens

### oai_fdw 1.3.0
2022-10-21

 * New Features *
   - Connection retry option for HTTP requests (CREATE SERVER).
   
### oai_fdw 1.2.0
2022-10-19

 * New Features *
   - OAI_HarvestTable: new procedure to harvest data from OAI foreign tables and store them into a 
     local table.
   - Connection retry option for HTTP requests (CREATE SERVER).

 * Bug Fixes *
   - Fix memory leaks in the libxml2 parser for ListRecords and ListIdentifiers requests 
   - Fix missing timeout parameter for non-proxy OAI requests
   
* Enhancements *
   - Add regression tests for PostgreSQL 15.

### oai_fdw 1.1.0
2022-10-01

 * New Features *
   - Proxy support for HTTP requests (CREATE SERVER).
   - Connection timeout option for HTTP requests (CREATE SERVER).
     
 * Bug Fixes *
   - cURL exception handling: Adds missing cURL error message and response code in case of failed http 
    requests. In previous versions these values were never fetched.
 
 * Enhancements *
   - URL validator no longer checks if the URL is reachable. It now only checks the URL's syntax.
   - URls with protocols other than HTTP and HTTPS are now rejected before execution, as other
     protocols are not supported in the OAI-PMH Standard.

### oai_fdw 1.0.0
2022-09-05

 * Features *

   This release fully enables usage of the following OAI-PMH 2.0 Requests via SQL queries (see README):

    - ListRecords
    - ListIdentifiers
    - GetRecord
    - Identify
    - ListMetadataFormats
    - ListSets