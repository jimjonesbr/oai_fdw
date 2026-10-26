### oai_fdw 1.15
**unreleased**

* Enhancements

  **Regression tests without network access**: `make installcheck` now only runs the tests that need nothing but a PostgreSQL server. Tests against live repositories and through the Squid proxies are opt-in, with `INCLUDE_EXTERNAL_TESTS=1`, `INCLUDE_LOCAL_TESTS=1` or `INCLUDE_ALL_TESTS=1`.

  **Reproducible builds**: the build date shown by `oai_fdw_settings()` is taken from `SOURCE_DATE_EPOCH`, if set.

  **HTTP 429 flow control**: `429 Too Many Requests` is handled like `503`, honouring `Retry-After`. Without `Retry-After`, the wait before each retry doubles, starting at 5 seconds, and so does the wait after network errors, which used to be retried immediately. Other client errors (4xx), also from a proxy, are no longer retried. libcurl's reason for a failed attempt is written to the server log.

  **Validation of `from` and `until`**: the table options must be a valid `YYYY-MM-DD` or `YYYY-MM-DDThh:mm:ssZ` value.

* Bug fixes

  **Fixed backend crashes**: a NULL element in a `setspec` array filter, a prefix operator in `WHERE`, support functions called on a server of another FDW, and `ListSets`/`ListMetadataFormats` responses with missing elements crashed the backend.

  **Fixed leaks on query cancel**: cancelling a query during a request no longer leaks the curl handle, its socket and the response buffer.

  **Fixed leaks on errors while parsing**: an error while reading a response, e.g. a value that cannot be converted to the database encoding, no longer leaks the response document in `OAI_Identify()`, `OAI_ListSets()` and `OAI_ListMetadataFormats()`, nor libxml2's copies of the values in queries.

  **Support functions require `USAGE`**: `OAI_Identify()`, `OAI_ListSets()` and `OAI_ListMetadataFormats()` now check the `USAGE` privilege on the foreign server.

  **`ListSets` follows `resumptionToken`**: `OAI_ListSets()` and `IMPORT FOREIGN SCHEMA oai_sets` no longer stop after the first page of sets.

  **`from`/`until` granularity**: both are sent in a granularity the repository supports, as announced by `Identify`, so repositories with day granularity no longer answer `badArgument`. `GetRecord`, which takes neither, needs no `Identify` request.

  **Character encoding**: values are converted between UTF-8 and the database encoding, in both directions.

  **Responses are checked**: a response that is not an OAI-PMH document (e.g. an HTML maintenance page) raises an error instead of returning no rows, and OAI errors are raised for all requests. libxml2 parser errors are no longer written to the server's stderr.

  **Pushdown**: `date` constants, infinite and BC timestamps, and constants of other types (e.g. `name`) are no longer pushed down with a wrong value.

  **Pushdown of values known at execution time**: conditions comparing with a parameter (e.g. `$1` in a generic plan of a prepared statement or PL/pgSQL) or an expression such as `now() - interval '1 day'` were not pushed down, so they harvested the whole repository. They are now evaluated when the scan starts, and `EXPLAIN` lists them as `Runtime arguments`.

  **`varchar` columns**: `varchar(n)` limits are applied, and array operators work on `varchar[]` `setspec` columns.

  **`<metadata>` content**: the root element is returned, even if a comment precedes it.

  **`IMPORT FOREIGN SCHEMA`**: the user mapping is used, and `LIMIT TO` only imports sets that exist. Sets whose `setSpec` is longer than 63 characters get unique table names instead of making the import fail.

  **User mapping of views**: a foreign table queried through a view now uses the user mapping of the view owner, as permissions are checked as that role.

  **Redirects**: a redirect that is not followed now raises an error naming its target and suggesting `request_redirect`, instead of reporting an invalid response. A followed redirect no longer warns about the content-type of the redirect response. Neither does a `Content-Type` header without a space after the colon.

  **`OAI_HarvestTable`**: the last time window is no longer skipped, quoted table names work, a current schema whose name needs quoting works, the default `end_date` is the current time in UTC instead of the session's local time (which left out the latest records in sessions west of UTC), and same-named tables in other schemas are no longer mixed up. A `page_size` that is not positive, or a NULL argument, is rejected instead of reporting a completed harvest of nothing. Its "target table created" and "harvester complete" messages are now NOTICEs instead of INFOs, so that they can be silenced with `client_min_messages`; the per-page messages of `exec_verbose` remain INFOs.

  **Server options**: `request_redirect`, `request_max_redirect` and the URL scheme are validated, and `connect_retry '0'` disables retries. Server options are read by one function for queries, the support functions and `IMPORT FOREIGN SCHEMA`, so they are interpreted the same way everywhere.

  **Linking**: the library now links against libxml2.

  **Build with older libcurl**: the build no longer fails with libcurl older than 7.66, e.g. 7.61.1 on RHEL / Rocky Linux 8. `oai_fdw_settings()` then does not report nghttp2.

  **Stalled transfers**: a transfer that receives less than one byte per second for 300 seconds is now aborted and retried, instead of hanging forever with the default `request_timeout` of `0`. Timeouts are reported with libcurl's reason.

  **No signals from libcurl**: libcurl builds without an asynchronous resolver used `SIGALRM` for DNS timeouts, which PostgreSQL uses itself (e.g. for `statement_timeout`). `CURLOPT_NOSIGNAL` is now set.

### oai_fdw 1.14
2026-09-09

* Enhancements

  **Improved EXPLAIN diagnostics**: `EXPLAIN` output now include oai_fdw-specific details for each Foreign Scan node, showing which parameters are pushed down to the remote OAI Server.

  **Enhanced version information**: The `oai_fdw_version()` function now returns a comprehensive version string that includes PostgreSQL version, compiler information, and all dependency versions (libxml, librdf, libcurl) in a single formatted output. A new `oai_fdw_settings()` function provides extended dependency information including optional components like SSL, zlib, libSSH, and nghttp2. The `oai_fdw_settings` view parses this extended information into a table format for convenient programmatic access to individual component versions.

  **Add 'request_timeout' to FOREIGN SERVERS**: This option sets the maximum time in seconds allowed for a complete HTTP request (connect + transfer). `0` disables the limit (default). Unlike `connect_timeout`, this applies to the entire duration of the request, including data transfer.

* Bug fixes

  **Fixed invalid libcurl lifecycle**: `curl_global_init()`/`curl_global_cleanup()` were being called on every OAI request instead of once per backend process. This could interfere with other libcurl users loaded in the same backend (e.g. other FDWs). Global initialization now happens once in `_PG_init()`; cleanup is left to the OS at process exit.

  **Fixed catalog lookup overhead**: Foreign table column metadata (name, OAI node mapping, PostgreSQL type, type modifier, and attribute number) is now loaded once during session initialization and cached in the scan state, then passed to the executor via the serialized plan. Previously this information was looked up from the system catalogs (`GetForeignColumnOptions`) for every column of every row during `CreateOAITuple()`, causing significant syscache overhead on large result sets.

  **Add missing user mapping in helper functions**: GetIdentity, ListSets, and ListMetadataFormats were being executed without loading the `USER MAPPINGS`, which could fail requests if the server needed any sort of authentication. This has now been solved.

  **Encoded credentials are no longer written to server logs via libcurl verbose output**: When `client_min_messages` is set to `DEBUG3`, libcurl's verbose output is now routed through PostgreSQL's `elog(DEBUG3)` via a custom `CURLOPT_DEBUGFUNCTION` callback (`CurlDebugCallback`) instead of being written directly to `stderr`. Crucially, any outgoing or incoming HTTP header whose name matches `Authorization:` or `Proxy-Authorization:` has its value replaced with `[REDACTED]` before being logged, so Bearer tokens, Basic auth credentials (base64-encoded), and proxy passwords never appear in plaintext in the PostgreSQL server log.
  
  **HTTP error response bodies are now truncated in log and error messages**: Error bodies included in `errdetail()` and server-log `elog()` calls were previously unbounded. A misconfigured proxy returning a large HTML error page would be written verbatim into the PostgreSQL server log. Error bodies are now truncated to 512 bytes (`OAI_FDW_MAX_ERROR_BODY`) before being included in any message.

  **SQL identifiers and literals are now properly quoted in `IMPORT FOREIGN SCHEMA`**: The `CREATE FOREIGN TABLE` statements generated by `IMPORT FOREIGN SCHEMA` interpolated the server name, the `metadataPrefix` and the `setSpec` directly into the command text without quoting. Since the `metadataPrefix` and `setSpec` values are taken from the remote OAI repository's `ListMetadataFormats` and `ListSets` responses, a malicious or compromised repository could inject arbitrary SQL that would then be executed with the privileges of the user running the import. Server names and generated table names are now passed through `quote_identifier()`, and option values through `quote_literal_cstr()`.

  **`metadataPrefix` is now URL-encoded in `GetRecord` requests**: Unlike the `ListRecords` and `ListIdentifiers` code paths, the `GetRecord` request builder appended the `metadataPrefix` option to the request URL without passing it through `curl_easy_escape()`. A value containing `&` or `#` could therefore inject additional parameters into the OAI request.

  **OAI header values are no longer returned XML-escaped**: The `identifier`, `setSpec` and `datestamp` values of a record header were extracted with `xmlNodeDump()`, which *serialises* a node back to XML instead of decoding it. Any header value containing a character that has to be escaped in XML was therefore returned to SQL in its escaped form - an identifier such as `oai:example.org/a&b` came back as `oai:example.org/a&amp;b`. Besides being wrong on its own, this silently broke pushdown: a query filtering on the true identifier pushed a correct `GetRecord` request, but the escaped value returned by the scan then failed the local re-check, so a record that exists yielded no rows at all. These values are now read with `xmlNodeGetContent()`, which resolves XML escapes. The `content` node is unaffected, as it legitimately holds serialised XML.

  **OAI-PMH flow control (HTTP 503 / `Retry-After`) is now honoured**: Section 3.4 of the OAI-PMH specification lets a repository answer `503 Service Unavailable` with a `Retry-After` header to ask a harvester to slow down. Such a response was treated as an ordinary failure and retried immediately, up to `connect_retry` times with no delay whatsoever, which hammers a repository that has just reported itself overloaded and then aborts the harvest. A 503 is now recognised, and the delay requested through `Retry-After` is waited out before the next attempt. Both forms allowed by RFC 9110 are accepted (delta-seconds and HTTP-date), a repository that sends no usable `Retry-After` falls back to 5 seconds, and delays are capped at 300 seconds so that a repository cannot pin a backend down indefinitely. The wait is implemented with `WaitLatch()` rather than `pg_usleep()`, so queries remain cancellable while it is in progress.

  **Harvests no longer accumulate every page in memory**: The scan context holding the records of a page was only reset when the scan was restarted or finished, so `LoadOAIRecords()` dropped its reference to the previous page without releasing it. Memory use therefore grew with the size of the whole harvest rather than with the size of a single page. The context is now reset before each new page is requested, with the resumption token carried across the reset in a context of its own.

  **A page that is empty but carries a resumption token no longer truncates the harvest**: `OAIFdwIterateForeignScan()` loaded at most one page per call, so an empty page arriving with a resumption token ended the scan silently and every record behind that token was lost. Pages are now requested until a record is produced or the repository stops handing out tokens.

  **XML response documents are no longer leaked when a request fails**: `LoadOAIRecords()` freed the parsed response only after walking it, so any error raised in between - including every OAI error condition reported by `RaiseOAIException()`, such as `badArgument` or `cannotDisseminateFormat` - skipped the call. As libxml2 allocates these documents outside PostgreSQL's memory contexts, they were not reclaimed at the end of the query and stayed lost for the life of the backend. The document is now released through `PG_TRY()`/`PG_CATCH()` and the pointer cleared, which also removes a stale pointer that could be freed a second time.

  **libcurl callbacks no longer raise PostgreSQL errors**: `WriteMemoryCallback()` and `HeaderCallbackFunction()` could `ereport(ERROR)` - and used `palloc()`, which throws on out-of-memory - while running inside `curl_easy_perform()`. Either would longjmp straight out of libcurl, leaving its handle and connection state inconsistent and skipping the cleanup that follows the transfer. The callbacks now allocate with `malloc()`, record the condition and return a short count, so the transfer fails cleanly and the caller reports it.

  **OAI requests are sent as GET instead of POST**: The request arguments were passed through `CURLOPT_POSTFIELDS`, which made every request a POST even though the log line claimed otherwise. On a 301 or 302 libcurl turns a POST into a GET and drops the body while doing so, so a redirected request reached the repository with no `verb`, no `metadataPrefix` and no other argument - relevant for any `SERVER` created with `request_redirect`. The arguments are now part of the request URL and survive redirects.

### oai_fdw 1.13
2026-02-20

* Bug Fixes

  Moved proxy authentication options (`proxy_user` and `proxy_password`) from SERVER to USER MAPPING. These credentials are now correctly specified in CREATE USER MAPPING instead of CREATE SERVER.

### oai_fdw 1.12
2026-01-09

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