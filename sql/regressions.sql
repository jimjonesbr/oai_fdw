-- Regression tests for fixed bugs that need no OAI-PMH repository.
-- Port 1 on localhost refuses connections, so requests fail right away.
\set VERBOSITY terse

CREATE SERVER regress_offline FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'http://127.0.0.1:1/oai', connect_retry '0');

CREATE FOREIGN TABLE regress_t (
  id text           OPTIONS (oai_node 'identifier'),
  sets text[]       OPTIONS (oai_node 'setspec'),
  updated timestamp OPTIONS (oai_node 'datestamp'),
  deleted boolean   OPTIONS (oai_node 'status')
) SERVER regress_offline OPTIONS (metadataprefix 'oai_dc');

-- NULL element in a setspec array: used to crash the backend
EXPLAIN (COSTS OFF) SELECT id FROM regress_t WHERE sets && ARRAY[NULL::text];

-- prefix operator: used to read past the argument list and crash
CREATE FUNCTION regress_not(boolean) RETURNS boolean
  LANGUAGE plpgsql IMMUTABLE AS 'BEGIN RETURN NOT $1; END';
CREATE OPERATOR !!! (RIGHTARG = boolean, FUNCTION = regress_not);
EXPLAIN (COSTS OFF) SELECT id FROM regress_t WHERE !!! deleted;
DROP OPERATOR !!! (NONE, boolean);
DROP FUNCTION regress_not(boolean);

-- constants that cannot be pushed down as they are
EXPLAIN (COSTS OFF) SELECT id FROM regress_t WHERE updated < '2020-01-06'::date;
EXPLAIN (COSTS OFF) SELECT id FROM regress_t WHERE updated > '-infinity';
EXPLAIN (COSTS OFF) SELECT id FROM regress_t WHERE updated > '4714-11-24 BC';
EXPLAIN (COSTS OFF) SELECT id FROM regress_t WHERE id = 'oai:x:1';

-- support functions on a server of another FDW: used to crash
CREATE FOREIGN DATA WRAPPER regress_dummy_fdw;
CREATE SERVER regress_other FOREIGN DATA WRAPPER regress_dummy_fdw;
SELECT * FROM OAI_Identify('regress_other');
DROP FOREIGN DATA WRAPPER regress_dummy_fdw CASCADE;

-- support functions require USAGE on the server
CREATE ROLE regress_oai_nopriv;
SET ROLE regress_oai_nopriv;
SELECT * FROM OAI_ListSets('regress_offline');
RESET ROLE;
DROP ROLE regress_oai_nopriv;

-- connect_retry 0 disables retries: no "request ... failed (n/m)" warnings
SELECT * FROM OAI_Identify('regress_offline');

-- server option validation
CREATE SERVER regress_bad FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'http://127.0.0.1:1/oai', request_redirect 'maybe');
CREATE SERVER regress_bad FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'http://127.0.0.1:1/oai', request_max_redirect 'foo');
CREATE SERVER regress_bad FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'ftp://127.0.0.1/oai');

-- from/until table options
CREATE FOREIGN TABLE regress_bad_t (id text OPTIONS (oai_node 'identifier'))
  SERVER regress_offline OPTIONS (metadataprefix 'oai_dc', from 'yesterday');
CREATE FOREIGN TABLE regress_bad_t (id text OPTIONS (oai_node 'identifier'))
  SERVER regress_offline OPTIONS (metadataprefix 'oai_dc', until '2020-02-30');
CREATE FOREIGN TABLE regress_bad_t (id text OPTIONS (oai_node 'identifier'))
  SERVER regress_offline OPTIONS (metadataprefix 'oai_dc', until '2020-01-02 10:00:00');
CREATE FOREIGN TABLE regress_ok_t (id text OPTIONS (oai_node 'identifier'))
  SERVER regress_offline OPTIONS (metadataprefix 'oai_dc', from '2020-01-02T10:00:00Z');
ALTER FOREIGN TABLE regress_ok_t OPTIONS (ADD until 'tomorrow');
DROP FOREIGN TABLE regress_ok_t;

-- hint for an invalid oai_node
\set VERBOSITY default
CREATE FOREIGN TABLE regress_bad_t (x text OPTIONS (oai_node 'foo'))
  SERVER regress_offline OPTIONS (metadataprefix 'oai_dc');
\set VERBOSITY terse

-- OAI_HarvestTable finds foreign tables whose name needs quoting
CREATE FOREIGN TABLE "Regress T" (
  id text           OPTIONS (oai_node 'identifier'),
  updated timestamp OPTIONS (oai_node 'datestamp')
) SERVER regress_offline OPTIONS (metadataprefix 'oai_dc');
CALL OAI_HarvestTable('"Regress T"', 'regress_h', interval '1 day', '2020-01-01', '2020-01-02');

-- values only known at execution time are pushed down (see the DETAIL)
\set VERBOSITY default
SET regress.oai_id = 'oai:x:2';
SELECT id FROM regress_t WHERE id = current_setting('regress.oai_id');
SET regress.oai_set = 'a';
SELECT id FROM regress_t WHERE sets && ARRAY[current_setting('regress.oai_set')];
\set VERBOSITY terse
EXPLAIN (COSTS OFF) SELECT id FROM regress_t
WHERE id = current_setting('regress.oai_id') AND updated > now() - interval '1 day'
  AND updated > now() - interval '2 days';
\set VERBOSITY default
-- a parameter is evaluated again on each rescan, a NULL one needs no request
SELECT v.x, (SELECT count(*) FROM regress_t WHERE id = v.x)
FROM (VALUES (NULL), ('oai:x:1')) v(x);
\set VERBOSITY terse

-- GetRecord takes no from/until: no Identify request for their granularity
CREATE FOREIGN TABLE regress_from_t (id text OPTIONS (oai_node 'identifier'))
  SERVER regress_offline OPTIONS (metadataprefix 'oai_dc', from '2020-01-01');
\set VERBOSITY default
SELECT id FROM regress_from_t WHERE id = 'oai:x:1';
\set VERBOSITY terse

-- OAI_HarvestTable in a current schema whose name needs quoting
CREATE SCHEMA "Regress Schema";
SET search_path = "Regress Schema", public;
CREATE FOREIGN TABLE regress_schema_t (
  id text           OPTIONS (oai_node 'identifier'),
  updated timestamp OPTIONS (oai_node 'datestamp')
) SERVER regress_offline OPTIONS (metadataprefix 'oai_dc');
CALL OAI_HarvestTable('regress_schema_t', 'regress_h', interval '1 day', '2020-01-01', '2020-01-02');
RESET search_path;
DROP SCHEMA "Regress Schema" CASCADE;

-- OAI_HarvestTable checks its arguments before creating anything
CALL OAI_HarvestTable('regress_t', 'regress_h', interval '-1 day', '2020-01-01', '2020-01-10');
CALL OAI_HarvestTable('regress_t', 'regress_h', interval '0', '2020-01-01', '2020-01-10');
CALL OAI_HarvestTable('regress_t', 'regress_h', interval '1 day', NULL, '2020-01-10');
SELECT to_regclass('regress_h') IS NULL AS no_target_table;

DROP SERVER regress_offline CASCADE;
