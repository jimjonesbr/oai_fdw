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

DROP SERVER regress_offline CASCADE;
