-- Stress test: harvest the whole oai_dc catalogue of the ULB Münster
-- repository (about 75,000 records in September 2026), once with
-- ListRecords and once with ListIdentifiers, and check that both harvests
-- are consistent. Takes a while.
--
-- Each check shows 'ok', or what went wrong. The number of records grows
-- with the repository, so it is written to results/stress-test-ulb.log
-- instead: compare it with the completeListSize of the URL logged there.
--
-- ULB throttles fast harvesters with HTTP 429, so the server retries more
-- often than by default: with the growing waits, for about 25 minutes.
\set VERBOSITY terse
SET statement_timeout = 0;
-- throttling warnings depend on the repository's load: only errors count
SET client_min_messages TO error;

CREATE SERVER stress_ulb FOREIGN DATA WRAPPER oai_fdw
OPTIONS (url 'https://sammlungen.ulb.uni-muenster.de/oai', request_redirect 'true',
         connect_retry '10');

-- ListRecords: the table has a content column
CREATE FOREIGN TABLE stress_ulb_records (
  id text             OPTIONS (oai_node 'identifier'),
  content xml         OPTIONS (oai_node 'content'),
  sets text[]         OPTIONS (oai_node 'setspec'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  deleted boolean     OPTIONS (oai_node 'status')
) SERVER stress_ulb OPTIONS (metadataprefix 'oai_dc');

-- ListIdentifiers: the same records, without content
CREATE FOREIGN TABLE stress_ulb_identifiers (
  id text             OPTIONS (oai_node 'identifier'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  deleted boolean     OPTIONS (oai_node 'status')
) SERVER stress_ulb OPTIONS (metadataprefix 'oai_dc');

-- Both harvests stop at the same moment, so that records changed in
-- between cannot make them differ.
SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS until \gset
ALTER FOREIGN TABLE stress_ulb_records OPTIONS (ADD until :'until');
ALTER FOREIGN TABLE stress_ulb_identifiers OPTIONS (ADD until :'until');

-- Harvest 1: one scan through all ListRecords pages
CREATE TABLE stress_ulb_harvest AS SELECT * FROM stress_ulb_records;

-- Harvest 2: one scan through all ListIdentifiers pages
CREATE TABLE stress_ulb_harvest_ids AS SELECT * FROM stress_ulb_identifiers;

SELECT "check", CASE WHEN ok THEN 'ok' ELSE result END AS result
FROM (
  SELECT 1, 'records harvested', count(*) || ' records', count(*) > 0
  FROM stress_ulb_harvest
  UNION ALL
  SELECT 2, 'no duplicate identifiers',
         (count(*) - count(DISTINCT id)) || ' duplicates',
         count(*) = count(DISTINCT id)
  FROM stress_ulb_harvest
  UNION ALL
  SELECT 3, 'every record has an identifier and a datestamp',
         count(*) FILTER (WHERE id IS NULL OR datestamp IS NULL) || ' without',
         count(*) FILTER (WHERE id IS NULL OR datestamp IS NULL) = 0
  FROM stress_ulb_harvest
  UNION ALL
  SELECT 4, 'every record not deleted has a well-formed document',
         count(*) FILTER (WHERE NOT deleted AND (content IS NULL OR content IS NOT DOCUMENT)) || ' without',
         count(*) FILTER (WHERE NOT deleted AND (content IS NULL OR content IS NOT DOCUMENT)) = 0
  FROM stress_ulb_harvest
  UNION ALL
  SELECT 5, 'no record after the until bound',
         count(*) FILTER (WHERE datestamp > :'until'::timestamp) || ' after',
         count(*) FILTER (WHERE datestamp > :'until'::timestamp) = 0
  FROM stress_ulb_harvest
  UNION ALL
  SELECT 6, 'ListRecords and ListIdentifiers return the same records',
         (SELECT count(*) FROM (SELECT id, datestamp, deleted FROM stress_ulb_harvest
                                EXCEPT SELECT id, datestamp, deleted FROM stress_ulb_harvest_ids) a)
         || ' / ' ||
         (SELECT count(*) FROM (SELECT id, datestamp, deleted FROM stress_ulb_harvest_ids
                                EXCEPT SELECT id, datestamp, deleted FROM stress_ulb_harvest) b)
         || ' differ',
         NOT EXISTS (SELECT id, datestamp, deleted FROM stress_ulb_harvest
                     EXCEPT SELECT id, datestamp, deleted FROM stress_ulb_harvest_ids)
     AND NOT EXISTS (SELECT id, datestamp, deleted FROM stress_ulb_harvest_ids
                     EXCEPT SELECT id, datestamp, deleted FROM stress_ulb_harvest)
) AS checks(n, "check", result, ok)
ORDER BY n;

\o results/stress-test-ulb.log
\x on
SELECT (SELECT count(*) FROM stress_ulb_harvest) AS "ListRecords",
       (SELECT count(*) FROM stress_ulb_harvest_ids) AS "ListIdentifiers",
       'https://sammlungen.ulb.uni-muenster.de/oai?verb=ListIdentifiers&metadataPrefix=oai_dc&until='
       || :'until' AS "completeListSize of";
\x off
\o

DROP TABLE stress_ulb_harvest, stress_ulb_harvest_ids;
DROP SERVER stress_ulb CASCADE;
