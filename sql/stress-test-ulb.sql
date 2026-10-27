-- Stress test: harvest the whole oai_dc catalogue of the ULB Münster
-- repository (about 75,000 records in September 2026) with
-- OAI_HarvestTable() in monthly windows. Takes a while.
--
-- OAI_HarvestTable() commits after every window, so the progress can be
-- followed from another session:  SELECT count(*) FROM stress_ulb_harvest;
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

CREATE FOREIGN TABLE stress_ulb_records (
  id text             OPTIONS (oai_node 'identifier'),
  content xml         OPTIONS (oai_node 'content'),
  sets text[]         OPTIONS (oai_node 'setspec'),
  datestamp timestamp OPTIONS (oai_node 'datestamp'),
  deleted boolean     OPTIONS (oai_node 'status')
) SERVER stress_ulb OPTIONS (metadataprefix 'oai_dc');

-- The harvest covers the repository from its earliest datestamp up to now.
SELECT description AS earliest FROM OAI_Identify('stress_ulb')
WHERE name = 'earliestDatestamp' \gset
SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS until \gset

-- end_date is exclusive: one second more includes the records of the last
-- second.
CALL OAI_HarvestTable('stress_ulb_records', 'stress_ulb_harvest', interval '1 month',
                      :'earliest'::timestamp, :'until'::timestamp + interval '1 second');

SELECT "check", CASE WHEN ok THEN 'ok' ELSE result END AS result
FROM (
  SELECT 1, 'records harvested', count(*) || ' records', count(*) > 0
  FROM stress_ulb_harvest
  UNION ALL
  SELECT 2, 'every record has an identifier and a datestamp',
         count(*) FILTER (WHERE id IS NULL OR datestamp IS NULL) || ' without',
         count(*) FILTER (WHERE id IS NULL OR datestamp IS NULL) = 0
  FROM stress_ulb_harvest
  UNION ALL
  SELECT 3, 'every record not deleted has a well-formed document',
         count(*) FILTER (WHERE NOT deleted AND (content IS NULL OR content IS NOT DOCUMENT)) || ' without',
         count(*) FILTER (WHERE NOT deleted AND (content IS NULL OR content IS NOT DOCUMENT)) = 0
  FROM stress_ulb_harvest
  UNION ALL
  SELECT 4, 'no record after the until bound',
         count(*) FILTER (WHERE datestamp > :'until'::timestamp) || ' after',
         count(*) FILTER (WHERE datestamp > :'until'::timestamp) = 0
  FROM stress_ulb_harvest
) AS checks(n, "check", result, ok)
ORDER BY n;

\o results/stress-test-ulb.log
\x on
SELECT count(*) AS "OAI_HarvestTable",
       'https://sammlungen.ulb.uni-muenster.de/oai?verb=ListIdentifiers&metadataPrefix=oai_dc&until='
       || :'until' AS "completeListSize of"
FROM stress_ulb_harvest;
\x off
\o

DROP TABLE stress_ulb_harvest;
DROP SERVER stress_ulb CASCADE;
