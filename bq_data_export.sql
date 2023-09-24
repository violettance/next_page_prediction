-- main table data check using item name

SELECT
  ga4.event_name,
  ga4.user_pseudo_id,
  ga4.event_timestamp,
  it.item_id,
  it.item_name
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` ga4,
  unnest(ga4.items) as it
WHERE
  it.item_id is not null;

  -- view item data check
SELECT
  *
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` ga4
WHERE
  ga4.event_name = 'view_item';

-- session check with view_item
SELECT
  ga4.*
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` ga4,
  unnest(ga4.event_params) ep
WHERE
  ep.value.int_value = 8567490518
  AND ga4.event_name = 'page_view'
ORDER BY
  ga4.event_timestamp;

-- page view order - main query
WITH base_data AS (
  SELECT
    ga4.*
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` ga4,
    unnest(ga4.event_params) ep
  WHERE
    -- ep.value.int_value = 8567490518
    -- AND
    ga4.event_name = 'page_view'
  ORDER BY
    ga4.event_timestamp
)
SELECT
  -- bep.key,
  bd.event_timestamp,
  SUM(COALESCE(bep.value.int_value, 0)) AS session_id,
  STRING_AGG(bep.value.string_value, '') AS page_path
FROM
  base_data bd,
  unnest(bd.event_params) bep
WHERE
  bep.key IN ('page_location', 'ga_session_id')
  GROUP BY
  bd.event_timestamp
  ORDER BY
  session_id,
  event_timestamp;