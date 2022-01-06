set search_path TO 'central_insights_sandbox';

SELECT TRUE::bool as is_signed_in,
       audience_id,
       date_of_event,
       page_name,
       content_id,
       app_name,
       app_type,
       device_type,
       browser_brand,
       destination,
       page_views_total,
       event_datetime_min,
       event_datetime_max,
       location,
       town,
       nation,
       barb_region,
       acorn_category_description
FROM audience.audience_activity_daily_summary_enriched
WHERE date_of_event = '2020-01-01'
  AND destination = 'PS_BITESIZE'
LIMIT 10;


SELECT is_signed_in,
       audience_id,
       dt::date       as date_of_event,
       page_name,
       content_id,
       app_name,
       app_type,
       device_type,
       browser_brand,
       destination,
       page_views_total,
       event_datetime_min,
       event_datetime_max,
       null:: varchar as location,
       null:: varchar as town,
       null:: varchar as nation,
       null:: varchar as barb_region,
       null:: varchar as acorn_category_description
FROM s3_audience.audience_activity_daily_summary
WHERE dt = '20200101'
  AND destination = 'PS_BITESIZE'
  AND is_signed_in = FALSE
 AND page_name ilike '%keepalive%'
ORDER BY event_datetime_min
LIMIT 10;


