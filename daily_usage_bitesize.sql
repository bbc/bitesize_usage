set search_path TO 'central_insights_sandbox';

CREATE TABLE vb_bitesize_web_daily
    (
        is_signed_in bool,
        dt date,
        num_users bigint
    )
;
INSERT INTO central_insights_sandbox.vb_bitesize_web_daily
SELECT is_signed_in,
       dt::date as date_of_event,
       count(distinct audience_id)
FROM s3_audience.audience_activity_daily_summary
WHERE dt = '20190501'
  AND destination = 'PS_BITESIZE'
GROUP BY 1, 2;


SELECT * FROM vb_bitesize_web_daily LIMIT 100;
--DELETE  FROM vb_bitesize_web_daily WHERE dt = 	'2019-05-01';

CREATE TABLE vb_bitesize_iplayer_daily
    (
        dt date,
        tleo varchar(250),
        num_users bigint,
        playback_time_total bigint
    )
;

INSERT INTO vb_bitesize_iplayer_daily
SELECT date_of_event,
       top_level_editorial_object,
       count(distinct audience_id) as num_users,
       sum(playback_time_total)    as playback_time_total
FROM audience.audience_activity_daily_summary_enriched
WHERE date_of_event = '2019-05-01'
  AND destination = 'PS_IPLAYER'
  AND brand_title ILIKE '%bitesize%'
GROUP BY 1, 2;

SELECT dt, max(num_users) from central_insights_sandbox.vb_bitesize_web_daily;--2,418,726

SELECT * FROM central_insights_sandbox.vb_bitesize_web_daily WHERE num_users = 2418726;
