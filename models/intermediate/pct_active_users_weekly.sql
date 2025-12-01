

with events as(
    select
        contact_id,
        TIMESTAMP_TRUNC(message_inserted_at, WEEK(SUNDAY)) AS week_start,
        IF(journey_start_flag IS TRUE, 1, 0) AS journey_start_num,
        IF(opted_in = 'true', 1, 0) AS opted_in_num

    from {{ ref('contact_chats_messages') }}
    where message_inserted_at is not null
),



-- journey_start_flag count for each contact
per_contact_week as(
    select
        contact_id,
        week_start,
        sum(journey_start_num) as journeys_triggered_in_week,
        max(opted_in_num) as opted_in_flag_for_this_contact

    from events
    group by contact_id, week_start
),

-- numerator: distinct contacts in the week with >= 3 journey starts
weekly_active AS (
  SELECT
    week_start,
    COUNTIF(journeys_triggered_in_week >= 3) AS active_teachers
  FROM per_contact_week
  GROUP BY week_start
),

-- denominator #1 (global): total opted-in teachers in the entire population (distinct contacts)
total_opted_in_global AS (
  SELECT COUNT(DISTINCT contact_id) AS total_opted_in
  FROM {{ ref('contact_chats_messages') }}
  WHERE opted_in = 'true'
),

-- denominator #2 (global): total teachers in the entire population (distinct contacts)
total_all_global AS (
  SELECT COUNT(DISTINCT contact_id) AS total_all
  FROM {{ ref('contact_chats_messages') }}
),


-- denominator per-week: number of opted-in teachers who had any activity that week
opted_in_active_weekly AS (
  SELECT
    week_start,
    COUNTIF(opted_in_flag_for_this_contact = 1) AS opted_in_with_activity_that_week
  FROM per_contact_week
  GROUP BY week_start
)



select
    w.week_start,
    w.active_teachers,

    -- global denominators
    t.total_opted_in,
    a.total_all,


    -- pct metrics
    SAFE_DIVIDE(w.active_teachers, t.total_opted_in) AS pct_active_of_total_opted_in_global,
    SAFE_DIVIDE(w.active_teachers, a.total_all)     AS pct_active_of_total_all_global,

    -- percent of active among opted-in who had activity that week
    o.opted_in_with_activity_that_week,
    SAFE_DIVIDE(w.active_teachers, o.opted_in_with_activity_that_week) 
    AS pct_active_of_opted_in_with_activity_that_week



from weekly_active w
cross join total_opted_in_global t
cross join total_all_global a
left join opted_in_active_weekly o using(week_start)

order by week_start desc