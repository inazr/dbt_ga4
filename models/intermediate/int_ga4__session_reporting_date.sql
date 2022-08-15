/*
 This model is needed to create a single date for the session start. As we only load the last X days into the staging layer
 there is a risk for creating doublicate sessions if a session has events before and after the dateline.
 */


{{
    config(
        materialized = 'incremental',
        partition_by = {
              "field": "session_start_date",
              "data_type": "date",
              "granularity": "day"
        },
        cluster_by = 'ga_session_id'
    )
}}

SELECT
        ga_session_id,
        MIN(event_date) AS session_start_date,
        MAX(event_date) AS session_end_date,
        CURRENT_DATE AS _load_date,
FROM
        {{ ref('stg_ga4__flat_events') }}

WHERE   1=1

{% if is_incremental() %}

  AND   stg_ga4__flat_events.ga_session_id NOT IN (SELECT ga_session_id FROM {{ this }})

{% endif %}

GROUP BY
        1