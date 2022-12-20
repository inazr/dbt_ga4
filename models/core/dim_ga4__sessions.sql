{{
    config(

        materialized='incremental',
        partition_by={
              "field": "session_reporting_date",
              "data_type": "date",
              "granularity": "day"
        },
        cluster_by = "session_reporting_date",
    )
}}


WITH session_data_from_events_cte AS (

    SELECT
            int_ga4__session_reporting_date.{{ var('ga4__session_reporting_date') }} AS session_reporting_date,
            stg_ga4__flat_events.ga_session_id,
            stg_ga4__flat_events.user_pseudo_id,
            stg_ga4__flat_events.unique_session_id,
            MIN(stg_ga4__flat_events.event_timestamp) AS session_first_event_timestamp,
            MAX(stg_ga4__flat_events.event_timestamp) AS session_last_event_timestamp,
            COUNT(stg_ga4__flat_events.ga_session_id) AS number_of_events,
    FROM
            {{ ref('stg_ga4__flat_events') }}

    LEFT JOIN
            {{ ref('int_ga4__session_reporting_date') }}
       ON   stg_ga4__flat_events.unique_session_id = int_ga4__session_reporting_date.unique_session_id

    GROUP BY
            1,2,3,4


)


SELECT
        session_data_from_events_cte.ga_session_id,
        session_data_from_events_cte.user_pseudo_id,
        session_data_from_events_cte.unique_session_id,
        session_data_from_events_cte.session_reporting_date,
        session_data_from_events_cte.session_first_event_timestamp,
        session_data_from_events_cte.session_last_event_timestamp,
        session_data_from_events_cte.number_of_events,
        int_ga4__session_default_channel_grouping.source,
        int_ga4__session_default_channel_grouping.medium,
        int_ga4__session_default_channel_grouping.campaign,
        int_ga4__session_default_channel_grouping.default_channel_grouping,
        int_ga4__session_landing_pages.landing_page,

        {% for event_name in var('ga4__funnel_steps') %}
           int_ga4__session_funnel_steps.funnel_step_{{ loop.index }},
        {% endfor %}
FROM
        session_data_from_events_cte

LEFT JOIN
        {{ref('int_ga4__session_default_channel_grouping')}}
   ON   session_data_from_events_cte.unique_session_id = int_ga4__session_default_channel_grouping.unique_session_id

LEFT JOIN
        {{ref('int_ga4__session_funnel_steps')}}
   ON   int_ga4__session_default_channel_grouping.unique_session_id = int_ga4__session_funnel_steps.unique_session_id

LEFT JOIN
        {{ref('int_ga4__session_landing_pages')}}
   ON   int_ga4__session_default_channel_grouping.unique_session_id = int_ga4__session_landing_pages.unique_session_id
