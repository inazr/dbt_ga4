{{
    config(
        materialized = 'incremental',
        partition_by = {
              "field": "event_date",
              "data_type": "date",
              "granularity": "day"
        },
        cluster_by = 'event_date'
    )
}}


SELECT
        stg_ga4__flat_events.ga_session_id,
        stg_ga4__flat_events.event_date,
        {% for event_name in var('ga4__funnel_steps') %}
           CASE WHEN COUNT(CASE WHEN stg_ga4__flat_events.event_name = '{{event_name}}' THEN stg_ga4__flat_events.ga_session_id END) > 0 THEN TRUE ELSE FALSE END AS funnel_step_{{ loop.index }} {{ ", " if not loop.last else "" }}
        {% endfor %}

FROM
--         stg_ga4.stg_ga4__flat_events
        {{ref('stg_ga4__flat_events')}}

        {% if is_incremental() %}

WHERE   1=1
  AND   stg_ga4__flat_events.event_date >= (SELECT MAX(event_date) FROM {{ this }})

        {% endif %}

GROUP BY
        1,2