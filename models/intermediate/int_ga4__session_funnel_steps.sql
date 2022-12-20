{{
    config(
        materialized = 'incremental',
        partition_by = {
              "field": "session_reporting_date",
              "data_type": "date",
              "granularity": "day"
        },
        cluster_by = 'session_reporting_date'
    )
}}


SELECT
        int_ga4__session_reporting_date.ga_session_id,
        int_ga4__session_reporting_date.user_pseudo_id,
        int_ga4__session_reporting_date.unique_session_id,
        int_ga4__session_reporting_date.{{ var('ga4__session_reporting_date') }} AS session_reporting_date,
        {% for event_name in var('ga4__funnel_steps') %}
           CASE WHEN COUNT(CASE WHEN stg_ga4__flat_events.event_name = '{{event_name}}' THEN stg_ga4__flat_events.unique_session_id END) > 0 THEN TRUE ELSE FALSE END AS funnel_step_{{ loop.index }} {{ ", " if not loop.last else "" }}
        {% endfor %}

FROM
        {{ ref('stg_ga4__flat_events') }}

LEFT JOIN
        {{ ref('int_ga4__session_reporting_date') }}
   ON   stg_ga4__flat_events.unique_session_id = int_ga4__session_reporting_date.unique_session_id

        {% if is_incremental() %}

WHERE   1=1
  AND   stg_ga4__flat_events.event_date >= (SELECT MAX(session_reporting_date) FROM {{ this }})

        {% endif %}

GROUP BY
        1,2,3,4