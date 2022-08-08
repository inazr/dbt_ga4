{{
    config(

        materialized='incremental',
        partition_by={
              "field": "event_month",
              "data_type": "date",
              "granularity": "month"
        },
        cluster_by = "event_month",
    )
}}


SELECT
        DATE_TRUNC( stg_ga4__flat_events.event_date, MONTH ) AS event_month,
        stg_ga4__flat_events.category,
        stg_ga4__flat_events.operating_system,
        stg_ga4__flat_events.mobile_brand_name,
        stg_ga4__flat_events.browser,
        stg_ga4__flat_events.medium,
        stg_ga4__flat_events.source,
        COUNT( DISTINCT stg_ga4__flat_events.ga_session_id )    AS sessions,
        COUNT( DISTINCT stg_ga4__flat_events.user_pseudo_id )   AS users
FROM
        {{ ref('stg_ga4__flat_events') }}

        {% if is_incremental() %}

WHERE   1=1
  AND   stg_ga4__flat_events.event_date >= ( SELECT MAX( event_month ) FROM {{ this }} )

        {% endif %}

GROUP BY
        1,2,3,4,5,6,7