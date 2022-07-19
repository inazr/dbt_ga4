--https://support.google.com/analytics/answer/9756891?hl=en

{{ config(

        materialized = 'incremental',
        partition_by = {
              "field": "DATE(session_first_event)",
              "data_type": "date",
              "granularity": "day"
        },
        cluster_by = "session_first_event",
)}}


SELECT
        stg_ga4__flat_events.user_pseudo_id,
        stg_ga4__flat_events.ga_session_id,
        MIN(stg_ga4__flat_events.event_timestamp) AS session_first_event,
        MAX(stg_ga4__flat_events.event_timestamp) AS session_last_event,
        stg_ga4__flat_events.source,
        stg_ga4__flat_events.medium,
        stg_ga4__event_params.string_value AS campaign,
        CASE    WHEN stg_ga4__flat_events.source = '(direct)'
                 AND ( stg_ga4__flat_events.medium = '(none)' OR stg_ga4__flat_events.medium = '(not set)')
                THEN 'Direct'

                WHEN stg_ga4__event_params.string_value LIKE '%cross-network%'
                THEN 'Cross-network'

--                 WHEN REGEXP_CONTAINS(stg_ga4__event_params.string_value, r'^(.*(([^a-df-z]|^)shop|shopping).*)$)' )
--                  AND REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*cp.*|ppc|paid.*)$')
--                 THEN 'Paid Shopping'

                WHEN stg_ga4__flat_events.source IN ('google')
                 AND REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*cp.*|ppc|paid.*)$')
                THEN 'Paid Search'

                WHEN stg_ga4__flat_events.source IN ('facebook')
                 AND REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*cp.*|ppc|paid.*)$')
                THEN 'Paid Social'

                WHEN stg_ga4__flat_events.source IN ('youtube')
                 AND REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*cp.*|ppc|paid.*)$')
                THEN 'Paid Video'

                WHEN stg_ga4__flat_events.medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm')
                THEN 'Display'

                 WHEN stg_ga4__flat_events.source IN ('amazon')
                   OR REGEXP_CONTAINS(stg_ga4__event_params.string_value, r'^(.*(([^a-df-z]|^)shop|shopping).*)$' )
                 THEN 'Organic Shopping'

                WHEN stg_ga4__flat_events.source IN ('facebook')
                  OR stg_ga4__flat_events.medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media')
                THEN 'Organic Social'

                WHEN stg_ga4__flat_events.source IN ('youtube')
                  OR REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*video.*)$')
                THEN 'Organic Video'

                WHEN stg_ga4__flat_events.source IN ('google')
                 AND stg_ga4__flat_events.medium = 'organic'
                THEN 'Organic Search'

                WHEN REGEXP_CONTAINS(stg_ga4__flat_events.source, r'^(.*e.mail.*)$')
                  OR REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*e.mail.*)$')
                THEN 'Email'

                WHEN stg_ga4__flat_events.medium = 'affiliate'
                THEN 'Affiliates'

                WHEN stg_ga4__flat_events.medium = 'referral'
                THEN 'Referral'

                WHEN stg_ga4__flat_events.medium = 'audio'
                THEN 'Audio'

                WHEN stg_ga4__flat_events.medium = 'sms'
                THEN 'SMS'

                WHEN stg_ga4__flat_events.medium LIKE '%push'
                  OR stg_ga4__flat_events.medium LIKE '%mobile%'
                  OR stg_ga4__flat_events.medium LIKE '%notification%'
                THEN 'Mobile Push Notifications'


        END AS default_channel_grouping
FROM
--         stg_ga4.stg_ga4__flat_events
        {{ref('stg_ga4__flat_events')}}
LEFT JOIN
--         stg_ga4.stg_ga4__event_params
        {{ref('stg_ga4__event_params')}}
        ON stg_ga4__flat_events.join_key = stg_ga4__event_params.join_key
        AND stg_ga4__event_params.key = 'campaign'

        {% if is_incremental() %}
WHERE
        stg_ga4__flat_events.event_date >= ( SELECT MAX( DATE( session_first_event ) ) FROM {{ this }} )

        {% endif %}

GROUP BY
        1,2,5,6,7,8
