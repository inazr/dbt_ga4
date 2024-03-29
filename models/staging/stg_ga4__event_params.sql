WITH event_params_cte AS (

    SELECT
            user_pseudo_id,
            CASE WHEN event_params.key = 'ga_session_id' THEN event_params.value.int_value ELSE NULL END AS ga_session_id,
            PARSE_DATE('%Y%m%d', event_date) AS event_date,
            event_timestamp,
            event_name,
            event_params.key,
            event_params.value.string_value,
            event_params.value.int_value,
            event_params.value.float_value,
            event_params.value.double_value,
    FROM
            {{ source('ga4', 'events') }}

    CROSS JOIN
            unnest(event_params)
            AS event_params

{% if not flags.FULL_REFRESH %}

    WHERE   1=1
      AND   _table_suffix >= CAST(TIMESTAMP '{{ var('ga4__current_date') }}' - INTERVAL {{ var('ga4__look_back_window_days') }} DAY AS STRING FORMAT 'YYYYMMDD')

{% endif %}

)


SELECT
        user_pseudo_id,
        MAX(ga_session_id) OVER (PARTITION BY event_timestamp, event_name) AS ga_session_id,
        user_pseudo_id||'.'||MAX(ga_session_id) OVER (PARTITION BY event_timestamp, event_name) AS unique_session_id,
        event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        event_name,
        key,
        string_value,
        int_value,
        float_value,
        double_value,
        TO_HEX(SHA256(CONCAT(user_pseudo_id, event_timestamp, event_name, row_number() OVER (PARTITION BY user_pseudo_id, event_timestamp, event_name)))) AS join_key,
FROM
        event_params_cte