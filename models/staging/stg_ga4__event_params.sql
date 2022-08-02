WITH event_params_cte AS (

    SELECT
            user_pseudo_id,
            case when event_params.key = 'ga_session_id' then event_params.value.int_value else null end as ga_session_id,
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

    WHERE   1=1
      AND   _table_suffix >= CAST(TIMESTAMP '{{ var('ga4__current_date') }}' - INTERVAL {{ var('ga4__look_back_window_days') }} DAY AS STRING FORMAT 'YYYYMMDD')

)


SELECT
        MAX(ga_session_id) OVER (PARTITION BY event_timestamp, event_name) AS ga_session_id,
        user_pseudo_id,
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