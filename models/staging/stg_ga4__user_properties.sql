SELECT
        user_pseudo_id,
        (SELECT VALUE.int_value FROM unnest(event_params) WHERE KEY = 'ga_session_id') AS ga_session_id,
        user_pseudo_id||'.'||(SELECT VALUE.int_value FROM unnest(event_params) WHERE KEY = 'ga_session_id') AS unique_session_id,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        user_properties,
        user_properties.key,
        user_properties.value.string_value,
        user_properties.value.int_value,
        user_properties.value.float_value,
        user_properties.value.double_value,
        TO_HEX(SHA256(CONCAT(user_pseudo_id, event_timestamp, event_name, row_number() OVER (PARTITION BY user_pseudo_id, event_timestamp, event_name)))) AS join_key,
FROM
        {{ source('ga4', 'events') }}

CROSS JOIN
        unnest(user_properties)
        AS user_properties

{% if not flags.FULL_REFRESH %}

WHERE   1=1
  AND   _table_suffix >= CAST(TIMESTAMP '{{ var('ga4__current_date') }}' - INTERVAL {{ var('ga4__look_back_window_days') }} DAY AS STRING FORMAT 'YYYYMMDD')

{% endif %}