SELECT
        user_pseudo_id,
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

WHERE   1=1
  AND   _table_suffix > '20201101'