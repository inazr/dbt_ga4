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
        ga4_obfuscated_sample_ecommerce.events_20201101
CROSS JOIN
        unnest(user_properties)
        AS user_properties