SELECT
        user_pseudo_id,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        event_name,
        items.item_id,
        items.item_name,
        items.item_brand,
        items.item_variant,
        items.item_category,
        items.item_category2,
        items.item_category3,
        items.item_category4,
        items.item_category5,
        items.price_in_usd,
        items.price,
        items.quantity,
        items.item_revenue_in_usd,
        items.item_revenue,
        items.item_refund_in_usd,
        items.item_refund,
        items.coupon,
        items.affiliation,
        items.location_id,
        items.item_list_id,
        items.item_list_name,
        items.item_list_index,
        items.promotion_id,
        items.promotion_name,
        items.creative_name,
        items.creative_slot,
        TO_HEX(SHA256(CONCAT(user_pseudo_id, event_timestamp, event_name, row_number() OVER (PARTITION BY user_pseudo_id, event_timestamp, event_name)))) AS join_key,
FROM
        {{ source('ga4', 'events') }}

CROSS JOIN
        unnest(items)
        AS items

WHERE   1=1
  AND   _table_suffix >= CAST(TIMESTAMP '{{ var('ga4__current_date') }}' - INTERVAL {{ var('ga4__look_back_window_days') }} DAY AS STRING FORMAT 'YYYYMMDD')
