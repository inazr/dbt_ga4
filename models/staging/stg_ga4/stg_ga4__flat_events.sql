-- noinspection SqlNoDataSourceInspection

SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
        event_name,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info.analytics_storage,
        privacy_info.ads_storage,
        privacy_info.uses_transient_token,
        TIMESTAMP_MICROS(user_first_touch_timestamp) AS user_first_touch_timestamp,
        user_ltv.revenue AS user_ltv_revenue,
        user_ltv.currency AS user_ltv_currency,
        device.category,
        device.mobile_brand_name,
        device.mobile_model_name,
        device.mobile_marketing_name,
        device.mobile_os_hardware_model,
        device.operating_system,
        device.operating_system_version,
        device.vendor_id,
        device.advertising_id,
        device.language,
        device.is_limited_ad_tracking,
        device.time_zone_offset_seconds,
        device.web_info.browser,
        device.web_info.browser_version,
        geo.continent,
        geo.sub_continent,
        geo.country,
        geo.region,
        geo.city,
        geo.metro,
        app_info,
        traffic_source.medium,
        traffic_source.name,
        traffic_source.source,
        stream_id,
        platform,
        event_dimensions,
        ecommerce.total_item_quantity,
        ecommerce.purchase_revenue_in_usd,
        ecommerce.purchase_revenue,
        ecommerce.refund_value_in_usd,
        ecommerce.refund_value,
        ecommerce.shipping_value_in_usd,
        ecommerce.shipping_value,
        ecommerce.tax_value_in_usd,
        ecommerce.tax_value,
        ecommerce.unique_items,
        ecommerce.transaction_id,
        TO_HEX(SHA256(concat(user_pseudo_id,event_timestamp,event_name,row_number() over(partition by user_pseudo_id, event_timestamp, event_name)))) as join_key
FROM
        {{ source('ga4', 'events') }}

WHERE   1=1
  AND   _table_suffix >=  '20201101'