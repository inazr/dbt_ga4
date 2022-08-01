WITH get_row_numbers_for_session_events AS (

    SELECT
            stg_ga4__event_params.ga_session_id,
            stg_ga4__event_params.event_timestamp,
            stg_ga4__event_params.event_name,
            stg_ga4__event_params.string_value,
            ROW_NUMBER() over (PARTITION BY stg_ga4__event_params.ga_session_id ORDER BY stg_ga4__event_params.event_timestamp ASC) as row_number_per_session_id,
    FROM
            {{ ref('stg_ga4__event_params') }}
    WHERE   1=1
      AND   stg_ga4__event_params.key = 'page_location'

)

SELECT
        ga_session_id,
        string_value AS landing_page,
FROM
        get_row_numbers_for_session_events
WHERE   1=1
  AND   row_number_per_session_id = 1
