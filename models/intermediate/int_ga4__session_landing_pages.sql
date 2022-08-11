{{
    config(
        materialized = 'incremental',
        partition_by = {
              "field": "session_start_date",
              "data_type": "date",
              "granularity": "day"
        },
        cluster_by = 'session_start_date'
    )
}}

WITH get_row_numbers_for_session_events AS (

    SELECT
            stg_ga4__event_params.ga_session_id,
            int_ga4__session_start_date.session_start_date,
            stg_ga4__event_params.event_timestamp,
            stg_ga4__event_params.string_value,
            ROW_NUMBER() over (PARTITION BY stg_ga4__event_params.ga_session_id ORDER BY stg_ga4__event_params.event_timestamp ASC) as row_number_per_session_id,
    FROM
            {{ ref('stg_ga4__event_params') }}

    LEFT JOIN
            {{ ref('int_ga4__session_start_date') }}
            ON stg_ga4__event_params.ga_session_id = int_ga4__session_start_date.ga_session_id

    WHERE   1=1
      AND   stg_ga4__event_params.key = 'page_location'

    {% if is_incremental() %}

      AND   stg_ga4__event_params.event_date >= (SELECT MAX(session_start_date) FROM {{ this }})

    {% endif %}

)

SELECT
        ga_session_id,
        session_start_date,
        string_value AS landing_page,
FROM
        get_row_numbers_for_session_events

WHERE   1=1
  AND   row_number_per_session_id = 1

GROUP BY
        1,2,3


