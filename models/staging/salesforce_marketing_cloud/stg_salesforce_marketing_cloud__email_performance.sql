WITH

source_data as (

    SELECT * FROM {{ source('salesforce_marketing_cloud', 'salesforce_marketing_cloud_email_performance') }}

),

recast AS (
  
    SELECT 
    
        *
        
        REPLACE (
            CAST(date AS DATE) AS date,
            
            CASE
               WHEN time_of_day = 0 THEN '00-04'
               WHEN time_of_day = 1 THEN '04-08'
               WHEN time_of_day = 2 THEN '08-12'
               WHEN time_of_day = 3 THEN '12-16'
               WHEN time_of_day = 4 THEN '16-20'
               WHEN time_of_day = 5 THEN '20-24'
            END AS time_of_day,
            
            DATE(
                PARSE_TIMESTAMP(
                    '%a %b %d %T %z %Y', 
                    CASE
                        WHEN REGEXP_CONTAINS(email_original_send_date, r'EST') = TRUE THEN REPLACE(email_original_send_date, ' EST', '-0500')
                        WHEN REGEXP_CONTAINS(email_original_send_date, r'EDT') = TRUE THEN REPLACE(email_original_send_date, ' EDT', '-0400')
                    END)
                , 'America/New_York') 
            AS email_original_send_date
        )
    
    FROM source_data

),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'time_of_day', 'audience_segment', 'email_id']) }} AS id,
        date,
        time_of_day,
        audience_segment,
        email_original_send_date,
        email_id,
        email_name,
        email_subject,
        email_preview_url,
        
        SUM(opens) AS opens,
        SUM(unsubscribes) AS unsubscribes,
        SUM(bounces) AS bounces,
        SUM(sends) AS sends,
        SUM(clicks) AS clicks,
        SUM(clicks_unique) AS clicks_unique,
        SUM(opens_unique) AS opens_unique

    FROM recast

    GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT * FROM final