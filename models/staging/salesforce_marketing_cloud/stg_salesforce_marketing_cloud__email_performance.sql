WITH

source_data as (

    SELECT * FROM {{ source('salesforce_marketing_cloud', 'salesforce_marketing_cloud_email_performance') }}

),

final AS (
  
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

)

SELECT * FROM final