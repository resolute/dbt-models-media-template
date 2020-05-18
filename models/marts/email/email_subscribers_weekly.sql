WITH

data AS (
  
    SELECT * FROM {{ ref('email_performance') }}

),

final AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        DATE_TRUNC(email_original_send_date, WEEK) AS week,
        audience_segment,
        
        MAX(sends) AS email_subscriber_total
    
    FROM data
    
    GROUP BY 1,2,3,4,5,6,7,8

)

SELECT * FROM final