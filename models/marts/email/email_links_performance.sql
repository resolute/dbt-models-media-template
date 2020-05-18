WITH

data AS (
  
    SELECT * FROM {{ ref('stg_salesforce_marketing_cloud__email_links_performance') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'Salesforce Marketing Cloud' AS data_source,
        'Email' AS channel_source_name,
        'Paid' AS channel_source_type,
        'Email' AS channel_name
        
    FROM data

),
  
rename_columns_and_set_defaults AS (

    SELECT
    
        data_source,
        '(not set)' AS account_id,
        '(not set)' AS account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        time_of_day,
        audience_segment,
        email_original_send_date,
        email_id,
        email_name,
        email_subject,
        email_preview_url,
        email_link_url,
        
        clicks,
        clicks_unique
        
     FROM general_definitions

),

calculate_email_age AS (

    SELECT
        
        *,
        DATE_DIFF(date, email_original_send_date, DAY) AS email_age_in_days,
    
    FROM rename_columns_and_set_defaults

),

final AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        channel_source_name,
        channel_source_type,
        channel_name,
        date,
        time_of_day,
        audience_segment,
        email_original_send_date,
        email_id,
        email_name,
        email_subject,
        email_preview_url,
        email_link_url,
        email_age_in_days,
        
        SUM(clicks) AS clicks,
        SUM(clicks_unique) AS clicks_unique
    
    FROM calculate_email_age
    
    --Include emails for Yale Today newsletter
    WHERE email_original_send_date >= '2020-01-12'
    
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

)
      
SELECT * FROM final