{{ config(enabled= get_account_ids('google analytics')|length > 0 is true) }}

WITH

data AS (
  
    SELECT * FROM {{ ref('stg_google_analytics__session_performance') }}

),
  
general_definitions AS (
    
    SELECT
    
        *,
        'Google Analytics' AS data_source
        
    FROM data

),
  
data_cleaning AS (

    SELECT
        
        data_source,
        account_id,
        account_name,
        date,
        TRIM(LOWER(utm_source)) AS utm_source,
        TRIM(LOWER(utm_medium)) AS utm_medium,
        TRIM(LOWER(utm_campaign)) AS utm_campaign,
        TRIM(LOWER(utm_content)) AS utm_content,
        TRIM(LOWER(utm_term)) AS utm_term,        
        user_type,
        device_category,
        
        TRIM(
            CASE
                WHEN STRPOS(landing_page_path, "?") > 0 THEN SUBSTR(landing_page_path, 0, STRPOS(landing_page_path, "?") - 1)
                WHEN STRPOS(landing_page_path, "#") > 0 THEN SUBSTR(landing_page_path, 0, STRPOS(landing_page_path, "#") - 1)
                ELSE landing_page_path
            END
        ) AS landing_page_path,
        
        website_sessions,
        website_session_duration,
        bounces,
        page_views,
        transactions,
        transaction_revenue,
        adds_to_cart,
        goal_1_completions,
        goal_2_completions,
        goal_3_completions,
        goal_4_completions,
        goal_5_completions,
        goal_6_completions,
        goal_7_completions,
        goal_8_completions,
        goal_9_completions,
        goal_10_completions,
        goal_11_completions,
        goal_12_completions,
        goal_13_completions,
        goal_14_completions,
        goal_15_completions,
        goal_16_completions,
        goal_17_completions,
        goal_18_completions,
        goal_19_completions,
        goal_20_completions,
        total_goal_completions,
        goal_1_value,
        goal_2_value,
        goal_3_value,
        goal_4_value,
        goal_5_value,
        goal_6_value,
        goal_7_value,
        goal_8_value,
        goal_9_value,
        goal_10_value,
        goal_11_value,
        goal_12_value,
        goal_13_value,
        goal_14_value,
        goal_15_value,
        goal_16_value,
        goal_17_value,
        goal_18_value,
        goal_19_value,
        goal_20_value
        
     FROM general_definitions

),

identify_social_referral_sessions AS (

    SELECT
    
        *,
        
        CASE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(facebook)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(^t.co$|twitter)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(instagram)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(linkedin)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(youtube)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(pinterest)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(reddit)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(ycombinator)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(meetedgar)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(lnkd.in)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(blogspot)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(blogger)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(wordpress)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(instapaper)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(naver)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(plurk)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(quora)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(yammer)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(slashdot)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(vk\.com)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(typepad)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(plus\.url\.google)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(getpocket)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(paper\.li)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(groups\.google\.com)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(buzzfeed)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(tumblr)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(scoop\.it)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(meetup)') THEN TRUE
            WHEN utm_medium = 'referral' AND REGEXP_CONTAINS(utm_source, r'(weebly)') THEN TRUE
            ELSE FALSE
        END AS is_social_referral_session
    
    FROM data_cleaning

),

define_channel_name AS (

    SELECT
    
        *,
        
        CASE
            WHEN utm_source = '(direct)' AND (utm_medium = '(not set)' OR utm_medium = '(none)') THEN 'Direct'
            WHEN utm_medium = 'organic' THEN 'Organic Search'
            WHEN utm_medium = 'paid_social' THEN 'Paid Social'
            WHEN is_social_referral_session = TRUE OR REGEXP_CONTAINS(utm_medium, r'^(social|social-network|social-media|sm|social network|social media)$') THEN 'Organic Social'
            WHEN utm_medium = 'email' OR (utm_source = 'email' AND utm_medium = 'abstract') THEN 'Email'
            WHEN utm_medium = 'affiliate' THEN 'Affiliates'
            WHEN utm_medium = 'referral' THEN 'Referral'
            WHEN REGEXP_CONTAINS(utm_medium, r'^(cpc|ppc|paidsearch)$') THEN 'Paid Search'
            WHEN REGEXP_CONTAINS(utm_medium, r'^(cpv|cpa|cpp|content-text)$') THEN 'Other Advertising'
            WHEN REGEXP_CONTAINS(utm_medium, r'^(display|cpm|banner)$') THEN 'Display'
            ELSE '(Other)'
        END AS channel_name
    
    FROM identify_social_referral_sessions

),

define_channel_dimensions AS (

    SELECT
    
        *,
        
        CASE
            WHEN channel_name = 'Email' THEN 'Email'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(facebook)') THEN 'Facebook'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(^t.co$|twitter)') THEN 'Twitter'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(instagram)') THEN 'Instagram'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(linkedin)') THEN 'LinkedIn'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(youtube)') THEN 'YouTube'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(pinterest)') THEN 'Pinterest'
            WHEN channel_name = 'Organic Social' AND REGEXP_CONTAINS(utm_source, r'(meetup)') THEN 'Meetup'
            WHEN channel_name = '(Other)' THEN '(Other)'
            ELSE utm_source
        END AS channel_source_name,
        
        CASE
            WHEN channel_name IN ('Email', 'Paid Social', 'Paid Search') THEN 'Paid'
            WHEN channel_name = '(Other)' THEN '(Other)'
            ELSE 'Organic'
        END AS channel_source_type,
    
    FROM define_channel_name

),

aggregate AS (

    SELECT
    
        data_source,
        account_id,
        account_name,
        date,
        channel_name,
        channel_source_name,
        channel_source_type,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,        
        user_type,
        device_category,
        landing_page_path,
        
        SUM(website_sessions) AS website_sessions,
        SUM(website_session_duration) AS website_session_duration,
        SUM(bounces) AS bounces,
        SUM(page_views) AS page_views,
        SUM(transactions) AS transactions,
        SUM(transaction_revenue) AS transaction_revenue,
        SUM(adds_to_cart) AS adds_to_cart,
        SUM(goal_1_completions) AS goal_1_completions,
        SUM(goal_2_completions) AS goal_2_completions,
        SUM(goal_3_completions) AS goal_3_completions,
        SUM(goal_4_completions) AS goal_4_completions,
        SUM(goal_5_completions) AS goal_5_completions,
        SUM(goal_6_completions) AS goal_6_completions,
        SUM(goal_7_completions) AS goal_7_completions,
        SUM(goal_8_completions) AS goal_8_completions,
        SUM(goal_9_completions) AS goal_9_completions,
        SUM(goal_10_completions) AS goal_10_completions,
        SUM(goal_11_completions) AS goal_11_completions,
        SUM(goal_12_completions) AS goal_12_completions,
        SUM(goal_13_completions) AS goal_13_completions,
        SUM(goal_14_completions) AS goal_14_completions,
        SUM(goal_15_completions) AS goal_15_completions,
        SUM(goal_16_completions) AS goal_16_completions,
        SUM(goal_17_completions) AS goal_17_completions,
        SUM(goal_18_completions) AS goal_18_completions,
        SUM(goal_19_completions) AS goal_19_completions,
        SUM(goal_20_completions) AS goal_20_completions,
        SUM(total_goal_completions) AS total_goal_completions,
        SUM(goal_1_value) AS goal_1_value,
        SUM(goal_2_value) AS goal_2_value,
        SUM(goal_3_value) AS goal_3_value,
        SUM(goal_4_value) AS goal_4_value,
        SUM(goal_5_value) AS goal_5_value,
        SUM(goal_6_value) AS goal_6_value,
        SUM(goal_7_value) AS goal_7_value,
        SUM(goal_8_value) AS goal_8_value,
        SUM(goal_9_value) AS goal_9_value,
        SUM(goal_10_value) AS goal_10_value,
        SUM(goal_11_value) AS goal_11_value,
        SUM(goal_12_value) AS goal_12_value,
        SUM(goal_13_value) AS goal_13_value,
        SUM(goal_14_value) AS goal_14_value,
        SUM(goal_15_value) AS goal_15_value,
        SUM(goal_16_value) AS goal_16_value,
        SUM(goal_17_value) AS goal_17_value,
        SUM(goal_18_value) AS goal_18_value,
        SUM(goal_19_value) AS goal_19_value,
        SUM(goal_20_value) AS goal_20_value
    
    FROM define_channel_dimensions
    
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

),

final AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key(['date', 'account_id', 'channel_name', 'channel_source_name', 'channel_source_type', 'utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'user_type', 'device_category', 'landing_page_path']) }} AS id,
        *

    FROM aggregate
)
      
SELECT * FROM final