{%- set dict = {
    'platform': 'facebook', 'has_data': var('facebook_organic_ids') != None, 'model_ref': 'facebook_organic__followers_daily',
    'platform': 'instagram', 'has_data': var('instagram_organic_ids') != None, 'model_ref': 'instagram_organic__followers_daily',
    'platform': 'twitter', 'has_data': var('twitter_organic_ids') != None, 'model_ref': 'twitter_organic__followers_daily',
    'platform': 'linkedin', 'has_data': var('linkedin_organic_ids') != None, 'model_ref': 'linkedin_organic__followers_daily'
    } 

-%}

{%- set list = [
    (var('facebook_organic_ids') != None, 'facebook_organic__followers_daily'),
    (var('instagram_organic_ids') != None, 'instagram_organic__followers_daily'),
    (var('twitter_organic_ids') != None, 'twitter_organic__followers_daily'),
    (var('linkedin_organic_ids') != None, 'linkedin_organic__followers_daily')
    ]

-%}

{%- for item in dict -%}

    
    {{dict["platform"]}}

{%- endfor -%}

{{-
    config(
        enabled = false == true
    )
-}}

WITH

--Facebook Organic
fb_organic AS (
    
    SELECT * FROM {{ ref('facebook_organic__followers_daily') }}

),
    
--Instagram Organic
ig_organic AS (

    SELECT * FROM {{ ref('instagram_organic__followers_daily') }}

),

{% if twitter_ids %}
--Twitter Organic
tw_organic AS (

    SELECT * FROM {{ ref('twitter_organic__followers_daily') }}

),
{% endif %}

--LinkedIn Organic
li_organic AS (

    SELECT * FROM {{ ref('linkedin_organic__followers_daily') }}

),

--Union all social networks
  
union_tables AS (
  
    SELECT * FROM fb_organic

    UNION ALL
    
    SELECT * FROM ig_organic
    
    UNION ALL
    
    {% if twitter_ids %}
    SELECT * FROM tw_organic
    
    UNION ALL
    {% endif %}
    
    SELECT * FROM li_organic
    
),

final AS (

    SELECT

        {{ dbt_utils.surrogate_key(['date', 'account_id']) }} AS id,
        *
    
    FROM union_tables
)

SELECT * FROM final