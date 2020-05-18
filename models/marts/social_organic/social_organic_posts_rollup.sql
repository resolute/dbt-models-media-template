WITH

--Facebook Organic
fb_organic AS (
    
    SELECT * FROM {{ ref('facebook_organic__posts_lifetime') }}

),
    
--Instagram Organic
ig_organic AS (

    SELECT * FROM {{ ref('instagram_organic__posts_lifetime') }}

),
  
--Twitter Organic
tw_organic AS (

    SELECT * FROM {{ ref('twitter_organic__posts_lifetime') }}

),

--LinkedIn Organic
li_organic AS (

    SELECT * FROM {{ ref('linkedin_organic__posts_lifetime') }}

),

--Union all social networks
  
final AS (
  
    SELECT * FROM fb_organic

    UNION ALL
    
    SELECT * FROM ig_organic
    
    UNION ALL
    
    SELECT * FROM tw_organic
    
    UNION ALL
    
    SELECT * FROM li_organic
    
)

SELECT * FROM final