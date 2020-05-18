WITH

--Facebook Paid View
fb_paid AS (
    
    SELECT * FROM {{ ref('facebook_ads__creatives_daily') }}

),
/*    
--Instagram Paid View
ig_paid AS (

    SELECT * FROM `yale-opac.instagram_organic.int_instagram_organic_post_lifetime`

),
  
--Twitter Paid View
tw_paid AS (

    SELECT * FROM `yale-opac.twitter_organic.int_twitter_organic_tweets_lifetime`

),

--LinkedIn Paid View
li_paid AS (

    SELECT * FROM `yale-opac.linkedin_organic.int_linkedin_organic_share_lifetime`

),
*/
--Union all social networks
  
final AS (
  
    SELECT * FROM fb_paid
    /*
    UNION ALL
    
    SELECT * FROM ig_paid
    
    UNION ALL
    
    SELECT * FROM tw_paid
    
    UNION ALL
    
    SELECT * FROM li_paid
    */
)

SELECT * FROM final