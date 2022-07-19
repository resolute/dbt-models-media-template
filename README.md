# Resolute Digital standard dbt models

This package models data from Improvado's BigQuery extraction templates.

----
## Contents

**[Models](#models)**
  - [Google Campaign Manager](#google-campaign-manager)
  - [Google Ads](#google-ads)
  - [Facebook Ads](#facebook-ads)
  - [LinkedIn Ads](#linkedin-ads)
  - [Twitter Ads](#twitter-ads)
  - [Pinterest Ads](#pinterest-ads)
  - [Social Organic Media Posts](#social-organic-media-posts)
  - [Social Organic Media Followers](#social-organic-media-followers)
  - [YouTube Organic](#youtube-organic)
  - [Google Analytics](#google-analytics)

**[Installation Instructions](#installation-instructions)**

**[Configuration](#configuration)**
  - [Data Source Account IDs](#data-source-account-ids)
  - [Data Source Custom Conversions Enabled](#data-source-custom-conversions-enabled)
  - [Location of Improvado Source Tables](#location-of-improvado-source-tables)

----
## Models

This package contains transformation models, designed to be starting models for any Resolute Digital reporting. The primary outputs of this package are described below.

### Google Campaign Manager
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| google_campaign_manager__performance_daily | Each record represents the daily performance of each Google Campaign Manager creative, ad, and placement. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Google Campaign Manager (by Advertisers) | Ads Creatives Placements | google_cm_ads_creatives_placements |
| Google Campaign Manager (by Advertisers) | Ads Placement Sites (MCDM) | google_cm_ads_placement_sites |

### Google Ads
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| google_ads__performance_daily | Each record represents the daily performance of each Google Ads ad. |
| google_ads__keyword_performance_daily | Each record represents the daily performance of each Google Ads keyword. |
| google_ads__search_query_performance_daily | Each record represents the daily performance of each Google Ads search query. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Google Ads | Ads (MCDM) | google_ads_ads |
| Google Ads | Ads Device Conversions | google_ads_ads_device_conversions |
| Google Ads | Keywords Extended | google_ads_keywords_extended |
| Google Ads | Keywords Conversions | google_ads_keywords_conversions |
| Google Ads | Search Query Keywords | google_ads_search_query_keywords |
| Google Ads | Search Query Conversions | google_ads_search_query_conversions |

### Facebook Ads
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| facebook_ads__ads_performance_daily | Each record represents the daily performance of each Facebook Ads ad. |
| facebook_ads__creatives_performance_daily | Each record represents the daily performance of each Facebook Ads creative. |
| facebook_ads__campaign_month_reach | Each record represents the monthly performance of each Facebook Ads campaign with Reach metric. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Facebook | Ads Placements | facebook_ads_placements |
| Facebook | Ads Creative Platform | facebook_ads_creative_platform |
| Facebook | Campaign Month Reach | facebook_campaign_month_reach |
| Facebook | Entity Campaigns (MCDM) | facebook_entity_campaigns |
| Facebook | Entity Adsets (MCDM) | facebook_entity_adsets |
| Facebook | Entity Ads (MCDM) | facebook_entity_ads |
| Facebook | Entity Creatives (MCDM) | facebook_entity_creatives |

### LinkedIn Ads
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| linkedin_ads__performance_daily | Each record represents the daily performance of each LinkedIn Ads creative. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| LinkedIn Ads | Creatives (MCDM) | linkedin_ads_creatives |
| LinkedIn Ads | Creative With Conversions | linkedin_ads_creative_with_conversions |

### Twitter Ads
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| twitter_ads__promoted_tweets_performance_daily | Each record represents the daily performance of each Twitter Ads promoted tweet. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Twitter | Promoted Tweets With Cards | twitter_promoted_tweets_with_cards |

### Pinterest Ads
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| pinterest_ads__pins_performance_daily | Each record represents the daily performance of each Pinterest Ads ad. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Pinterest Ads | Promoted Tweets With Cards | pinterest_ads_pins_1v_30en_30cl |

### Social Organic Media Posts
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| rollup_social_organic__posts_lifetime | Each record represents the lifetime performance of each social organic post for Facebook, Instagram, LinkedIn, and Twitter. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Facebook Pages | Post | facebook_pages_post |
| Instagram Organic | Post Lifetime (MCDM) | instagram_organic_post |
| LinkedIn Organic | Share Lifetime | linkedin_organic_share_lifetime |
| Twitter | Organic Tweets (MCDM) | twitter_organic_tweets |

### Social Organic Media Followers
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| rollup_social_organic__followers_daily | Each record represents the daily follower totals of each social organic account for Facebook, Instagram, LinkedIn, and Twitter. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Facebook Pages | Page (MCDM) | facebook_pages_page |
| Instagram Organic | Page Info (MCDM) | instagram_organic_page_info |
| LinkedIn Organic | Follower Count Lifetime Incremental | linkedin_organic_followers_lifetime_inc |
| Twitter | Page (MCDM) | twitter_page |

### YouTube Organic
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| youtube_organic__top_videos | Each record represents the daily performance of each YouTube channel video. |
| youtube_organic__traffic_source_daily | Each record represents the daily performance of each YouTube channel traffic source. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Youtube Organic | Top videos (MCDM) | youtube_organic_top_videos |
| Youtube Organic | Traffic source daily | youtube_organic_traffic_source_daily |

### Google Analytics
***Generated Tables:***
| Table Name | Description |
| ---------- | ----------- |
| website__session_performance | Each record represents the daily session performance from Google Analytics. |

***Required Improvado Extraction Templates:***
| Connection Data Source | Extraction Template Name | BigQuery Table Name |
| ---------------------- | ------------------------ | ------------------- |
| Google Analytics | Session Performance | ga_session_performance |

----
## Installation Instructions
[Read the dbt docs](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/#git-packages) for more information on installing git packages. Please add the following configuration to you `packages.yml` file, make sure to enter a correct revision:

```yml
packages:
  - git: "https://github.com/resolute/dbt-models-media-template.git"
    revision: "x.x.x"
```

----
## Configuration

### Data Source Account IDs
***(REQUIRED)***  
For each data source you need to populate the appropriate Improvado account_id, please add the following configuration:  
*Note: If both option 1 and 2 are populated, then option 1 will take precendence over option 2*

***Option 1***  
Define dbt Project environment variables. The format of each environment variable accepts a YAML list. [Read the dbt docs](https://docs.getdbt.com/docs/dbt-cloud/using-dbt-cloud/cloud-environment-variables) for more information on environment variables.
```
DBT_FACEBOOK_ADS_IDS = ['123','456']
DBT_FACEBOOK_ORGANIC_IDS = ['123','456']
DBT_GOOGLE_ADS_IDS = ['123','456']
DBT_GOOGLE_ANALYTICS_IDS = ['123','456']
DBT_GOOGLE_CAMPAIGN_MANAGER_IDS = ['123','456']
DBT_INSTAGRAM_ORGANIC_IDS = ['123','456']
DBT_LINKEDIN_ADS_IDS = ['123','456']
DBT_LINKEDIN_ORGANIC_IDS = ['123','456']
DBT_PINTEREST_ADS_IDS = ['123','456']
DBT_TWITTER_ADS_IDS = ['123','456']
DBT_TWITTER_ORGANIC_IDS = ['123','456']
DBT_YOUTUBE_ORGANIC_IDS = ['123','456']
```

***Option 2***  
Define dbt variables in your `dbt_project.yml` file.
```yml
vars:
  facebook_ads_ids: ['123','456']                # List of Facebook Ads Account IDs eg. ['123']
  facebook_organic_ids: ['123','456']            # List of Facebook Account IDs eg. ['123']
  google_ads_ids: ['123','456']                  # List of Google Ads Account IDs eg. ['123']
  google_analytics_ids: ['123','456']            # List of Google Analytics View IDs eg. ['123']
  google_campaign_manager_ids: ['123','456']     # List of Google Campaign Manager Advertiser IDs eg. ['123']
  instagram_organic_ids: ['123','456']           # List of Instagram Account IDs eg. ['123']
  linkedin_ads_ids: ['123','456']                # List of LinkedIn Ads Account IDs eg. ['123']
  linkedin_organic_ids: ['123','456']            # List of LinkedIn Account IDs eg. ['123']
  pinterest_ads_ids: ['123','456']               # List of Pinterest Account IDs eg. ['123']
  twitter_ads_ids: ['123','456']                 # List of Twitter Ads Account IDs eg. ['123']
  twitter_organic_ids: ['123','456']             # List of Twitter Account IDs eg. ['123']
  youtube_organic_ids: ['123','456']             # List of YouTube Account IDs eg. ['123']  
```

### Data Source Custom Conversions Enabled
***(OPTIONAL)***  
By default, this package assumes that all custom conversions source tables for Google Ads, Google Campaign Mananger, and LinkedIn Ads are present in the source BigQuery project and schema. If any of these data source's conversion tables are *not* present, then disable them in this package by setting the relevant settings to `false`. This setting is typically only applicable for dbt projects where Improvado loaded source tables are not in the Improvado managed BigQuery project:  
*Note: If both option 1 and 2 are populated, then option 1 will take precendence over option 2*

***Option 1***  
Define dbt Project environment variables. [Read the dbt docs](https://docs.getdbt.com/docs/dbt-cloud/using-dbt-cloud/cloud-environment-variables) for more information on environment variables.
```
DBT_GOOGLE_ADS_CONVERSIONS_ENABLED = false
DBT_GOOGLE_CAMPAIGN_MANAGER_CONVERSIONS_ENABLED = false
DBT_LINKEDIN_ADS_CONVERSIONS_ENABLED = false
```

***Option 2***  
Define dbt variables in your `dbt_project.yml` file.
```yml
vars:
  google_ads_conversions_enabled: false
  google_campaign_manager_conversions_enabled: false
  linkedin_ads_conversions_enabled: false
```

### Data Source Conversions Settings
***(OPTIONAL)***  
By default, this package assumes that all *conversion types* (ex. Activity Groups, Activity, Action Category, Action, etc), 
*conversion metrics* (ex. Conversion, Click Through Conversion, View Through Conversion, Conversion Value, Click Through Conversion Value, View Through Conversion Value), 
and *converion names* from Google Ads, Google Campaign Mananger, and LinkedIn Ads are to be loaded from the source tables to the final data models. 
The below settings allow you to customize by data source which conversion types, conversion metrics, and conversion names to *include* in the load:  
*Note: If both option 1 and 2 are populated, then option 1 will take precendence over option 2*

***Option 1***  
Define dbt Project environment variables. [Read the dbt docs](https://docs.getdbt.com/docs/dbt-cloud/using-dbt-cloud/cloud-environment-variables) for more information on environment variables.
```
DBT_GOOGLE_ADS_CONVERSION_TYPES = ['action_name', 'action_category']
DBT_GOOGLE_ADS_CONVERSION_METRICS = ['all_conv', 'conversions', 'conversions_view_through', 'value_all_conv', 'value_conversions']
DBT_GOOGLE_ADS_CONVERSION_NAMES = ['Add to Cart', 'Form Submitted']     # List of converion names as seen in the platform's UI
DBT_GOOGLE_CAMPAIGN_MANAGER_CONVERSION_TYPES = ['activity', 'activity_group']
DBT_GOOGLE_CAMPAIGN_MANAGER_CONVERSION_METRICS = ['conversions', 'conversions_click_through', 'conversions_view_through', 'value_conversions', 'value_conversions_click_through', 'value_conversions_view_through']
DBT_GOOGLE_CAMPAIGN_MANAGER_CONVERSION_NAMES = ['Add to Cart', 'Form Submitted']      # List of converion names as seen in the platform's UI
DBT_LINKEDIN_ADS_CONVERSION_TYPES = ['conversion_name', 'conversion_type']
DBT_LINKEDIN_ADS_CONVERSION_METRICS = ['conversions', 'conversions_click_through', 'conversions_view_through', 'viral_conversions', 'viral_conversions_click_through', 'viral_conversions_view_through']
DBT_LINKEDIN_ADS_CONVERSION_NAMES = ['Add to Cart', 'Form Submitted']       # List of converion names as seen in the platform's UI
```

***Option 2***  
Define dbt variables in your `dbt_project.yml` file.
```yml
vars:
  google_ads_conversion_types: ['action_name', 'action_category']
  google_ads_conversion_metrics: ['all_conv', 'conversions', 'conversions_view_through', 'value_all_conv', 'value_conversions']
  google_ads_conversion_names: ['Add to Cart', 'Form Submitted']        # List of converion names as seen in the platform's UI
  google_campaign_manager_conversion_types: ['activity', 'activity_group']
  google_campaign_manager_conversion_metrics: ['conversions', 'conversions_click_through', 'conversions_view_through', 'value_conversions', 'value_conversions_click_through', 'value_conversions_view_through']
  google_campaign_manager_conversion_names: ['Add to Cart', 'Form Submitted']       # List of converion names as seen in the platform's UI
  linkedin_ads_conversion_types: ['conversion_name', 'conversion_type']
  linkedin_ads_conversion_metrics: ['conversions', 'conversions_click_through', 'conversions_view_through', 'viral_conversions', 'viral_conversions_click_through', 'viral_conversions_view_through']
  linkedin_ads_conversion_names: ['Add to Cart', 'Form Submitted']        # List of converion names as seen in the platform's UI
```

### Location of Improvado Source Tables
***(OPTIONAL)***  
By default, this package will look for your data in the Improvado BigQuery `green-post-223109` project and `agency_4333` schema. If this is not where your data is, please add the following configuration:  
*Note: If both option 1 and 2 are populated, then option 1 will take precendence over option 2*

***Option 1***  
Define dbt Project environment variables. [Read the dbt docs](https://docs.getdbt.com/docs/dbt-cloud/using-dbt-cloud/cloud-environment-variables) for more information on environment variables.
```
DBT_IMPROVADO_DATABASE = your_schema_name
DBT_IMPROVADO_SCHEMA = your_database_name
```

***Option 2***  
Define dbt variables in your `dbt_project.yml` file.
```yml
vars:
  improvado_database: your_database_name 
  improvado_schema: your_schema_name
```

----
## Contributions
Additional contributions to this package are very welcome! Please create issues
or open PRs against `master`. Check out 
[this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) 
on the best workflow for contributing to a package.

## Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
