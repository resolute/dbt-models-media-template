# Resolute Digital standard dbt models

This package models data from Improvado's BigQuery report types.

## Models

This package contains transformation models, designed to be starting models for any Resolute Digital reporting. The primary outputs of this package are described below.

### Paid Media
#### google_campaign_manager__performance_daily
Each record represents the daily performance of each Google Campaign Manager creative, ad, and placement.
##### Improvado Report Types:
* dcm_ads_creatives_placements
* dcm_ads_placement_sites

#### google_ads__performance_daily
Each record represents the daily performance of each Google Ads ad.
##### Improvado Report Types:
* google_adwords_ads
* google_adwords_ad_conversions

#### google_ads__keyword_performance_daily
Each record represents the daily performance of each Google Ads keyword.
##### Improvado Report Types:
* google_adwords_keywords_extended
* google_adwords_keywords_conversions

#### google_ads__search_query_performance_daily
Each record represents the daily performance of each Google Ads search query.
##### Improvado Report Types:
* google_adwords_search_query_extended
* google_adwords_search_query_conversions

#### facebook_ads__ads_performance_daily
Each record represents the daily performance of each Facebook Ads ad.
##### Improvado Report Types:
* facebook_ads_placements
* facebook_entity_ads
* facebook_entity_adsets
* facebook_entity_campaigns

#### facebook_ads__creatives_performance_daily
Each record represents the daily performance of each Facebook Ads creative.
##### Improvado Report Types:
* facebook_ads_creative
* facebook_entity_ads
* facebook_entity_adsets
* facebook_entity_campaigns
* facebook_entity_creatives

#### facebook_ads__campaign_month_reach
Each record represents the monthly performance of each Facebook Ads campaign with Reach metric.
##### Improvado Report Types:
* facebook_campaign_month_reach
* facebook_entity_ads
* facebook_entity_campaigns

#### linkedin_ads__performance_daily
Each record represents the daily performance of each LinkedIn Ads creative.
##### Improvado Report Types:
* linkedin_ads_creatives
* linkedin_ads_conversions

#### twitter_ads__promoted_tweets_performance_daily
Each record represents the daily performance of each Twitter Ads promoted tweet.
##### Improvado Report Types:
* twitter_promoted_tweets_with_cards

#### pinterest_ads__pins_performance_daily
Each record represents the daily performance of each Pinterest Ads ad.
##### Improvado Report Types:
* pinterest_ads_pins_1v_30en_30cl

### Organic Media
#### social_organic__posts_rollup
Each record represents the lifetime performance of each social organic post for Facebook, Instagram, LinkedIn, and Twitter.
##### Improvado Report Types:
* facebook_pages_post
* instagram_organic_post
* linkedin_organic_share_lifetime
* twitter_organic_tweets

#### social_organic__followers_rollup
Each record represents the daily follower totals of each social organic account for Facebook, Instagram, LinkedIn, and Twitter.
##### Improvado Report Types:
* facebook_pages_page
* instagram_organic_page_info
* linkedin_organic_followers_lifetime
* twitter_page

#### youtube_organic__top_videos
Each record represents the daily performance of each YouTube channel video.
##### Improvado Report Types:
* youtube_organic_top_videos

#### youtube_organic__traffic_source_daily
Each record represents the daily performance of each YouTube channel traffic source.
##### Improvado Report Types:
* youtube_organic_traffic_source_daily

### Website
#### website__session_performance
Each record represents the daily session performance from Google Analytics
##### Improvado Report Types:
* ga_session_performance

## Installation Instructions
[Read the dbt docs](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/#git-packages) for more information on installing git packages. Please add the following configuration to you `packages.yml` file, make sure to enter a correct revision:

```yml
# packages.yml

...
packages:
  - git: "https://github.com/resolute/dbt-models-media-template.git"
    revision: "x.x.x"

```

## Configuration
By default, this package will look for your data in the Improvado BigQuery schema. If this is not where your data is, please add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
config-version: 2

vars:
    improvado_schema: your_schema_name
    improvado_database: your_database_name 
```
For each data source you need to populate the appropriate Improvado account_id, please add the following configuration to your `dbt_project.yml` file:

```yml
# dbt_project.yml

...
config-version: 2

vars:
  google_analytics_ids: []            # List of Google Analytics View IDs eg. ['123']
  google_ads_ids: []                  # List of Google Ads Account IDs eg. ['123']
  google_campaign_manager_ids: []     # List of Google Campaign Manager Advertiser IDs eg. ['123']
  facebook_ads_ids: []                # List of Facebook Ads Account IDs eg. ['123']
  linkedin_ads_ids: []                # List of LinkedIn Ads Account IDs eg. ['123']
  twitter_ads_ids: []                 # List of Twitter Ads Account IDs eg. ['123']
  pinterest_ads_ids: []               # List of Pinterest Account IDs eg. ['123']
  facebook_organic_ids: []            # List of Facebook Account IDs eg. ['123']
  instagram_organic_ids: []           # List of Instagram Account IDs eg. ['123']
  linkedin_organic_ids: []            # List of LinkedIn Account IDs eg. ['123']
  twitter_ads_ids: []                 # List of Twitter Account IDs eg. ['123']
  twitter_organic_ids: []             # List of Twitter Account IDs eg. ['123']
  pinterest_organic_ids: []           # List of Pinterest Account IDs eg. ['123']
  youtube_organic_ids: []             # List of YouTube Account IDs eg. ['123']
```

The package assumes that all custom conversions for Google Ads, Google Campaign Mananger, LinkedIn Ads, and Twitter Ads are not enabled. If you want to include custom conversions from these data sources, enable those data sources' custom conversions in this package by setting the relevant variables to `true`:

```yml
# dbt_project.yml

...
config-version: 2

vars:
  google_ads_conversions_enabled: true
  google_campaign_manager_conversions_enabled: true
  linkedin_ads_conversions_enabled: true
```

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
