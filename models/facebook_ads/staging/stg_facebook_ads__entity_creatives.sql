{%- set source_account_ids = get_account_ids('facebook ads') -%}

{{ config(enabled= source_account_ids|length > 0 is true) }}

WITH

source_data AS (

    SELECT * FROM {{ source('improvado', 'facebook_entity_creatives') }}

    WHERE REPLACE(account_id, 'act_', '') IN (SELECT REPLACE(x, 'act_', '') FROM UNNEST({{ source_account_ids }}) AS x)

),

final AS (

    SELECT
        account_id,
        account_name,
        date,
        creative_id,
        actor_id,
        applink_treatment,
        body,
        branded_content_sponsor_page_id,
        call_to_action_type,
        effective_instagram_story_id,
        effective_object_story_id,
        image_hash,
        image_url,
        instagram_actor_id,
        instagram_permalink_url,
        instagram_story_id,
        link_og_id,
        link_url,
        name AS creative_name,
        object_id,
        object_story_id,
        object_type,
        object_url,
        product_set_id,
        status,
        template_url,
        thumbnail_url,
        title,
        url_tags,
        use_page_actor_override,
        video_id,
        creative_destination_url,
        description,
        caption,
        creative_link,
        lead_gen_form_id,
        template_data_link,
        source_instagram_media_id,
        website_destination_url,
        creative_child_attachments_urls,
        asset_links_website_url

    FROM source_data

    QUALIFY ROW_NUMBER() OVER (PARTITION BY creative_id ORDER BY __insert_date DESC) = 1
    
)

SELECT * FROM final