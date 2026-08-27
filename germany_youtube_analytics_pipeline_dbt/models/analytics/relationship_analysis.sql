{{ config(
    materialized='table'
) }}

SELECT
    youtuber,
    category,
    subscribers,
    video_views,
    video_count,
    avg_views_per_video,
    views_per_subscriber,

    LN(subscribers + 1) AS log_subscribers,
    LN(video_views + 1) AS log_video_views,
    LN(video_count + 1) AS log_video_count

FROM {{ ref('stg_youtube_channels') }}