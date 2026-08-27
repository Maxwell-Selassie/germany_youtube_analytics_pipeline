{{
    config(materialized="table")
}}

with channels as (
    SELECT * 
    FROM {{ ref("stg_youtube_channels") }}
),

ranked as (
    SELECT 
        youtuber,
        category,
        subscribers,
        video_views,
        video_count,
        rank,
        started,
        views_per_subscriber,
        avg_views_per_video,

        RANK() OVER (
            ORDER BY subscribers DESC
        ) AS subscriber_rank,

        RANK() OVER (
            ORDER BY video_views DESC
        ) AS views_rank,

        RANK() OVER (
            ORDER BY video_count DESC
        ) AS content_volume_rank

    FROM channels
)

SELECT *
FROM ranked