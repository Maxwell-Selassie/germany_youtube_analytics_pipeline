{{ config(
    materialized='table'
) }}

WITH channels AS (

    SELECT *
    FROM {{ ref('stg_youtube_channels') }}

),

category_metrics AS (

    SELECT
        category,

        COUNT(*) AS total_channels,

        SUM(video_count) AS total_video_count,

        SUM(subscribers) AS total_subscribers,

        SUM(video_views) AS total_video_views,

        AVG(subscribers) AS avg_subscribers,

        AVG(video_count) AS avg_video_count,

        AVG(video_views) AS avg_video_views,

        AVG(avg_views_per_video) AS avg_views_per_video,

        AVG(views_per_subscriber) AS avg_views_per_subscriber

    FROM channels

    GROUP BY category

)

SELECT *
FROM category_metrics
ORDER BY avg_video_views DESC