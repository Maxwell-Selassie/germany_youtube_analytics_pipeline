SELECT
    category,
    avg_subscribers,
    avg_video_count,
    avg_video_views,
    avg_views_per_video,
    avg_views_per_subscriber

FROM {{ ref('category_performance') }}

WHERE avg_subscribers < 0
   OR avg_video_count < 0
   OR avg_video_views < 0
   OR avg_views_per_video < 0
   OR avg_views_per_subscriber < 0