SELECT
    category,
    total_video_views

FROM {{ ref('category_performance') }}

WHERE total_video_views < 0