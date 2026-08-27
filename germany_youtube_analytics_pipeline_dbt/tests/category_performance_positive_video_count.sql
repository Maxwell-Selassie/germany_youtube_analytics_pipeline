SELECT
    category,
    total_video_count

FROM {{ ref('category_performance') }}

WHERE total_video_count < 0