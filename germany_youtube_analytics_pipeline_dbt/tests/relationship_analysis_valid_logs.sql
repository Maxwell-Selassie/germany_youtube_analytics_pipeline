SELECT
    youtuber,
    log_subscribers,
    log_video_views,
    log_video_count

FROM {{ ref('relationship_analysis') }}

WHERE log_subscribers < 0
   OR log_video_views < 0
   OR log_video_count < 0