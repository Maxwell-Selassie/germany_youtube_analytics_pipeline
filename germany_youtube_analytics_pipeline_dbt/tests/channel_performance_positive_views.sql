SELECT
    youtuber,
    video_views

FROM {{ ref('channel_performance') }}

WHERE video_views < 0