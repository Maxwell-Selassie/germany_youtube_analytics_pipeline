SELECT
    youtuber,
    video_count

FROM {{ ref('channel_performance') }}

WHERE video_count < 0