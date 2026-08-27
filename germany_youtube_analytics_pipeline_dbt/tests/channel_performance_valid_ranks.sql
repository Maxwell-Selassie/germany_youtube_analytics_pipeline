SELECT
    youtuber,
    subscriber_rank,
    views_rank,
    content_volume_rank

FROM {{ ref('channel_performance') }}

WHERE subscriber_rank <= 0
   OR views_rank <= 0
   OR content_volume_rank <= 0