SELECT
    youtuber,
    subscribers

FROM {{ ref('channel_performance') }}

WHERE subscribers < 0