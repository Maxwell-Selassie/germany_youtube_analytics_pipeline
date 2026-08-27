with source as (
    select * 
    from {{ source ("raw", "youtube_channels") }}
),

renamed as (
    select
        youtuber,
        category,
        subscribers,
        video_views,
        video_count,
        rank,
        started,
        views_per_subscriber,
        avg_views_per_video

    from source
)

select *
from renamed