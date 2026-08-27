import streamlit as st
import duckdb


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="German YouTube Analytics",
    page_icon="🇩🇪",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# DUCKDB CONFIGURATION
# ============================================================

DB_PATH = "germany_youtube_analytics_pipeline_dbt/dev.duckdb"


# ============================================================
# DUCKDB CONNECTION
# ============================================================

@st.cache_resource
def get_duckdb_connection():
    """
    Create a read-only DuckDB connection.

    DuckDB is used as the analytical database containing
    dbt-generated models.
    """
    return duckdb.connect(
        DB_PATH,
        read_only=True,
    )


# ============================================================
# LOAD DBT ANALYTICS MODELS
# ============================================================

@st.cache_data
def load_channel_data():
    """
    Load channel-level analytics produced by dbt.
    """

    con = get_duckdb_connection()

    query = """
        SELECT *
        FROM main.channel_performance
    """

    return con.execute(query).fetchdf()


@st.cache_data
def load_category_data():
    """
    Load category-level analytics produced by dbt.
    """

    con = get_duckdb_connection()

    query = """
        SELECT *
        FROM main.category_performance
        WHERE category NOT LIKE 'Https://%'
    """

    return con.execute(query).fetchdf()


@st.cache_data
def load_relationship_data():
    """
    Load relationship-analysis data produced by dbt.
    """

    con = get_duckdb_connection()

    query = """
        SELECT *
        FROM main.relationship_analysis
    """

    return con.execute(query).fetchdf()


# ============================================================
# LOAD DATA
# ============================================================

channel_df = load_channel_data()
category_df = load_category_data()
relationship_df = load_relationship_data()


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("Dashboard Controls")

st.sidebar.markdown(
    """
    Use the filters below to explore the German YouTube ecosystem.
    """
)


# ============================================================
# CATEGORY FILTER
# ============================================================

categories = sorted(
    category_df["category"]
    .dropna()
    .unique()
)

selected_categories = st.sidebar.multiselect(
    "Select Categories",
    categories,
    default=categories,
)


# ============================================================
# TOP CHANNEL CONTROL
# ============================================================

top_n = st.sidebar.slider(
    "Number of Top Channels",
    min_value=5,
    max_value=20,
    value=10,
    step=5,
)


# ============================================================
# APPLY CATEGORY FILTER
# ============================================================

filtered_category_df = category_df[
    category_df["category"].isin(selected_categories)
]


# ============================================================
# HEADER
# ============================================================

st.title("🇩🇪 German YouTube Analytics")

st.markdown(
    """
    ### Exploring channel popularity, content volume and category performance

    This dashboard is powered by analytical models built with
    **PySpark → DuckDB → dbt**.
    """
)

st.divider()


# ============================================================
# KPI CALCULATIONS
# ============================================================

total_channels = len(channel_df)

total_categories = len(filtered_category_df)

total_videos = filtered_category_df[
    "total_video_count"
].sum()

avg_category_views = filtered_category_df[
    "avg_video_views"
].mean()


# ============================================================
# KPI CARDS
# ============================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Channels",
        f"{total_channels:,}",
    )

with col2:
    st.metric(
        "Categories",
        f"{total_categories:,}",
    )

with col3:
    st.metric(
        "Total Videos",
        f"{total_videos:,}",
    )

with col4:
    st.metric(
        "Avg Video Views",
        f"{avg_category_views / 1_000_000:.1f}M",
    )


st.divider()


# ============================================================
# TOP CHANNELS
# ============================================================

st.subheader("🏆 Top YouTube Channels")

top_channels = (
    channel_df
    .sort_values(
        "subscribers",
        ascending=False,
    )
    .head(top_n)
)

st.bar_chart(
    top_channels.set_index("youtuber")["subscribers"]
)


# ============================================================
# CATEGORY PERFORMANCE
# ============================================================

st.subheader("📈 Category Performance")

category_chart_col1, category_chart_col2 = st.columns(2)


# ------------------------------------------------------------
# Average Views
# ------------------------------------------------------------

with category_chart_col1:

    st.markdown("**Average Views by Category**")

    category_views = (
        filtered_category_df
        .sort_values(
            "avg_video_views",
            ascending=False,
        )
        .set_index("category")
    )

    st.bar_chart(
        category_views["avg_video_views"]
    )


# ------------------------------------------------------------
# Content Volume
# ------------------------------------------------------------

with category_chart_col2:

    st.markdown("**Content Volume by Category**")

    category_volume = (
        filtered_category_df
        .sort_values(
            "total_video_count",
            ascending=False,
        )
        .set_index("category")
    )

    st.bar_chart(
        category_volume["total_video_count"]
    )


# ============================================================
# RELATIONSHIP ANALYSIS
# ============================================================

st.subheader("🔎 Relationship Analysis")

relationship_col1, relationship_col2 = st.columns(2)


# ------------------------------------------------------------
# Subscribers vs Video Views
# ------------------------------------------------------------

with relationship_col1:

    st.markdown(
        "**Subscribers vs. Total Video Views**"
    )

    st.scatter_chart(
        relationship_df,
        x="subscribers",
        y="video_views",
    )


# ------------------------------------------------------------
# Content Volume vs Average Views
# ------------------------------------------------------------

with relationship_col2:

    st.markdown(
        "**Content Volume vs. Average Views**"
    )

    st.scatter_chart(
        filtered_category_df,
        x="total_video_count",
        y="avg_video_views",
    )


# ============================================================
# CATEGORY DETAILS
# ============================================================

st.subheader("📊 Category Details")

display_category_df = filtered_category_df.copy()

display_category_df[
    "avg_video_views"
] = (
    display_category_df[
        "avg_video_views"
    ].round(0)
)

display_category_df = display_category_df.rename(
    columns={
        "category": "Category",
        "total_channels": "Total Channels",
        "total_video_count": "Total Videos",
        "total_subscribers": "Total Subscribers",
        "total_video_views": "Total Video Views",
        "avg_subscribers": "Average Subscribers",
        "avg_video_count": "Average Video Count",
        "avg_video_views": "Average Video Views",
        "avg_views_per_video": "Average Views per Video",
        "avg_views_per_subscriber": "Average Views per Subscriber",
    }
)

st.dataframe(
    display_category_df,
    use_container_width=True,
    hide_index=True,
)


# ============================================================
# CHANNEL DETAILS
# ============================================================

st.subheader("📺 Channel Details")

display_channel_df = channel_df.copy()

display_channel_df = display_channel_df.rename(
    columns={
        "rank": "Rank",
        "youtuber": "YouTuber",
        "subscribers": "Subscribers",
        "video_views": "Video Views",
        "video_count": "Video Count",
        "category": "Category",
        "started": "Started",
        "views_per_subscriber": "Views per Subscriber",
        "avg_views_per_video": "Average Views per Video",
        "subscriber_rank": "Subscriber Rank",
        "views_rank": "Views Rank",
        "content_volume_rank": "Content Volume Rank",
    }
)

st.dataframe(
    display_channel_df,
    use_container_width=True,
    hide_index=True,
)


# ============================================================
# INSIGHTS
# ============================================================

st.subheader("💡 Key Insights")

if not filtered_category_df.empty:

    best_category = filtered_category_df.loc[
        filtered_category_df[
            "avg_video_views"
        ].idxmax()
    ]

    largest_category = filtered_category_df.loc[
        filtered_category_df[
            "total_video_count"
        ].idxmax()
    ]

    st.markdown(
        f"""
        **Highest average video views:** {best_category['category']}
        with approximately
        {best_category['avg_video_views'] / 1_000_000:.2f}M
        average views per video.

        **Largest content volume:** {largest_category['category']}
        with approximately
        {largest_category['total_video_count']:,}
        videos.
        """
    )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "German YouTube Analytics Pipeline | "
    "PySpark • DuckDB • dbt • Airflow • Docker • Streamlit"
)
