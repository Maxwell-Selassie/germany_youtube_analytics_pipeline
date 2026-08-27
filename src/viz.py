import matplotlib.pyplot as plt

from src.spark import get_spark_session
from configs.config import GOLD_DATA_DIR

VIZ_DIR = GOLD_DATA_DIR / "visualizations"
VIZ_DIR.mkdir(parents=True, exist_ok=True)


def visualize_channel_performance(spark):
    path = GOLD_DATA_DIR / "channel_performance"
    
    df = spark.read.parquet(str(path))
    
    # top 10 channels by subscribers 
    top_subscribers = (
        df.orderBy(df["subscribers"].desc())
        .limit(10)
        .collect()
    )
    
    names = [row["youtuber"] for row in top_subscribers]
    subscribers = [row["subscribers"] for row in top_subscribers]
    
    plt.figure(figsize=(10, 7))
    plt.barh(names[::-1], subscribers[::-1])
    plt.xlabel("Subscribers")
    plt.ylabel("Youtuber")
    plt.title("Top 10 German Youtube Channels by Subscribers")
    
    output = VIZ_DIR / "top_10_channels_by_subscribers.png"
    plt.savefig(output, dpi=300)
    plt.close()
    
    print(f"Saved: {output}")
    
def visualize_category_views(spark):
    path = GOLD_DATA_DIR / "category_performance"

    df = spark.read.parquet(str(path))

    # Remove the malformed scraped URL category
    df = df.filter(
        ~df["category"].startswith("Https://")
    )

    rows = (
        df.orderBy(df["avg_views_per_category"].desc())
        .collect()
    )

    categories = [row["category"] for row in rows]
    avg_views = [row["avg_views_per_category"] for row in rows]

    plt.figure(figsize=(12, 8))
    plt.barh(categories[::-1], avg_views[::-1])
    plt.xlabel("Average Views")
    plt.ylabel("Category")
    plt.title("Average Views by YouTube Category")
    plt.tight_layout()

    output = VIZ_DIR / "average_views_by_category.png"
    plt.savefig(output, dpi=300)
    plt.close()
    
    print(f"Saved: {output}")
    
def visualize_category_video_count(spark):
    path = GOLD_DATA_DIR / "category_performance"

    df = spark.read.parquet(str(path))

    # Remove malformed category
    df = df.filter(
        ~df["category"].startswith("Https://")
    )

    rows = (
        df.orderBy(df["total_video_count_per_category"].desc())
        .collect()
    )

    categories = [row["category"] for row in rows]
    video_counts = [
        row["total_video_count_per_category"]
        for row in rows
    ]

    plt.figure(figsize=(12, 8))
    plt.barh(categories[::-1], video_counts[::-1])
    plt.xlabel("Total Video Count")
    plt.ylabel("Category")
    plt.title("Total Video Count by YouTube Category")
    plt.tight_layout()

    output = VIZ_DIR / "video_count_by_category.png"
    plt.savefig(output, dpi=300)
    plt.close()

    print(f"Saved: {output}")
    
def visualize_subscribers_vs_views(spark):
    path = GOLD_DATA_DIR / "channel_performance"

    df = spark.read.parquet(str(path))

    rows = (
        df.select("subscribers", "video_views")
        .dropna()
        .collect()
    )

    subscribers = [row["subscribers"] for row in rows]
    video_views = [row["video_views"] for row in rows]

    plt.figure(figsize=(10, 7))
    plt.scatter(subscribers, video_views, alpha=0.6)

    plt.xlabel("Subscribers")
    plt.ylabel("Total Video Views")
    plt.title("Subscribers vs. Total Video Views")
    plt.tight_layout()

    output = VIZ_DIR / "subscribers_vs_video_views.png"
    plt.savefig(output, dpi=300)
    plt.close()

    print(f"Saved: {output}")


def visualize_video_count_vs_views(spark):
    path = GOLD_DATA_DIR / "category_performance"

    df = spark.read.parquet(str(path))

    df = df.filter(
        ~df["category"].startswith("Https://")
    )

    rows = (
        df.select(
            "total_video_count_per_category",
            "avg_views_per_category"
        )
        .dropna()
        .collect()
    )

    video_counts = [
        row["total_video_count_per_category"]
        for row in rows
    ]

    avg_views = [
        row["avg_views_per_category"]
        for row in rows
    ]

    plt.figure(figsize=(10, 7))
    plt.scatter(video_counts, avg_views, alpha=0.6)

    plt.xlabel("Total Videos in Category")
    plt.ylabel("Average Views")
    plt.title("Content Volume vs. Average Views by Category")
    plt.tight_layout()

    output = VIZ_DIR / "video_count_vs_average_views.png"
    plt.savefig(output, dpi=300)
    plt.close()

    print(f"Saved: {output}")
    
    
def main():
    spark = get_spark_session()

    try:
        visualize_channel_performance(spark)
        visualize_category_views(spark)
        visualize_category_video_count(spark)
        visualize_subscribers_vs_views(spark)
        visualize_video_count_vs_views(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main() 