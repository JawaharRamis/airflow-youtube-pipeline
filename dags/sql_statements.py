create_yt_table = """
    CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id VARCHAR(20) NOT NULL,
    title VARCHAR(255) NOT NULL,
    region_code VARCHAR(10) NOT NULL,
    release_date VARCHAR(30) NOT NULL,
    channel VARCHAR(100) NOT NULL,
    views_num NUMERIC NOT NULL,
    likes_num NUMERIC NOT NULL,
    comments VARCHAR(10) NOT NULL,
    views VARCHAR(10) NOT NULL,
    likes VARCHAR(10) NOT NULL
    
);
"""

analysis_sql_queries = [
        """
        SELECT region_code, title
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY region_code ORDER BY views DESC) as rnk
            FROM youtube_videos
        ) ranked
        WHERE rnk = 1;
        """,
        """
        SELECT title, likes
        FROM youtube_videos
        ORDER BY likes_num DESC
        LIMIT 3;
        """,
        """
        SELECT title, likes, views, 
        CAST(likes_num AS float) / CAST(views_num AS float) * 100 AS likes_to_views_percentage
        FROM youtube_videos
        ORDER BY likes_to_views_percentage DESC
        LIMIT 5;
        """
    ]