-- twitter_Data table
CREATE EXTERNAL TABLE IF NOT EXISTS twitter.twitter_Data (
  id STRING,
  text STRING,
  author_id STRING,
  retweet_count INT,
  quote_count INT, 
  reply_count INT,
  created_at TIMESTAMP,
  name STRING,
  username STRING,
  followers_count INT, 
  following_count INT,
  user_id STRING,
  User_name STRING,
  tweet_count INT,
  listed_count INT,
  verified STRING
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/twitter-landing-data';

-----------------------------------------------------

MSCK REPAIR TABLE twitter.twitter_Data;

------------------------------------------------------
-- user dimension table

CREATE TABLE IF NOT EXISTS twitter.user_dim_raw (
  author_id STRING ,
  followers_count INT,
  following_count INT,
  name STRING,
  username STRING,
  tweet_count INT,
  verified STRING
)
STORED AS PARQUET
LOCATION '/twitter-raw-data/user_dim_raw';

-- Insert DISTINCT tweets into the user dimension table

INSERT OVERWRITE TABLE twitter.user_dim_raw
SELECT DISTINCT
author_id,
followers_count,
following_count,
name,
username,
tweet_count,
verified
FROM twitter.twitter_Data;

---------------------------------------------------------
-- Tweet dimension table
CREATE TABLE IF NOT EXISTS twitter.tweet_dim_raw (
  id STRING ,
  text STRING,
  quote_count INT,
  reply_count INT,
  retweet_count INT
)
STORED AS PARQUET
LOCATION '/twitter-raw-data/tweet_dim_raw';

-- Insert distinct tweets into the tweet dimension table
INSERT OVERWRITE TABLE twitter.tweet_dim_raw
SELECT DISTINCT
  id,
  text,
  quote_count,
  reply_count,
  retweet_count
FROM twitter.twitter_Data;

-----------------------------------------------------------
-- Twitter fact table
CREATE TABLE IF NOT EXISTS twitter_fact_raw (
  id STRING,
  author_id STRING,
  created_at TIMESTAMP,
  tweet_count INT, 
  day_tweet_count INT, 
  month_tweet_count INT,
  war_word_count_per_hour INT 
  
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/twitter-raw-data/twitter_fact_raw';


INSERT OVERWRITE TABLE twitter.twitter_fact_raw PARTITION (year, month, day, hour)
SELECT id, author_id, created_at ,
       hour_tweet_count, 
       day_tweet_count, 
       month_tweet_count,
       war_word_count_per_hour , year, month, day, hour  
FROM (
  SELECT td.id, td.author_id, td.created_at, 
         COUNT(*) OVER(PARTITION BY td.year, td.month, td.day, td.hour) as hour_tweet_count,
         COUNT(*) OVER(PARTITION BY td.year, td.month, td.day) as day_tweet_count,
         COUNT(*) OVER(PARTITION BY td.year, td.month) as month_tweet_count,
         sum (CASE WHEN lower(text) LIKE '%war%' THEN 1 ELSE 0 END ) OVER(PARTITION BY td.year, td.month, td.day, td.hour) AS war_word_count_per_hour,
         td.year, td.month, td.day, td.hour
  FROM twitter.twitter_Data td
) t;