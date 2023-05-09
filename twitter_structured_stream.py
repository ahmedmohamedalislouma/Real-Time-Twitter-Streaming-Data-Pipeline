# Import your dependencies
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("TwitterStream").getOrCreate()
spark


tweet_schema = StructType([
    StructField("text", StringType(), True),
    StructField("id", StringType(), False),
    StructField("author_id", StringType(), False),
    StructField("retweet_count", IntegerType(), True),
    StructField("reply_count", IntegerType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("quote_count", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("name", StringType(), True),
    StructField("username", StringType(), True),
    StructField("followers_count", IntegerType(), True),
    StructField("following_count", IntegerType(), True),
    StructField("tweet_count", IntegerType(), True),
    StructField("listed_count", IntegerType(), True),
    StructField("verified", StringType(), True)
])


tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7777) \
    .load() \
    .select(from_json(col("value").cast("string"), tweet_schema).alias("tweet")) \
    .selectExpr(
        "tweet.id",
        "tweet.text",
        "tweet.author_id",
        "tweet.retweet_count",
        "tweet.quote_count",
        "tweet.created_at",
        "tweet.name",
        "tweet.username",
        "tweet.followers_count",
        "tweet.following_count",
        "tweet.tweet_count",
        "tweet.listed_count",
        "tweet.verified",
        "date(tweet.created_at) as date",
        "year(tweet.created_at) as year",
        "month(tweet.created_at) as month",
        "day(tweet.created_at)  as day", 
        "hour(tweet.created_at) as hour"
    )
    
    
    
    
# Write the streaming data to HDFS in parquet format
# query = tweet_df.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("checkpointLocation", "/checkpoint/twssiroj")\
#     .option("path","/twitter-landing-data")\
#     .partitionBy("year", "month", "day", "hour") \
#     .start()
# query.awaitTermination()

query = tweet_df.coalesce(1)\
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/checkpoint/tweet_cheek")\
    .option("path","/twitter-landing-data")\
    .partitionBy("year", "month", "day", "hour") \
    .start()
query.awaitTermination()


spark.stop()