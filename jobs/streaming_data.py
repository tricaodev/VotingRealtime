from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, count, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType

if __name__ == "__main__":
    spark = SparkSession.builder \
                .appName("RealtimeVotingEngineering") \
                .master("spark://spark-master:7077") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6") \
                .getOrCreate()

    votes_schema = StructType([
        StructField("voter_id", StringType(), False),
        StructField("voter_name", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("candidate_id", StringType(), False),
        StructField("candidate_name", StringType(), False),
        StructField("party_affiliation", StringType(), False),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("voting_time", TimestampType(), False),
        StructField("vote", IntegerType(), False)
    ])

    # Read message from kafka
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "votes_topic") \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) value") \
            .select(from_json(col("value"), votes_schema).alias("data")) \
            .selectExpr("data.*") \
            .withWatermark("voting_time", "1 minute")

    votes_per_candidate = (df.groupBy(col("candidate_id"), col("candidate_name"), col("party_affiliation"),col("photo_url"))
                            .agg(sum(col("vote")).alias("total_votes"))
                            .select(to_json(struct("*")).alias("value")))

    turnout_per_location = (df.groupBy(col("address.state"))
                            .agg(count("*").alias("total_votes"))
                            .select(to_json(struct("*")).alias("value")))

    votes_per_candidate = votes_per_candidate.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "votes_per_candidate") \
        .outputMode("update") \
        .option("checkpointLocation", "./checkpoint/votes_per_candidate") \
        .start()

    turnout_per_location = turnout_per_location.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "turnout_per_location") \
        .outputMode("update") \
        .option("checkpointLocation", "./checkpoint/turnout_per_location") \
        .start()

    votes_per_candidate.awaitTermination()
    turnout_per_location.awaitTermination()