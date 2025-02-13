"""
Real-Time Transaction Classification Pipeline

This script processes streaming transaction data using Apache Spark and Kafka.
It reads transaction data from Kafka, enriches it with historical statistics from landing zone,
applies a pre-trained RandomForest classifier,and publishes results back to Kafka in micro-batces.

Author: Benjamin Castro B.
Version: 1.0
"""
if __name__ == "__main__":
    from pyspark.ml.classification import RandomForestClassificationModel
    from pyspark.ml.feature import VectorAssembler, StringIndexer, StringIndexerModel, OneHotEncoder
    
    from pyspark.sql import functions as f
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
    from pyspark.sql.window import Window
    
    try:
        print("Creating Spark session...")
        
        spark = SparkSession.builder\
            .appName("real-time-analytics")\
            .config("spark.sql.caseSensitive", "true")\
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "s3access") \
            .config("spark.hadoop.fs.s3a.secret.key", "_s3access123$") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
    
        print("Spark session created successfully.")
    
    except Exception as e:
        # Handle exception and continue execution
        print(f"An error occurred while creating Spark session: {e}")
        sys.exit(1)

    spark.sparkContext.setLogLevel("ERROR")
    print(f"This cluster relies on Spark '{spark.version}'")
    
    # Streaming configurations
    kafka_topic_input = "transactions_raw_v1" # topic name for input transaction events
    kafka_topic_output = "transactions_classified_v1" # topic name input transaction classification events
    kafka_bootstrap_server_address = "localhost:9092" # server address for kafka connection
    checkpoint_filepath = "/home/osbdet/notebooks/real-time-analytics/checkpoint"
    
    # Object storage configurations
    s3_files_timestamp_col = "trans_date_trans_time" # timestamp col name of raw events landed in object storage
    num_historical_transactions = 5 # transaction history threshold for processing
    
    # Classifier configurations
    string_indexer_filepath = "/home/osbdet/notebooks/real-time-analytics/classifier/string_indexer"
    rf_classifier_filepath = "/home/osbdet/notebooks/real-time-analytics/classifier/model"


    # Schema for deserialization
    schema = """
        cc_num BIGINT,
        trans_num STRING,
        trans_date_trans_time STRING,
        merchant STRING,
        category STRING,
        amt DOUBLE,
        first STRING,
        last STRING,
        gender STRING,
        street STRING,
        city STRING,
        state STRING,
        zip INT,
        lat DOUBLE,
        long DOUBLE,
        city_pop INT,
        job STRING,
        dob STRING,
        unix_time BIGINT,
        merch_lat DOUBLE,
        merch_long DOUBLE
    """

    def get_transaction_history(cc_num, num_historical_transactions):
        """Retrieves historical transaction data for a given credit card number."""
        path = f"s3a://raw-transactions/{cc_num}"
        relevant_columns = ["cc_num", "trans_date_trans_time", "amt", "lat", "long", "merch_lat", "merch_long", "category"]
        
        try:
            print(f"Retrieving transaction history for cc_num={cc_num}")
            data = spark.read.parquet(path).withColumn("file_name", f.input_file_name())
            file_count = data.select("file_name").distinct().count()
            
            if file_count <= num_historical_transactions:
                print("Not enough transaction history available.")
                return None
            
            if "transaction_time" in data.columns:
                data = data.orderBy("transaction_time", ascending=False).limit(num_historical_transactions)
            else:
                data = data.limit(num_historical_transactions)
    
            return data.withColumn("trans_date_trans_time", f.to_timestamp("trans_date_trans_time", "dd/MM/yyyy HH:mm"))\
                       .select(relevant_columns)
        except Exception as e:
            print(f"Error retrieving transaction history for cc_num={cc_num} reading from {path}: {e}")
            return None
    
    def clean_transaction(df_transaction):
        """Cleans and processes transaction data by adding new derived features."""
        print("Transaction cleaning...")
        return df_transaction\
            .withColumn("trans_date_trans_time", f.to_timestamp("trans_date_trans_time", "dd/MM/yyyy HH:mm"))\
            .withColumn("dob", f.to_date(f.col("dob"), "dd/MM/yyyy"))\
            .withColumn("hour", f.hour("trans_date_trans_time"))\
            .withColumn("day", f.dayofweek("trans_date_trans_time"))\
            .withColumn("month", f.month("trans_date_trans_time"))\
            .withColumn("year", f.year("trans_date_trans_time"))\
            .withColumn("weekend", f.when(f.col("day").isin(1, 7), 1).otherwise(0))\
            .withColumn("customer_age", f.round(f.datediff(f.col("trans_date_trans_time"), f.col("dob")) / 365, 0).cast('Integer'))\
            .withColumn("distance", f.sqrt(f.pow(f.col("lat") - f.col("merch_lat"), 2) + f.pow(f.col("long") - f.col("merch_long"), 2)))\
            .withColumn("gender", f.when(f.col("gender") == "Male", 1).otherwise(0))\
            .drop("merchant", "first", "last", "job", "dob", "street", "city", "state", "zip", "city_pop")
    
    def enrich_transaction(df_transaction, df_transaction_history):
        """Enriches transaction data with historical statistics."""
        print("Transaction enrichment...")
        window = Window.partitionBy("cc_num").orderBy(f.col("trans_date_trans_time"))
        
        df_amt_stats = df_transaction_history.agg(
            f.mean("amt").alias("historical_mean_amt"),
            f.stddev("amt").alias("historical_std_amt"),
            f.max("amt").alias("historical_max_amt"),
            f.min("amt").alias("historical_min_amt")
        )
        
        df_avg_time_diff = df_transaction_history\
            .withColumn("prev_trans_time", f.lag("trans_date_trans_time").over(window))\
            .agg(f.mean("prev_trans_time").alias("historical_avg_time_diff"))
        
        df_distance = df_transaction_history\
            .withColumn("distance", f.sqrt((f.col("lat") - f.col("merch_lat"))**2 + (f.col("long") - f.col("merch_long"))**2))
        
        df_avg_distance = df_distance.agg(
            f.mean("distance").alias("historical_avg_distance_from_merchant"),
            f.stddev("distance").alias("historical_std_distance_from_merchant")
        )
        
        df_num_categories = df_transaction_history.agg(
            f.countDistinct("category").cast("int").alias("historical_num_categories")
        )
        
        return df_transaction.crossJoin(df_amt_stats).crossJoin(df_avg_time_diff).crossJoin(df_avg_distance).crossJoin(df_num_categories)
    
    def encode_transaction(df_transaction):
        """Encodes categorical transaction features using a pre-trained StringIndexer model."""
        print("Transaction encoding...")
        indexer = StringIndexerModel.load(string_indexer_filepath)
        df_indexed = indexer.transform(df_transaction)
        encoder = OneHotEncoder(inputCol="category_index", outputCol="category_encoded")
        return encoder.fit(df_indexed).transform(df_indexed).drop("category_index")
    
    def vectorize_transaction(df_transaction):
        """Vectorizes transaction data for machine learning models."""
        print("Transaction vectorization...")
        feature_columns = [col for col in df_transaction.columns if col not in {"trans_num", "trans_date_trans_time", "category"}]
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        return assembler.transform(df_transaction)
    
    def construct_classification_event(df_transaction_classified):
        """Constructs transaction classification event for publishing in JSON format."""
        print("Constructing transaction classification event...")
        df_classification_event = df_transaction_classified.drop("category_encoded", "features", "rawPrediction", "probability")
        df_classification_event_json = df_classification_event.selectExpr("trans_num AS key", "to_json(struct(*)) AS value")
    
        return df_classification_event_json
    
    def process_transaction(df_transaction, batch_id):
        """
        Processes a single batch of transaction data:
        1. Fetch historical transaction data
        2. Clean the transaction data
        3. Enrich with historical statistics
        4. Encode categorical features
        5. Vectorize transaction features
        6. Classify using the pre-trained RandomForest model
        7. Publish classification results to Kafka
        """
        print(f"Initiated transaction processing for batch_id={batch_id}")
        if df_transaction.isEmpty():
            print(f"Skipping batch {batch_id} as it contains no data")
            return
        
        cc_num = df_transaction.select("cc_num").first()[0]
        trans_num = df_transaction.select("trans_num").first()[0]
        print(f"Processing transaction with trans_num={trans_num} for cc_num={cc_num}")
        
        df_transaction_history = get_transaction_history(cc_num, num_historical_transactions)
        if df_transaction_history is None:
            print(f"Lack of history - skipping transaction {trans_num}")
            return
        
        df_transaction_clean = clean_transaction(df_transaction)
        df_transaction_enriched = enrich_transaction(df_transaction_clean, df_transaction_history)
        df_transaction_encoded = encode_transaction(df_transaction_enriched)
        df_transaction_vectorized = vectorize_transaction(df_transaction_encoded)
        
        loaded_rf_model = RandomForestClassificationModel.load(rf_classifier_filepath)
        df_transaction_classified = loaded_rf_model.transform(df_transaction_vectorized)
        df_transaction_classification_event = construct_classification_event(df_transaction_classified)
        
        df_transaction_classification_event.write\
            .format("kafka")\
            .option("kafka.bootstrap.servers", kafka_bootstrap_server_address)\
            .option("topic", kafka_topic_output)\
            .save()

    
    # Defining Kafka input stream
    df_transaction_raw = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", kafka_bootstrap_server_address)\
        .option("subscribe", kafka_topic_input)\
        .option("startingOffsets", "latest")\
        .load()

    # Deserializing Kafka message
    df_transaction_deserialized = df_transaction_raw.selectExpr("CAST(value AS STRING) AS transaction") \
        .select(f.from_csv(f.col("transaction"), schema).alias("data")) \
        .select("data.*")
    print("deserialized transaction: \n")
    df_transaction_deserialized.printSchema()

    # Micro-batch processing
    sink = df_transaction_deserialized \
        .writeStream \
        .foreachBatch(process_transaction) \
        .option("checkpointLocation", checkpoint_filepath) \
        .start()
    
    sink.awaitTermination()
