##### Ingestion Flow Overview:

---> fetch batch .csv records ---> split into individual records 
        ---> serialize with Avro ---> publish to Kafka topic
        ---> convert to .parquet ---> save to Landing Zone

Landing zone partitioned to enable low-latency retrieval of transaction records based on {cc_num} field
Landing zone files stored in optimized .parquet binary format 

Kafka message schema defined within PublishKafkaRecord processor services

![ingestion_engine](https://github.com/user-attachments/assets/0dba47e1-46e9-4d1a-a82a-7d8ca321a998)
