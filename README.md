# real-time transaction classification

### This project is meant to define a prototype for processing events in real time using "big data" technologies. 

### Principal Components:
1. Ingestion engine implemented in Apache Nifi  
2. Event Landing zone implemeted in MinIO object storage (S3 compatible)  
3. Stream Processing implemented through Apache Spark Structured Streaming API  
4. ML Classification implemented through Apache Spark MLlib  

### Future improvements:
1. Reimplement ingestion and streaming capabilities in Confluent  
2. Enforce serialization/deserialization through a Schema Registry  
3. Deploy Spark workflows in Azure Databricks
4. Save classified events in persistent storage

![architecture_initial](https://github.com/user-attachments/assets/111ddc48-e790-43d0-98ab-8e62354585ec)
