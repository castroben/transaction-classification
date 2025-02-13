# real-time transaction classification

### This project is meant to define a prototype for processing events in real time using "big data" technologies. 

### The princpal components of this project are:
1. Ingestion engine implemented in Apache Nifi  
2. Event Landing zone implemeted in MinIO object storage (S3 compatible)  
3. Stream Processing implemented through Apache Spark Structured Streaming API  
4. ML Classification implemented through Apache Spark MLlib  

### Future improvements
1. Reimplement ingestion and streaming capabilities in Confluent  
2. Enforce serialization/deserialization through a Schema Registry  
3. Deploy Spark workflows in Azure Databricks  

### Running the Spark process  
(Assuming you have setup OSBDET environment appropriately)  
`export PYSPARK_PYTHON=/home/osbdet/.jupyter_venv/bin/python3`<br>  
`export SPARK_HOME=/home/osbdet/.jupyter_venv/lib/python3.11/site-packages/pyspark`<br>    
`$SPARK_HOME/bin/spark-submit --master local --packages \` <br>
    `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \` <br>
    `real-time-analytics.py 2>&1 | grep -v INFO`


### Ingestion Engine


### Landing Zone


### Stream Processing


### ML Classifier

