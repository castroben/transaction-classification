##### Ingestion Flow Overview:

---> fetch batch .csv records ---> split into individual records  
        ---> serialize with Avro ---> publish to Kafka topic  
        ---> convert to .parquet ---> save to Landing Zone  

Notes:  
Landing zone partitioned to enable low-latency retrieval of transaction records based on {cc_num} field  
Landing zone files stored in optimized .parquet binary format  
Kafka message schema defined within PublishKafkaRecord processor services


Raw .csv record:  
`_3540080000000000,bc8ee8c20785f105426452bd40c297d0,11/7/2020 1:05,Welch Inc,misc_net,2.43,Christian,Johns,M,892 Solis Neck,Lonsdale,MN,55046,44.4477,-93.4252,5211,Chief Strategy Officer,27/01/1987,1373504703,44.378395,-94.194587,0`

Avro shcema:  
`{`<br>
`  "name": "Transaction",`<br>
`  "namespace": "com.example",`<br>
`  "type": "record",`<br>
`  "fields": [`<br>
`      {"name": "cc_num", "type": "string"},`<br>
`      {"name": "trans_num", "type": "string"},`<br>
`      {"name": "trans_date_trans_time", "type": "string"},`<br>
`      {"name": "merchant", "type": "string"},`<br>
`      {"name": "category", "type": "string"},`<br>
`      {"name": "amt", "type": "double"},`<br>
`      {"name": "first", "type": "string"},`<br>
`      {"name": "last", "type": "string"},`<br>
`      {"name": "gender", "type": "string"},`<br>
`      {"name": "street", "type": "string"},`<br>
`      {"name": "city", "type": "string"},`<br>
`      {"name": "state", "type": "string"},`<br>
`      {"name": "zip", "type": "int"},`<br>
`      {"name": "lat", "type": "double"},`<br>
`      {"name": "long", "type": "double"},`<br>
`      {"name": "city_pop", "type": "int"},`<br>
`      {"name": "job", "type": "string"},`<br>
`      {"name": "dob", "type": "string"},`<br>
`      {"name": "unix_time", "type": "long"},`<br>
`      {"name": "merch_lat", "type": "double"},`<br>
`      {"name": "merch_long", "type": "double"}`<br>
`  ]` <br>
`}`


![ingestion_engine](https://github.com/user-attachments/assets/0dba47e1-46e9-4d1a-a82a-7d8ca321a998)
