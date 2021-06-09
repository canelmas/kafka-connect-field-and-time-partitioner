### Kafka Connect Field and Time Based Partitioner

####  Summary
- Partition initially by custom fields and then by time.
- It extends **[TimeBasedPartitioner](https://github.com/confluentinc/kafka-connect-storage-common/blob/master/partitioner/src/main/java/io/confluent/connect/storage/partitioner/TimeBasedPartitioner.java)**, so any existing time based partition config should be fine i.e. `path.format` will be respected.
- In order to make it work, set `"partitioner.class"="com.canelmas.kafka.connect.FieldAndTimeBasedPartitioner"` and `"partition.field.name"="<comma separated custom fields in your record>"` in your connector config.
- Set `partition.field.format.path=false` if you don't want to use field labels for partitions names.

    ```bash
    {
        ...
        "s3.bucket.name" : "data", 
        "partition.field.name" : "appId,eventName,country",   
        "partition.field.format.path" : true,
        "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
        ...
    }          
    ```
    will produce an output in the following format : 
    
    ```bash
    /data/appId=XXXXX/eventName=YYYYYY/country=ZZ/year=2020/month=11/day=30
    ```  

####  Example

```bash
KCONNECT_NODES=("localhost:18083" "localhost:28083" "localhost:38083")

for i in "${!KCONNECT_NODES[@]}"; do
    curl ${KCONNECT_NODES[$i]}/connectors -XPOST -H 'Content-type: application/json' -H 'Accept: application/json' -d '{
        "name": "connect-s3-sink-'$i'",
        "config": {     
            "topics": "events",
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "tasks.max" : 10,
                "flush.size": 50,
                "rotate.schedule.interval.ms": 600,
                "rotate.interval.ms": -1,
                "s3.part.size" : 5242880,
                "s3.region" : "us-east-1",
                "s3.bucket.name" : "playground-parquet-ingestion",        
                "topics.dir": "data",
                "storage.class" : "io.confluent.connect.s3.storage.S3Storage",        
                "partitioner.class": "com.canelmas.kafka.connect.FieldAndTimeBasedPartitioner",
                "partition.field.name" : "appId,eventName",
                "partition.duration.ms" : 86400000,
                "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
                "locale" : "US",
                "timezone" : "UTC",        
                "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "io.confluent.connect.avro.AvroConverter",
                "value.converter.schema.registry.url": "http://schema-registry:8081",
                "schema.compatibility": "NONE",                
                "timestamp.extractor": "RecordField",
                "timestamp.field" : "clientCreationDate",
                "parquet.codec": "snappy"                            
        }
    }'
done
```

#### Installation Guide

1. Before building make sure maven and java development kit is install.
2. Firstly build the package using the following command `mvn package`.
3. After building the package a new jar will created in `target/connect-fieldandtime-partitioner-1.1.0-SNAPSHOT.jar` copy the jar file into the s3 plugin directory.
4. Restart the connector if the user use helm redeploy the helm so that it can detect the plugin.

__Tips__

*Where is the plugin?*

If the plugin was installed via confluent-hub the jar file should be copy to `/usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/lib/` however if kafka-connect-s3-sink was installed somewhere else place the jar file in the __same directory as the connector plugin jars__.

