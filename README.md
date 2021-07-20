# kafka-demo
Learning Kafka to handle large scale analytics.

Run with `docker compose up`

Can monitor the output of the average with ...
```
docker exec -it kafka-demo_kafka_1 /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
--topic output-stream \
--bootstrap-server localhost:9092 \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter 
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer
