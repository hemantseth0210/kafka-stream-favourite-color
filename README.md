# kafka-stream-favourite-color

## Create Topics
kafka-topics --zookeeper 127.0.0.1:2181 --create --topic favourite-colour-input --partitions 1 --replication-factor 1

kafka-topics --zookeeper 127.0.0.1:2181 --create --topic favourite-colour-output --partitions 1 --replication-factor 1 --config cleanup.policy=compact

kafka-topics --zookeeper 127.0.0.1:2181 --create --topic user-keys-and-colours --partitions 1 --replication-factor 1  --config cleanup.policy=compact

## Create Consumer
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 \
--topic favourite-colour-output \
--from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

## Create Producer
kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic favourite-colour-input

hemant,blue
john,green
hemant,red
alice,red

kafka-topics --zookeeper localhost:2181 --list
