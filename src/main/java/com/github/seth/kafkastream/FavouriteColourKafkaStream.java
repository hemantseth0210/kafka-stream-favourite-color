package com.github.seth.kafkastream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColourKafkaStream {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the steps involved in the transformation - not recommended in prod
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // 1 - Stream from Kafka
        KStream<String, String> textLines = builder.stream("favourite-colour-input");
        KStream<String, String> userAndColour = textLines
                .filter((key, value) -> value.contains(",")) // 2 - Ensure that a comma is here as we will split on it
                .selectKey((key, value) -> value.split(",")[0].toLowerCase()) // 3 - Select a key that will be the userid (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase()) // 4 - Get the colour from value (lowercase for safety)
                .filter((user, colour) -> Arrays.asList("green", "blue", "red").contains(colour)); // 5 - Filter the colour

        userAndColour.to("user-keys-and-colours");

        // 6 - Read that topic as a KTable so that updates are read correctly
        KTable<String, String> userandColourTable = builder.table("user-keys-and-colours");

        KTable<String, Long> favouriteColour = userandColourTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour)) // 7 - Group by colour within the KTable
                .count(Named.as("CountsByColour"));

        // 7 - Output the result to a Kafka Topic
        favouriteColour.toStream().to("favourite-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        kafkaStreams.start();

        // printed topology
        System.out.println(kafkaStreams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
