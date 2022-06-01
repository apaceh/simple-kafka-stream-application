package com.github.apaceh;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        /*final Properties streamConfiguration = getStreamConfiguration();

        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> usersStream = builder.stream("users",
                Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> System.out.println("usersStream Key: " + k + ", Value: " + v));

        final KStream<String, String> locationStream = builder.stream("users-location",
                Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k, v) -> System.out.println("locationStream Key: " + k + ", Value: " + v));

        ValueJoiner<String, String, String> valueJoiner = (usersValue, locationValue) -> usersValue + locationValue;

        KStream<String, String> joined = usersStream.leftJoin(
                locationStream,
                valueJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(3)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

//        joined.foreach((k, v) -> System.out.println("Joined Key: " + k + ", Value: " + v));
        joined.peek((k, v) -> System.out.println("joined Key: " + k + ", Value: " + v))
                .to("output", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamConfiguration);

        logger.info("Starting stream...");

        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));*/
    }

    static Properties getStreamConfiguration() {
        final String bootstrapServers = "172.18.46.11:9092,172.18.46.12:9092,172.18.46.13:9092";

        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "full-user-data");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "full-user-data-client");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return properties;
    }
}