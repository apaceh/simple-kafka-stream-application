package com.github.apaceh;

import com.github.apaceh.model.User;
import com.github.apaceh.model.UserLocation;
import com.github.apaceh.model.UserLocationJoin;
import com.github.apaceh.serdes.JsonDeserializer;
import com.github.apaceh.serdes.JsonSerializer;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class MyStream {
    private static final String USER_TOPIC = "users";
    private static final String USER_LOCATION_TOPIC = "users-location";

    protected static Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    private static final Gson GSON_TO_PRINT = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    private static void printStream(final KStream<String, ?> stream) {
        stream.foreach((k, v) -> {
            log.info("Printing ====> Key [{}], Value [{}]", k, GSON_TO_PRINT.toJson(v));
        });
    }

    private void printStreamWindowed(final KStream<Windowed<Integer>, ?> stream) {
        stream.foreach((k, v) -> {
            log.info("Windowed Printing ====> Key [{}], Value [{}]", k, GSON_TO_PRINT.toJson(v));
        });
    }

    private static KStream<String, User> getUserAsStream(final StreamsBuilder streamsBuilder) {
        final KStream<String, String> sUserOrig = streamsBuilder.stream(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        printStream(sUserOrig);
        return sUserOrig.map((k, v) -> {
            final JSONObject jsonObject = new JSONObject(v);
            final User user = GSON.fromJson(jsonObject.toString(), User.class);
            return KeyValue.pair(jsonObject.get("id").toString(), user);
        });
    }

    private static KStream<String, UserLocation> getLocationAsStream(final StreamsBuilder streamsBuilder) {
        final KStream<String, String> sUserAddressOrig = streamsBuilder.stream(USER_LOCATION_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        printStream(sUserAddressOrig);
        return sUserAddressOrig
                .filter((k, v) -> {
                    return !Objects.isNull(v);
                })
                .map((k, v) -> {
                    final JSONObject jsonObject = new JSONObject(v);
                    final UserLocation userLocation = GSON.fromJson(jsonObject.toString(), UserLocation.class);
                    return KeyValue.pair(jsonObject.get("user_id").toString(), userLocation);
                });
    }

    private static Serde<List<UserLocation>> getListLocationSerde() {
        final JsonSerializer<List<UserLocation>> serializer = new JsonSerializer<>();
        final JsonDeserializer<List<UserLocation>> deserializer = new JsonDeserializer<List<UserLocation>>(List.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    private static <T> Serde<T> getSerde(T t) {
        final JsonSerializer<T> serializer = new JsonSerializer<>();
        final JsonDeserializer<T> deserializer = new JsonDeserializer<T>(t.getClass());
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static void main(String[] args) {

        try (InputStream propertiesFile = new FileInputStream("/home/alfi/config/stream.properties")) {

            Properties streamProperties = new Properties();
            streamProperties.load(propertiesFile);

            final StreamsBuilder streamsBuilder = new StreamsBuilder();
            final KStream<String, User> userKStream = getUserAsStream(streamsBuilder);
            final KStream<String, UserLocation> userLocationKStream = getLocationAsStream(streamsBuilder);
            final KGroupedStream<String, User> userKGroupedStream = userKStream.groupByKey(Grouped.with(Serdes.String(),
                    getSerde(new User())));

            final KTable<String, User> ktUser = userKGroupedStream.aggregate(User::new, (k, v, a) -> v,
                    Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as("aggregate-store")
                            .withValueSerde(getSerde(new User())));

            final KStream<String, UserLocationJoin> userLocationJoinKStream = userLocationKStream.join(ktUser,
                    (location, user) -> UserLocationJoin.getInstance(user, location),
                    Joined.with(Serdes.String(), getSerde(new UserLocation()), getSerde(new User())));

            printStream(userLocationJoinKStream);
            userLocationJoinKStream.to("users-data",  Produced.with(Serdes.String(), getSerde(new UserLocationJoin())));

            final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamProperties);

            streams.cleanUp();
            streams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
