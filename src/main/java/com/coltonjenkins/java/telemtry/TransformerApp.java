package com.coltonjenkins.java.telemtry;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.common.serialization.Serdes.Integer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 */
@Slf4j
public class TransformerApp 
{
    private static final String APP_ID = "demo";
    private static final String SERVER_URL = "kafka:9092";
    private static final String SOURCE = "input-stream";
    private static final String TO = "output-stream";

    public static void main( String[] args )
    {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_URL);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

        final var builder = new StreamsBuilder();
        KStream<String, Integer> s = builder.stream(SOURCE);

        s.groupByKey(Grouped.with(String(), Integer()))
        .aggregate(() -> new CountAndSum(0,0), 
                   (key, value, agg) -> {
                        agg.count++;
                        agg.sum += value;
                        log.info("SUM: " + agg.sum);
                        return agg;
                   }, Materialized.<String, CountAndSum, KeyValueStore<Bytes, byte[]>>as("agg-store").withValueSerde(new CountAndSumSerde()))
        .mapValues(countAndSum -> countAndSum.sum/countAndSum.count, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("avg-store"))
        .toStream()
        .to(TO);

        var topology = builder.build();
        final var streams = new KafkaStreams(topology, props);

        var countDown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("demo-stream") {
            @Override
            public void run() {
                streams.close();
                countDown.countDown();
            }
        });

        log.info(topology.describe().toString());

        try {
            log.info("STARTING...");
            streams.start();
            log.info("STARTED!");
            countDown.await();
            log.info("Exiting...");
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
            System.exit(1);
        } finally {
            streams.close();
        }
    }

    @Data
    static class CountAndSum {
        int count;
        int sum;

        CountAndSum() {}
        CountAndSum(int c, int s) {
            count = c;
            sum = s;
        }
    }

    static class CountAndSumSerde implements Serde<CountAndSum> {
        @Override
        public Serializer<CountAndSum> serializer() {
            return new CountAndSumSerializer();
        }

        @Override
        public Deserializer<CountAndSum> deserializer() {
            return new CountAndSumDeserializer();
        }
    }

    static class CountAndSumSerializer implements Serializer<CountAndSum> {
        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, CountAndSum data) {
            try {
                return objectMapper.writeValueAsBytes(data);        
            } catch(Exception ex) {
                ex.printStackTrace();
            }
            return new byte[]{};
        }
        
    }

    static class CountAndSumDeserializer implements Deserializer<CountAndSum> {
        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public CountAndSum deserialize(String topic, byte[] data) {
            try {
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), CountAndSum.class);
            } catch(Exception ex) {
                ex.printStackTrace();
            }
            return null;
        }
    }
}