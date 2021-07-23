package com.iwenqiang.chapter3;

import com.liwenqiang.timestamp_extractor.TransactionTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class KafkaStreamsYellingIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
    private final Time mockTime = Time.SYSTEM;

    private KafkaStreams kafkaStreams;
    private StreamsConfig streamsConfig;
    private Properties producerConfig;
    private Properties consumerConfig;

    private static final String YELL_A_TOPIC = "yell-A-topic";
    private static final String YELL_B_TOPIC = "yell-B-topic";
    private static final String OUT_TOPIC = "out-topic";


    @Test
    public void shouldYellFromMultipleTopics() throws Exception {
        StreamsBuilder builder = new StreamsBuilder();
        ValueMapper<String,String> mapper = String::toUpperCase;
        builder.stream(Pattern.compile("yell.*"), Consumed.with(Serdes.String(),Serdes.String()))
                .mapValues(mapper)
                .to(OUT_TOPIC);


        List<String> valuesToSendList = Arrays.asList("this", "should", "yell", "at", "you");
        List<String> expectedList = valuesToSendList.stream().map(String::toUpperCase).collect(Collectors.toList());

        kafkaStreams = new KafkaStreams(builder.build(),getProperties());
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(75000);
        kafkaStreams.close();

    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaStreamsYellingIntegrationTest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaStreamsYellingIntegrationTest_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaStreamsYellingIntegrationTest_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.3.250.19:9092,10.3.250.31:9092,10.3.250.32:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
        return props;
    }

}
