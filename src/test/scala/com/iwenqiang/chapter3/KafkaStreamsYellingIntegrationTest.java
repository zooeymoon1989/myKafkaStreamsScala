package com.iwenqiang.chapter3;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

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

    @ClassrRule
    public static final EmbeddedKafkaCluster EMBEDDED_KAFKA =new EmbeddedKafkaCluster(NUM_BROKERS);

}
