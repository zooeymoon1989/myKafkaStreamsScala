package com.iwenqiang.chapter8;

import com.liwenqiang.chapter8.ZMartTopology;
import com.liwenqiang.util.datagen.DataGenerator;
import com.liwenqiang.util.model.Purchase;
import com.liwenqiang.util.model.PurchasePattern;
import com.liwenqiang.util.model.RewardAccumulator;
import com.liwenqiang.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;


public class ZMartTopologyTest {

    private TopologyTestDriver topologyTestDriver;

    @Test
    @DisplayName("Testing the ZMart Topology Flow")
    public void testZMartTopology() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        this.topologyTestDriver = new TopologyTestDriver(ZMartTopology.build(), props);

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        Purchase purchase = DataGenerator.generatePurchase();

        TestInputTopic<String, Purchase> inputTopic = topologyTestDriver.createInputTopic("transactions", stringSerde.serializer(), purchaseSerde.serializer());
        inputTopic.pipeInput(purchase);
        TestOutputTopic<String, Purchase> outputTopic = topologyTestDriver.createOutputTopic("purchases", stringSerde.deserializer(), purchaseSerde.deserializer());

        Purchase expectedPurchase = Purchase.builder(purchase).maskCreditCard().build();
        assertThat(outputTopic.readValue(),equalTo(expectedPurchase));

        RewardAccumulator expectedRewardAccumulator = RewardAccumulator.builder(expectedPurchase).build();

        TestOutputTopic<String, RewardAccumulator> accumulatorProducerRecord = topologyTestDriver.createOutputTopic("rewards", stringSerde.deserializer(), rewardAccumulatorSerde.deserializer());

        assertThat(accumulatorProducerRecord.readValue(),equalTo(expectedRewardAccumulator));

        PurchasePattern expectedPurchasePattern = PurchasePattern.builder(expectedPurchase).build();

        TestOutputTopic<String, PurchasePattern> purchasePatternProducerRecord = topologyTestDriver.createOutputTopic("patterns", stringSerde.deserializer(), purchasePatternSerde.deserializer());

        assertThat(purchasePatternProducerRecord.readValue(),equalTo(expectedPurchasePattern));

        System.out.println("Testing is done");
    }

    @After
    public void tearDown(){
        topologyTestDriver.close();
    }
}
