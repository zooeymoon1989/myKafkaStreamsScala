package com.liwenqiang.chapter5;

import com.liwenqiang.clients.producer.MockDataProducer;
import com.liwenqiang.collectors.FixedSizePriorityQueue;
import com.liwenqiang.config.initConfig.InitGetProperties;
import com.liwenqiang.util.model.ShareVolume;
import com.liwenqiang.util.model.StockTransaction;
import com.liwenqiang.util.serde.StreamsSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

@SuppressWarnings("unchecked")
public class AggregationsAndReducingExample {
//    private static Logger LOG = LoggerFactory.getLogger(AggregationsAndReducingExample.class);

    public static void main(String[] args) throws Exception {
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<ShareVolume> shareVolumeSerde = StreamsSerdes.ShareVolumeSerde();
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = StreamsSerdes.FixedSizePriorityQueueSerde();
        NumberFormat numberFormat = NumberFormat.getInstance();

        Comparator<ShareVolume> comparator = (sv1, sv2) -> sv2.getShares() - sv1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedQueue = new FixedSizePriorityQueue<>(comparator, 5);

        ValueMapper<FixedSizePriorityQueue, String> valueMapper = fpq -> {
            StringBuilder builder = new StringBuilder();
            Iterator<ShareVolume> iterator = fpq.iterator();
            int counter = 1;
            while (iterator.hasNext()) {
                ShareVolume stockVolume = iterator.next();
                if (stockVolume != null) {
                    builder.append(counter++).append(")").append(stockVolume.getSymbol())
                            .append(":").append(numberFormat.format(stockVolume.getShares())).append(" ");
                }
            }
            return builder.toString();
        };

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, ShareVolume> shareVolume = builder.stream(MockDataProducer.STOCK_TRANSACTIONS_TOPIC,
                Consumed.with(stringSerde, stockTransactionSerde)
                        .withOffsetResetPolicy(EARLIEST))
                .mapValues(st -> ShareVolume.newBuilder(st).build())
                .groupBy((k, v) -> v.getSymbol(), Grouped.with(stringSerde, shareVolumeSerde))
                .reduce(ShareVolume::sum);


        shareVolume.groupBy((k, v) -> KeyValue.pair(v.getIndustry(), v), Grouped.with(stringSerde, shareVolumeSerde))
                .aggregate(() -> fixedQueue,
                        (k, v, agg) -> agg.add(v),
                        (k, v, agg) -> agg.remove(v),
                        Materialized.with(stringSerde, fixedSizePriorityQueueSerde))
                .mapValues(valueMapper)
                .toStream().peek((k, v) -> System.out.printf("Stock volume by industry %s %s\n", k, v))
                .to("stock-volume-by-company", Produced.with(stringSerde, stringSerde));


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), new InitGetProperties("AggregationsAndReducingExample").GetProperties());
        MockDataProducer.produceStockTransactions(15, 50, 25, false);
        System.out.println("First Reduction and Aggregation Example Application Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(65000);
        System.out.println("Shutting down the Reduction and Aggregation Example Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }
}
