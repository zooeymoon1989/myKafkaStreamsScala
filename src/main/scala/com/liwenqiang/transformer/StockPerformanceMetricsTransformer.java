package com.liwenqiang.transformer;

import com.liwenqiang.util.model.StockPerformance;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class StockPerformanceMetricsTransformer implements Transformer<String, StockTransaction, KeyValue<String, StockPerformance>> {

    private String stocksStateStore = "stock-performance-store";
    private KeyValueStore<String, StockPerformance> keyValueStore;
    private double differentialThreshold = 0.05;
    private org.apache.kafka.streams.processor.ProcessorContext processorContext;
    private Sensor metricsSensor;
    private static final AtomicInteger count = new AtomicInteger(1);


    public StockPerformanceMetricsTransformer(String stocksStateStore, double differentialThreshold) {
        this.stocksStateStore = stocksStateStore;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final org.apache.kafka.streams.processor.ProcessorContext processorContext) {
        keyValueStore = (KeyValueStore) processorContext.getStateStore(stocksStateStore);
        this.processorContext = processorContext;

        this.processorContext.schedule(Duration.ofMillis(5000), PunctuationType.WALL_CLOCK_TIME, this::doPunctuate);


        final String tagKey = "task-id";
        final String tagValue = processorContext.taskId().toString();
        final String nodeName = "StockPerformanceProcessor_"+count.getAndIncrement();
        metricsSensor = processorContext.metrics().addLatencyRateTotalSensor("transformer-node",
                nodeName, "stock-performance-calculation",
                Sensor.RecordingLevel.DEBUG,
                tagKey,
                tagValue);
    }

    @Override
    public KeyValue<String, StockPerformance> transform(String symbol, StockTransaction stockTransaction) {
        if (symbol != null) {
            StockPerformance stockPerformance = keyValueStore.get(symbol);

            if (stockPerformance == null) {
                stockPerformance = new StockPerformance();
            }

            long start = System.nanoTime();
            stockPerformance.updatePriceStats(stockTransaction.getSharePrice());
            stockPerformance.updateVolumeStats(stockTransaction.getShares());
            stockPerformance.setLastUpdateSent(Instant.now());
            long end = System.nanoTime();

            processorContext.metrics().recordLatency(metricsSensor, start, end);

            keyValueStore.put(symbol, stockPerformance);
        }
        return null;
    }

    private void doPunctuate(long timestamp) {
        KeyValueIterator<String, StockPerformance> performanceIterator = keyValueStore.all();

        while (performanceIterator.hasNext()) {
            KeyValue<String, StockPerformance> keyValue = performanceIterator.next();
            String key = keyValue.key;
            StockPerformance stockPerformance = keyValue.value;

            if (stockPerformance != null) {
                if (stockPerformance.priceDifferential() >= differentialThreshold ||
                        stockPerformance.volumeDifferential() >= differentialThreshold) {
                    processorContext.forward(key, stockPerformance);
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    public KeyValue<String, StockPerformance> punctuate(long l) {
        throw new UnsupportedOperationException("Should not call punctuate");
    }

    @Override
    public void close() {

    }
}
