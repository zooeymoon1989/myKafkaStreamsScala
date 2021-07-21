package com.liwenqiang.transformer;

import com.liwenqiang.Punctuator.StockPerformancePunctuator;
import com.liwenqiang.util.model.StockPerformance;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

public class StockPerformanceTransformer implements Transformer<String, StockTransaction, KeyValue<String, StockPerformance>> {
    private final String stateStoreName;
    private final double differentialThreshold;
    private KeyValueStore<String, StockPerformance> keyValueStore;


    public StockPerformanceTransformer(String stateStoreName, double differentialThreshold) {
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    public void init(ProcessorContext context) {
        keyValueStore = (KeyValueStore) context.getStateStore(stateStoreName);
        StockPerformancePunctuator punctuator = new StockPerformancePunctuator(differentialThreshold, context, keyValueStore);
        context.schedule(Duration.ofMillis(15000), PunctuationType.STREAM_TIME, punctuator);
    }

    @Override
    public KeyValue<String, StockPerformance> transform(String symbol, StockTransaction transaction) {
        if (symbol != null) {
            StockPerformance stockPerformance = keyValueStore.get(symbol);

            if (stockPerformance == null) {
                stockPerformance = new StockPerformance();
            }

            stockPerformance.updatePriceStats(transaction.getSharePrice());
            stockPerformance.updateVolumeStats(transaction.getShares());
            stockPerformance.setLastUpdateSent(Instant.now());

            keyValueStore.put(symbol, stockPerformance);
        }
        return null;
    }

    @Override
    public void close() {

    }
}
