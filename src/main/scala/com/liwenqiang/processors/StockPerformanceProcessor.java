package com.liwenqiang.processors;


import com.liwenqiang.Punctuator.StockPerformancePunctuator;
import com.liwenqiang.util.model.StockPerformance;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

public class StockPerformanceProcessor implements Processor<String, StockTransaction> {
    private KeyValueStore<String, StockPerformance> keyValueStore;
    private final String stateStoreName;
    private final double differentialThreshold;

    public StockPerformanceProcessor(String stateStoreName, double differentialThreshold) {
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    public void init(ProcessorContext context) {
        keyValueStore = (KeyValueStore) context.getStateStore(stateStoreName);
        StockPerformancePunctuator punctuator = new StockPerformancePunctuator(differentialThreshold,
                context,
                keyValueStore);

        context.schedule(Duration.ofMillis(10000), PunctuationType.WALL_CLOCK_TIME, punctuator);
    }

    @Override
    public void process(String key, StockTransaction value) {
        String symbol = value.getSymbol();
        if (symbol != null) {
            StockPerformance stockPerformance = keyValueStore.get(symbol);

            if (stockPerformance == null) {
                stockPerformance = new StockPerformance();
            }

            stockPerformance.updatePriceStats(value.getSharePrice());
            stockPerformance.updateVolumeStats(value.getShares());
            stockPerformance.setLastUpdateSent(Instant.now());

            keyValueStore.put(symbol, stockPerformance);
        }
    }

    @Override
    public void close() {

    }
}
