package com.liwenqiang.Punctuator;

import com.liwenqiang.util.model.StockPerformance;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


public class StockPerformancePunctuator implements Punctuator {

    private final double differentialThreshold;
    private final ProcessorContext context;
    private final KeyValueStore<String, StockPerformance> keyValueStore;

    public StockPerformancePunctuator(double differentialThreshold,
                                      ProcessorContext context,
                                      KeyValueStore<String, StockPerformance> keyValueStore) {

        this.differentialThreshold = differentialThreshold;
        this.context = context;
        this.keyValueStore = keyValueStore;

    }

    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, StockPerformance> performanceKeyValueIterator = keyValueStore.all();
        while (performanceKeyValueIterator.hasNext()) {
            KeyValue<String, StockPerformance> keyValue = performanceKeyValueIterator.next();
            String key = keyValue.key;
            StockPerformance value = keyValue.value;

            if (value != null) {
                if (value.priceDifferential() >= differentialThreshold || value.volumeDifferential() >= differentialThreshold) {
                    context.forward(key, value);
                }
            }
        }
        performanceKeyValueIterator.close();
        context.commit();
    }
}
