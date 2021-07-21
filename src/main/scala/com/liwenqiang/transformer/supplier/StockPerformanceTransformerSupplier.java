package com.liwenqiang.transformer.supplier;

import com.liwenqiang.transformer.StockPerformanceTransformer;
import com.liwenqiang.util.model.StockPerformance;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

public class StockPerformanceTransformerSupplier implements TransformerSupplier<String, StockTransaction, KeyValue<String, StockPerformance>> {
    private final String stateStoreName;
    private final double differentialThreshold;

    public StockPerformanceTransformerSupplier(String stateStoreName,double differentialThreshold) {
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    public Transformer<String, StockTransaction, KeyValue<String, StockPerformance>> get() {
        return new StockPerformanceTransformer(stateStoreName,differentialThreshold);
    }
}
