package com.liwenqiang.transformer.supplier;

import com.liwenqiang.util.model.StockPerformance;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import com.liwenqiang.transformer.StockPerformanceMetricsTransformer;

public class StockPerformanceMetricsTransformerSupplier implements TransformerSupplier<String, StockTransaction, KeyValue<String, StockPerformance>> {

    private String stocksStateStore;
    private double differentialThreshold;

    public StockPerformanceMetricsTransformerSupplier(String stocksStateStore, double differentialThreshold) {
        this.stocksStateStore = stocksStateStore;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    public Transformer<String, StockTransaction, KeyValue<String, StockPerformance>> get() {
        return new StockPerformanceMetricsTransformer(stocksStateStore, differentialThreshold);
    }
}
