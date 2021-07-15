package com.liwenqiang.processors.supplier;

import com.liwenqiang.processors.StockPerformanceProcessor;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class StockPerformanceProcessorSupplier implements ProcessorSupplier<String, StockTransaction> {
    private final String stateStoreName;
    private final double differentialThreshold;

    public StockPerformanceProcessorSupplier(String stateStoreName,double differentialThreshold){
        this.stateStoreName = stateStoreName;
        this.differentialThreshold = differentialThreshold;
    }

    @Override
    public Processor<String, StockTransaction> get() {
        return new StockPerformanceProcessor(this.stateStoreName,this.differentialThreshold);
    }
}
