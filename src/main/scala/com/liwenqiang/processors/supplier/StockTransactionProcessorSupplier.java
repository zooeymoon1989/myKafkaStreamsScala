package com.liwenqiang.processors.supplier;

import com.liwenqiang.processors.StockTransactionProcessor;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class StockTransactionProcessorSupplier implements ProcessorSupplier<String, StockTransaction,String, StockTransaction> {
    @Override
    public Processor<String, StockTransaction, String, StockTransaction> get() {
        return new StockTransactionProcessor();
    }
}
