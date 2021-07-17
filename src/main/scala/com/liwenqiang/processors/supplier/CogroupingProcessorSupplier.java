package com.liwenqiang.processors.supplier;

import com.liwenqiang.processors.CogroupingProcessor;
import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.ClickEvent;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;


public class CogroupingProcessorSupplier implements ProcessorSupplier<String, Tuple<ClickEvent, StockTransaction>, String, Tuple<ClickEvent, StockTransaction>> {
    @Override
    public Processor<String, Tuple<ClickEvent, StockTransaction>, String, Tuple<ClickEvent, StockTransaction>> get() {
        return new CogroupingProcessor();
    }
}
