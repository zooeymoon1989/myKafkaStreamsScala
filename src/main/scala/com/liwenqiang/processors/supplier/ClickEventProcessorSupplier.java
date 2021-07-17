package com.liwenqiang.processors.supplier;

import com.liwenqiang.processors.ClickEventProcessor;
import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.ClickEvent;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class ClickEventProcessorSupplier implements ProcessorSupplier<String, ClickEvent,String, Tuple<ClickEvent, Object>> {
    @Override
    public Processor<String, ClickEvent, String, Tuple<ClickEvent, Object>> get() {
        return new ClickEventProcessor();
    }
}
