package com.liwenqiang.processors;

import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.ClickEvent;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ClickEventProcessor implements Processor<String, ClickEvent, String, ClickEvent> {
    private ProcessorContext<String, ClickEvent> context;

    @Override
    public void init(ProcessorContext<String, ClickEvent> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, ClickEvent> record) {
        if (record.key() != null) {
            Tuple<ClickEvent, Object> tuple = Tuple.of(record.value(), null);
            record.withValue(tuple);
            context.forward(record);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
