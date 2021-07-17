package com.liwenqiang.processors;

import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.ClickEvent;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ClickEventProcessor implements Processor<String, ClickEvent, String, Tuple<ClickEvent, Object>> {
    protected ProcessorContext<String, Tuple<ClickEvent, Object>> context;

    @Override
    public void init(ProcessorContext<String, Tuple<ClickEvent, Object>> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, ClickEvent> record) {
        if (record.key() != null) {
            Tuple<ClickEvent, Object> tuple = Tuple.of(record.value(), null);
            Record<String, Tuple<ClickEvent, Object>> newRecord = record.withValue(tuple);
            context.forward(newRecord);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
