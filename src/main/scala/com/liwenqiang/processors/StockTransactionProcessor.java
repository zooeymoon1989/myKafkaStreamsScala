package com.liwenqiang.processors;

import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class StockTransactionProcessor implements Processor<String, StockTransaction, String, Tuple<Object, StockTransaction>> {
    private ProcessorContext<String, Tuple<Object, StockTransaction>> context;

    @Override
    public void init(ProcessorContext<String, Tuple<Object, StockTransaction>> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, StockTransaction> record) {
        String key = record.key();
        if (key != null) {
            Tuple<Object, StockTransaction> tuple = Tuple.of(null, record.value());
            Record<String, Tuple<Object, StockTransaction>> newRecord = record.withValue(tuple);
            context.forward(newRecord);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
