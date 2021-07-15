package com.liwenqiang.processors;

import com.liwenqiang.util.model.BeerPurchase;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class KStreamPrinter implements Processor<String, BeerPurchase, String, BeerPurchase> {
    private ProcessorContext<String, BeerPurchase> context;
    @Override
    public void init(ProcessorContext<String, BeerPurchase> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void process(Record<String, BeerPurchase> record) {
        System.out.printf("Key [%s] Value[%s]%n",record.key(),record.value());
        context.forward(record);
    }
}
