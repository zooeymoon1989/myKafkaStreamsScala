package com.liwenqiang.processors.supplier;

import com.liwenqiang.processors.KStreamPrinter;
import com.liwenqiang.util.model.BeerPurchase;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class KStreamPrinterSupplier implements ProcessorSupplier<String, BeerPurchase, String, BeerPurchase> {
    @Override
    public Processor get() {
        return new KStreamPrinter();
    }
}
