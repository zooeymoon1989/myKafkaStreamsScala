package com.liwenqiang.processors.supplier;

import com.liwenqiang.processors.BeerPurchaseProcessor;
import com.liwenqiang.util.model.BeerPurchase;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

public class BeerPurchaseProcessorSupplier implements ProcessorSupplier<String, BeerPurchase, String, BeerPurchase> {

    private final String domesticSalesNode;
    private final String internationalSalesNode;

    public BeerPurchaseProcessorSupplier(String sink1, String sink2) {
        this.domesticSalesNode = sink1;
        this.internationalSalesNode = sink2;
    }

    @Override
    public Processor<String, BeerPurchase, String, BeerPurchase> get() {
        return new BeerPurchaseProcessor(domesticSalesNode, internationalSalesNode);
    }
}
