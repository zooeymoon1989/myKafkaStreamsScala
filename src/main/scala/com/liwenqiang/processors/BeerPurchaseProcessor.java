package com.liwenqiang.processors;

import com.liwenqiang.util.model.BeerPurchase;
import com.liwenqiang.util.model.Currency;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

import java.text.DecimalFormat;

import static com.liwenqiang.util.model.Currency.DOLLARS;

public class BeerPurchaseProcessor implements Processor<String, BeerPurchase, String, BeerPurchase> {

    private InternalProcessorContext context;

    private final String domesticSalesNode;
    private final String internationalSalesNode;

    public BeerPurchaseProcessor(String domesticSalesNode, String internationalSalesNode) {
        this.domesticSalesNode = domesticSalesNode;
        this.internationalSalesNode = internationalSalesNode;
    }

    @Override
    public void process(Record<String, BeerPurchase> record) {
        Currency purchaseCurrency = record.value().getCurrency();
        if (purchaseCurrency != DOLLARS) {
            BeerPurchase dollarBeerPurchase;
            BeerPurchase.Builder builder = BeerPurchase.newBuilder(record.value());
            double totalSale = record.value().getTotalSale();
            String pattern = "###.##";
            DecimalFormat decimalFormat = new DecimalFormat(pattern);
            builder.currency(DOLLARS);
            builder.totalSale(Double.parseDouble(decimalFormat.format(purchaseCurrency.convertToDollars(totalSale))));
            dollarBeerPurchase = builder.build();
            context.forward(record.key(), dollarBeerPurchase, To.child(internationalSalesNode));
        }else{
            context.forward(record.key(), purchaseCurrency, To.child(domesticSalesNode));
        }
    }
}
