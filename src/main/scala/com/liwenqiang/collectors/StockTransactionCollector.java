package com.liwenqiang.collectors;

import com.liwenqiang.util.model.StockTransaction;

public class StockTransactionCollector {
    private double amount;
    private String tickerSymbol;
    private int sharesPurchased;
    private int sharesSold;

    public StockTransactionCollector add(StockTransaction transaction){
        if(tickerSymbol == null){
            tickerSymbol = transaction.getSymbol();
        }

        this.amount += transaction.getSharePrice();
        if(transaction.getSector().equalsIgnoreCase("purchase")){
            this.sharesPurchased += transaction.getShares();
        } else{
            this.sharesSold += transaction.getShares();
        }
        return this;
    }

    @Override
    public String toString() {
        return "StockTransactionCollector{" +
                "amount=" + amount +
                ", tickerSymbol='" + tickerSymbol + '\'' +
                ", sharesPurchased=" + sharesPurchased +
                ", sharesSold=" + sharesSold +
                '}';
    }
}
