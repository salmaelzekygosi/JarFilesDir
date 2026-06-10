package com.gosi.kafka;

public class Payment {
    private String id;
    private double amount;
    private String currency;
    private String traceId;

    public Payment() {}

    public Payment(String id, double amount, String currency, String traceId) {
        this.id = id;
        this.amount = amount;
        this.currency = currency;
        this.traceId = traceId;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }

    @Override
    public String toString() {
        return "Payment{id='" + id + "', amount=" + amount + ", currency='" + currency + "', traceId='" + traceId + "'}";
    }
}
