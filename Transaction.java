/**
 * Transaction POJO — used by KafkaJsonSchemaSerializer to auto-generate
 * a proper JSON Schema with typed fields and register it in Schema Registry.
 */
public class Transaction {

    private String transactionId;
    private String accountId;
    private double amount;
    private String currency;
    private String merchant;
    private String country;
    private long timestamp;

    public Transaction() {
    }

    public Transaction(String transactionId, String accountId, double amount,
            String currency, String merchant, String country, long timestamp) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.currency = currency;
        this.merchant = merchant;
        this.country = country;
        this.timestamp = timestamp;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
