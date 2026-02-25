import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

/**
 * Transaction POJO — annotated so KafkaJsonSchemaSerializer
 * auto-generates the exact JSON Schema we want.
 */
public class Transaction {

    @JsonProperty(required = true)
    @JsonPropertyDescription("Unique transaction identifier")
    private String transactionId;

    @JsonProperty(required = true)
    @JsonPropertyDescription("Account identifier")
    private String accountId;

    @JsonProperty(required = true)
    @JsonPropertyDescription("Transaction amount")
    private double amount;

    @JsonProperty(required = true)
    @JsonPropertyDescription("Currency code")
    private Currency currency;

    @JsonProperty(required = true)
    @JsonPropertyDescription("Merchant name")
    private String merchant;

    @JsonProperty(required = true)
    @JsonPropertyDescription("Country code")
    private Country country;

    @JsonProperty(required = true)
    @JsonPropertyDescription("Epoch milliseconds")
    private long timestamp;

    // Enums for currency and country — serializer generates "enum" in schema
    public enum Currency {
        SAR, AED, USD, GBP, EUR
    }

    public enum Country {
        SA, AE, US, GB, EG
    }

    public Transaction() {
    }

    public Transaction(String transactionId, String accountId, double amount,
            Currency currency, String merchant, Country country, long timestamp) {
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

    public Currency getCurrency() {
        return currency;
    }

    public void setCurrency(Currency currency) {
        this.currency = currency;
    }

    public String getMerchant() {
        return merchant;
    }

    public void setMerchant(String merchant) {
        this.merchant = merchant;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
