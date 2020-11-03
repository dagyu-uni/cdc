package id.unimi.di.abd.model;

public class SourceRecord {
    public final String key;
    public final String value;

    public SourceRecord(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKHash() {
        return null;
    }
    public byte[] getHash() {
        return null;
    }
}
