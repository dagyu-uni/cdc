package it.unimi.di.abd.model;

public class SyncRecord {
    public String khash;
    public String hash;

    @Override
    public String toString() {
        return khash + ":" + hash;
    }
}
