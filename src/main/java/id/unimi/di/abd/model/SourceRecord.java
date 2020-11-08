package id.unimi.di.abd.model;

import java.awt.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SourceRecord {
    public final String key;
    public final String value;

    public SourceRecord(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKHash() {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return digest.digest(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] getHash() {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return digest.digest(value.getBytes(StandardCharsets.UTF_8));
    }

    public String getKHashStringed() {
        return bytesToHex(getKHash());
    }

    public String getHashStringed() {
        return bytesToHex(getHash());
    }

    private String bytesToHex(byte[] hash){
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if(hex.length() == 1) hexString.append("0");
            hexString.append(hex);
        }
        return hexString.toString();
    }

    @Override
    public String toString() {
        return key + ":" + value;
    }

    public SyncRecord toSyncRecord() {
        SyncRecord syncRecord = new SyncRecord();
        syncRecord.khash = getKHashStringed();
        syncRecord.hash = getHashStringed();
        return syncRecord;
    }
}
