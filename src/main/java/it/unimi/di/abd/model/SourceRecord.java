package it.unimi.di.abd.model;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.Arrays;

public class SourceRecord {
    private final JsonArray keys;
    private final JsonObject value;
    private final byte[] hash;
    private final byte[] khash;


    public SourceRecord(String row, String... keys){
        Gson gson = new Gson();
        this.keys = new JsonArray();
        JsonObject key = new JsonObject();
        this.value = gson.fromJson(row, JsonObject.class);

        Arrays.stream(keys).forEach(e -> {
            JsonElement element = this.value.get(e);
            key.add(e, element);
            this.keys.add(e);
        });

        this.khash = calculateHash(key.toString());
        this.hash = calculateHash(this.value.toString());
    }

    private byte[] calculateHash(String s) {
        return getMessageDigest().digest(s.getBytes(StandardCharsets.UTF_8));
    }

    public MessageDigest getMessageDigest(){
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public byte[] getKHash() {
        return khash;
    }

    public byte[] getHash() {
        return hash;
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
        return toJson(new JsonObject()).toString();
    }

    public SyncRecord toSyncRecord() {
        SyncRecord syncRecord = new SyncRecord();
        syncRecord.khash = getKHashStringed();
        syncRecord.hash = getHashStringed();
        return syncRecord;
    }

    public JsonObject toJson(JsonObject ctx) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("khash", getKHashStringed());
        jsonObject.addProperty("hash",getHashStringed());
        jsonObject.addProperty("ts_load", LocalDateTime.now().toString());
        jsonObject.addProperty("ts_key","");
        jsonObject.add("ctx", ctx);
        jsonObject.add("row_keys", keys);
        jsonObject.add("row", value);
        return jsonObject;
    }
}
