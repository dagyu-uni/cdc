package id.unimi.di.abd.model;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;

public class SyncRecord {
    public String khash;
    public String hash;

    @Override
    public String toString() {
        return khash + ":" + hash;
    }
}
