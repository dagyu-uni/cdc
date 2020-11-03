package id.unimi.di.abd.model;

import com.google.gson.Gson;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;

public class SyncRecord {
    public String khash;
    public String hash;

    public static HashMap<String,String> loadSyncJson(Reader reader){
        Gson gson = new Gson();
        SyncRecord[] records = gson.fromJson(reader,SyncRecord[].class);
        HashMap<String,String> dict = new HashMap<>();
        Arrays.stream(records).forEach(e -> dict.put(e.khash, e.hash));
        return dict;
    }
}
