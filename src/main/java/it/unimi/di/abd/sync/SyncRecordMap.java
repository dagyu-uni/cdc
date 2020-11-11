package it.unimi.di.abd.sync;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import it.unimi.di.abd.model.SourceRecord;
import it.unimi.di.abd.model.SyncRecord;

import java.io.*;
import java.util.*;


public class SyncRecordMap {

    private final File syncFile;
    private HashMap<String, String> dict;
    private List<SyncRecord> syncRecordList = new ArrayList<>();

    public SyncRecordMap(File file){
        this.syncFile = file;
        try {
            FileReader reader = new FileReader(file);
            initFromReader(reader);
        } catch (FileNotFoundException e) {
            this.dict = new HashMap<>();
        }
    }

    private void initFromReader(Reader reader) {
        Gson gson = new Gson();
        HashMap<String,String> dict = new HashMap<>();
        SyncRecord[] records = null;
        try{
            records = gson.fromJson(reader,SyncRecord[].class);
        } catch (JsonIOException | JsonSyntaxException e){
            e.printStackTrace();
        }
        if(records != null)
            Arrays.stream(records).forEach(e -> dict.put(e.khash, e.hash));
        this.dict = dict;
    }

    public boolean isChanged(SourceRecord item) {
        syncRecordList.add(item.toSyncRecord());
        return isInserted(item) || isUpdated(item);
    }

    private boolean isInserted(SourceRecord item) {
        return !dict.containsKey(item.getKHashStringed());
    }

    private boolean isUpdated(SourceRecord item) {
        String v = this.dict.remove(item.getKHashStringed());
        return !v.equals(item.getHashStringed());
    }

    public void finish() {
        try{
            Gson gson = new Gson();
            FileWriter fileWriter = new FileWriter(syncFile);
            gson.toJson(syncRecordList, fileWriter);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e){}
    }
}


