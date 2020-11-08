package id.unimi.di.abd;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import id.unimi.di.abd.model.SourceRecord;
import id.unimi.di.abd.model.SyncRecord;

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

    public SQLop submit(SourceRecord item) {
        SQLop sqLop = SQLop.NOP;
        if(isInserted(item)) {
            sqLop = SQLop.INSERT;
        } else if(isUpdated(item)) {
            sqLop = SQLop.UPDATE;
        }
        syncRecordList.add(item.toSyncRecord());
        return sqLop;

    }

    private boolean isInserted(SourceRecord item) {
        return !dict.containsKey(item.getKHashStringed());
    }

    private boolean isUpdated(SourceRecord item) {
        String v = this.dict.remove(item.getKHashStringed());
        return !v.equals(item.getHashStringed());
    }

    public void finish(MoveAndRenamePattern moveAndRenamePattern) {
        this.dict.forEach((k, v) -> moveAndRenamePattern.write(k,v, SQLop.DELETE));
        try{
            Gson gson = new Gson();
            FileWriter fileWriter = new FileWriter(syncFile);
            gson.toJson(syncRecordList, fileWriter);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e){}
    }
}


