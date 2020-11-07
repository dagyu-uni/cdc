package id.unimi.di.abd;

import id.unimi.di.abd.adapter.TargetAdapter;
import id.unimi.di.abd.model.SourceRecord;
import id.unimi.di.abd.model.SyncRecord;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.concurrent.Flow;

enum SQLop {
    UPDATE,
    INSERT,
    NOP
}

public class SyncManager implements Flow.Subscriber<SourceRecord> {
    private final HashDispatcher hashDispatcher;
    private final File syncFile;
    private final TargetAdapter targetAdapter;
    private Flow.Subscription subscription;

    private HashMap<String, String> syncRecords;

    private SyncManager(HashDispatcher hashDispatcher, File syncFile, TargetAdapter targetAdapter) {
        this.hashDispatcher = hashDispatcher;
        this.syncFile = syncFile;
        this.targetAdapter = targetAdapter;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        InputStream is = null;
        try {
            is = new FileInputStream(syncFile.getPath());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        BufferedReader streamReader = new BufferedReader(new InputStreamReader(is));
        syncRecords = SyncRecord.loadSyncJson(streamReader);

        subscription.request(1);
    }

    @Override
    public void onNext(SourceRecord item) {
        try {
            if(hashDispatcher.isValid(item.getKHash())) {
                switch(compareSQLop(syncRecords, item)) {
                    case UPDATE:
                        targetAdapter.writeLogUpdate(item);
                        break;
                    case INSERT:
                        targetAdapter.writeLogInsert(item);
                        break;
                    case NOP:
                        break;
                }
            }
        } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }

        subscription.request(1);
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {
        this.syncRecords.forEach((k, v) -> {
            try {
                this.targetAdapter.writeLogDelete(k, v);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private SQLop compareSQLop(HashMap<String, String> syncRecords, SourceRecord sourceRecord) {
        try {
            if(!syncRecords.containsKey(sourceRecord.getKHashStringed())) {
                return SQLop.INSERT;
            }

            String v = syncRecords.get(sourceRecord.getKHashStringed());
            this.syncRecords.remove(sourceRecord.getKHashStringed());

            if(!v.equals(sourceRecord.getHashStringed())) {
                return SQLop.UPDATE;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return SQLop.NOP;
    }

    public static class Builder {

        private HashDispatcher hashDispatcher;
        private File syncFile;
        private TargetAdapter targetAdapter;

        public Builder setHashDispatcher(HashDispatcher hashDispatcher) {
            this.hashDispatcher = hashDispatcher;
            return this;
        }

        public Builder setSyncJsonDir(File dir) {
            String fileName = String.format("sync%d.json",this.hashDispatcher.getPartition());
            this.syncFile = new File(dir,fileName);
            return this;
        }

        public Builder setTargetAdapter(TargetAdapter targetAdapter) {
            this.targetAdapter = targetAdapter;
            return this;
        }

        public SyncManager build() {
            return new SyncManager(hashDispatcher, syncFile, targetAdapter);
        }
    }
}
