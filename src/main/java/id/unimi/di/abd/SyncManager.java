package id.unimi.di.abd;

import id.unimi.di.abd.adapter.TargetAdapter;

import java.io.File;
import java.util.concurrent.Flow;

public class SyncManager<SourceRecord>  extends Thread implements Flow.Subscriber<SourceRecord> {
    private final HashDispatcher hashDispatcher;
    private final File syncFile;
    private final TargetAdapter targetAdapter;

    private SyncManager(HashDispatcher hashDispatcher, File syncFile, TargetAdapter targetAdapter) {
        this.hashDispatcher = hashDispatcher;
        this.syncFile = syncFile;
        this.targetAdapter = targetAdapter;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {

    }

    @Override
    public void onNext(SourceRecord item) {

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

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
