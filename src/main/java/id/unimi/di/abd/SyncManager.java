package id.unimi.di.abd;

import id.unimi.di.abd.adapter.TargetAdapter;

import java.io.File;
import java.util.concurrent.Flow;

public class SyncManager<SourceRecord>  extends Thread implements Flow.Subscriber<SourceRecord> {
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
        public Builder setHashDispatcher(HashDispatcher hashDispatcher) {
            return null;
        }

        public Builder setSyncJsonDir(File dir) {
            return null;
        }

        public Builder setTargetAdapter(TargetAdapter targetAdapter) {
            return  null;
        }

        public SyncManager build() {
            return  null;
        }
    }
}
