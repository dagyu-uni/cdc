package id.unimi.di.abd;

import id.unimi.di.abd.adapter.SourceAdapter;
import id.unimi.di.abd.adapter.TargetAdapter;
import id.unimi.di.abd.model.SourceRecord;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CDC extends Thread{
    private final SourceAdapter sourceAdapter;
    private final TargetAdapter targetAdapter;

    public CDC(SourceAdapter sourceAdapter, TargetAdapter targetAdapter){
        this.sourceAdapter = sourceAdapter;
        this.targetAdapter = targetAdapter;
    }

    @Override
    public void run() {
        SubmissionPublisher<SourceRecord> publisher = new SubmissionPublisher<>();
        List<HashDispatcher> list = HashDispatcher.generate(4).collect(Collectors.toList());
        ExecutorService executorService = Executors.newFixedThreadPool(list.size() + 1);
        list.forEach(e -> {
             SyncManager syncManager = new SyncManager.Builder()
                     .setSyncJsonDir(new File("./"))
                     .setHashDispatcher(e)
                     .setTargetAdapter(targetAdapter)
                     .build();
             publisher.subscribe(syncManager);
             executorService.execute(syncManager);
        });
        executorService.execute(() -> sourceAdapter.stream().forEach(publisher::submit));
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
