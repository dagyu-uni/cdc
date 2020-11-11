package it.unimi.di.abd;

import it.unimi.di.abd.adapter.SourceAdapter;
import it.unimi.di.abd.adapter.TargetAdapter;
import it.unimi.di.abd.sync.HashDispatcher;
import it.unimi.di.abd.sync.MoveAndRenamePattern;
import it.unimi.di.abd.sync.SyncManager;

import java.io.File;
import java.util.HashMap;

public class CDC {
    public static final int BIT = 4;
    public static final String EXT = ".tmp";
    private final SourceAdapter sourceAdapter;
    private final TargetAdapter targetAdapter;
    private final File dir;
    private HashMap<Long, SyncManager> dispatcherMap;

    public CDC(SourceAdapter sourceAdapter, TargetAdapter targetAdapter, File dir){
        this.sourceAdapter = sourceAdapter;
        this.targetAdapter = targetAdapter;
        this.dir = dir;
        this.dir.mkdir();

    }

    public void run() {

        MoveAndRenamePattern.clear(targetAdapter,EXT);

        HashDispatcher dispatcher = createDispatcherMap();

        sourceAdapter.stream().parallel().forEach(e -> {
            long dispatchedPartition = dispatcher.getPartition(e.getKHash());
            SyncManager syncManager = dispatcherMap.get(dispatchedPartition);
            syncManager.submit(e);
        });

        dispatcherMap.values().parallelStream().forEach(SyncManager::finish);
    }

    private HashDispatcher createDispatcherMap() {
        this.dispatcherMap = new HashMap<>();

        HashDispatcher.generate(BIT).forEach(e -> {
            MoveAndRenamePattern moveAndRenamePattern =
                    new MoveAndRenamePattern(targetAdapter, String.format("%d_%d", System.currentTimeMillis(),e), EXT);
            SyncManager syncManager = new SyncManager.Builder()
                    .setPartition(e)
                    .setSyncJsonDir(dir)
                    .setMoveAndRenamePattern(moveAndRenamePattern)
                    .build();
            dispatcherMap.put(e, syncManager);
        });
        return new HashDispatcher(BIT);
    }
}
