package id.unimi.di.abd;

import id.unimi.di.abd.model.SourceRecord;
import java.io.*;

public class SyncManager {
    private final File syncFile;
    private final MoveAndRenamePattern moveAndRenamePattern;

    private SyncRecordMap syncRecords;

    private SyncManager(File syncFile, MoveAndRenamePattern moveAndRenamePattern) {
        this.syncFile = syncFile;
        this.moveAndRenamePattern = moveAndRenamePattern;
        this.syncRecords = new SyncRecordMap(syncFile);
    }

    public synchronized void submit(SourceRecord item) {
        SQLop sqLop = syncRecords.submit(item);
        moveAndRenamePattern.write(item, sqLop);
    }

    public void finish() {
        try {
            moveAndRenamePattern.finish();
            syncRecords.finish(moveAndRenamePattern);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Builder {

        private File syncFile;
        private MoveAndRenamePattern moveAndRenamePattern;
        private Long partition;

        public Builder setPartition(Long partition) {
            this.partition = partition;
            return this;
        }

        public Builder setSyncJsonDir(File dir) {
            String fileName = String.format("sync%d.json",this.partition);
            this.syncFile = new File(dir,fileName);
            try {
                this.syncFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return this;
        }

        public Builder setMoveAndRenamePattern(MoveAndRenamePattern moveAndRenamePattern) {
            this.moveAndRenamePattern = moveAndRenamePattern;
            return this;
        }

        public SyncManager build() {
            return new SyncManager(syncFile, moveAndRenamePattern);
        }
    }
}
