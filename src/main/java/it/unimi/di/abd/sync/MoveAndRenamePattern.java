package it.unimi.di.abd.sync;

import it.unimi.di.abd.adapter.TargetAdapter;
import it.unimi.di.abd.model.SourceRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MoveAndRenamePattern {
    // 5MB
    private static final int MAX_SIZE = 5000000;
    private final String ext;
    private final TargetAdapter targetAdapter;
    private final String name;
    private final AtomicInteger size;
    private final AtomicInteger counter;
    private List<File> files;
    private FileWriter fileWriter;

    public MoveAndRenamePattern(TargetAdapter targetAdapter, String name, String ext){
        this.name = name;
        this.ext = ext;
        this.targetAdapter = targetAdapter;
        this.size = new AtomicInteger(0);
        this.counter = new AtomicInteger(0);
    }

    public static void clear(TargetAdapter targetAdapter, String ext) {
        targetAdapter.removeAllFilesEndingWith(ext);
    }

    public Writer getWriter() throws IOException {
        if(fileWriter == null){
            File file = targetAdapter.createFile(
                    String.format("%s_%d%s",name,counter.getAndIncrement(),ext)
            );
            getFileList().add(file);
            fileWriter = new FileWriter(file,true);
        }
        return fileWriter;
    }

    private List<File> getFileList() {
        if(this.files == null)
            files = new LinkedList<>();
        return files;
    }


    public void write(SourceRecord item) {
        String toAdd = item.toString();
        try {
            checkSize(toAdd);
            Writer writer = getWriter();
            writer.write(toAdd);
            writer.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkSize(String toAdd) throws IOException {
        if(size.addAndGet(toAdd.length()) > MAX_SIZE){
            fileWriter.flush();
            fileWriter.close();
            this.fileWriter = null;
            size.set(0);
        }
    }


    public void finish() throws IOException {
        if(fileWriter != null){
            fileWriter.flush();
            fileWriter.close();
            renameFiles();
        }
    }

    private void renameFiles() {
        getFileList().forEach(e -> {
            String oldName = e.getName();
            String newName = oldName.replaceFirst(String.format("%s$",ext),"");
            targetAdapter.renameFile(oldName, newName);
        });
    }


}
