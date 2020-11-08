package id.unimi.di.abd;

import id.unimi.di.abd.adapter.TargetAdapter;
import id.unimi.di.abd.model.SourceRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class MoveAndRenamePattern {
    private final String ext;
    private final TargetAdapter targetAdapter;
    private final String name;
    private File file;
    private FileWriter fileWriter;


    public MoveAndRenamePattern(TargetAdapter targetAdapter, String name, String ext){
        this.name = name;
        this.ext = ext;
        this.targetAdapter = targetAdapter;
    }

    public static void clear(TargetAdapter targetAdapter, String ext) {
        targetAdapter.removeAllFilesEndingWith(ext);
    }

    public Writer getWriter() throws IOException {
        if(fileWriter == null){
            this.file = targetAdapter.createFile(String.format("%s%s",name,ext));
            fileWriter = new FileWriter(file,true);
        }
        return fileWriter;
    }


    private void write(String s) {
        try {
            Writer writer = getWriter();
            writer.write(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void finish() throws IOException {
        if(fileWriter != null){
            fileWriter.flush();
            fileWriter.close();
            String oldName = file.getName();
            String newName = oldName.replaceFirst(String.format("%s$",ext),"");
            targetAdapter.renameFile(oldName, newName);
        }
    }


    public String logUpdate(SourceRecord item) {
        return String.format("UPDATE: %s\n",item);
    }


    public String logInsert(SourceRecord item) {
        return String.format("INSERT: %s\n",item);
    }

    public String logDelete(String k, String v) {
        return String.format("DELETE: %s\n",k);
    }

    public void write(SourceRecord item, SQLop sqLop) {
        if(sqLop == SQLop.UPDATE){
            write(logUpdate(item));
        } else if(sqLop == SQLop.INSERT){
            write(logInsert(item));
        }
    }

    public void write(String k, String v, SQLop sqLop){
        if(sqLop == SQLop.DELETE){
            write(logDelete(k,v));
        }
    }
}
