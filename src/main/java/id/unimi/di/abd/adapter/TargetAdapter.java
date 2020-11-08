package id.unimi.di.abd.adapter;

import id.unimi.di.abd.model.SourceRecord;

import java.io.*;

public abstract class TargetAdapter {

    private final String ext;
    private File file;
    private FileWriter fileWriter;

    protected TargetAdapter(String name, String ext){
        this.ext = ext;
        this.file = createFile(
            TargetAdapter.appendExt(name,ext)
        );
    }

    public Writer getWriter() throws IOException {
        if(fileWriter == null)
            fileWriter = new FileWriter(file,true);
        return fileWriter;
    }

    public static String appendExt(String name, String ext) {
        return String.format("%s%s",name,ext);
    }

    public void finish() throws IOException {
        getWriter().flush();
        String oldName = file.getName();
        String newName = oldName.replaceFirst(String.format("%s$",ext),"");
        renameFile(oldName, newName);
        getWriter().close();
    }

    public void writeLogDelete(String k, String v) throws IOException {
        getWriter().write(logDelete(k, v));
    }
    public void writeLogInsert(SourceRecord item) throws IOException {
        getWriter().write(logInsert(item));
    }
    public void writeLogUpdate(SourceRecord item) throws IOException {
        getWriter().write(this.logUpdate(item));
    }

    protected abstract File createFile(String name);
    protected abstract File renameFile(String oldName, String newName);
    protected abstract String logUpdate(SourceRecord item);
    protected abstract String logInsert(SourceRecord item);
    protected abstract String logDelete(String k, String v);
}
