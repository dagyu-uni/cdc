package id.unimi.di.abd.adapter;

import java.io.*;

public abstract class TargetAdapter {

    private final String ext;
    private File file;
    private FileWriter fileWriter;

    private TargetAdapter(String name, String ext){
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
        String oldName = file.getName();
        String newName = oldName.replaceFirst(String.format("%s$",ext),"");
        renameFile(oldName, newName);
        getWriter().close();
    }

    protected abstract File createFile(String name);
    protected abstract File renameFile(String oldName, String newName);

}
