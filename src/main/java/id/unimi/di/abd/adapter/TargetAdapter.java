package id.unimi.di.abd.adapter;


import java.io.File;

public interface TargetAdapter {
    File createFile(String pathName);
    File renameFile(String oldPathName, String newPathName);
    void removeAllFilesEndingWith(String ext);
}
