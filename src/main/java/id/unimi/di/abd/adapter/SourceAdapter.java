package id.unimi.di.abd.adapter;

import id.unimi.di.abd.model.SourceRecord;

import java.util.stream.Stream;

public interface SourceAdapter {
    Stream<SourceRecord> stream();
}
