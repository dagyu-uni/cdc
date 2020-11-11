package it.unimi.di.abd.adapter;

import it.unimi.di.abd.model.SourceRecord;

import java.util.stream.Stream;

public interface SourceAdapter {
    Stream<SourceRecord> stream();
}
