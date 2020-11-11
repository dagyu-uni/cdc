package it.unimi.di.abd.sync;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import it.unimi.di.abd.model.SourceRecord;
import it.unimi.di.abd.model.SyncRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Tag("IntegrationTest")
public class SyncManagerTest {
    public static final String DELETE = "DELETE";
    public static final String INSERT = "INSERT";
    public static final String UPDATE = "UPDATE";
    public static final long partition = 1;
    @TempDir
    File dir;
    @Mock
    MoveAndRenamePattern moveAndRenamePattern;
    StringWriter stringWriter;


    @BeforeEach
    public void setUp()  {
        stringWriter = new StringWriter();
    }

    @ParameterizedTest
    @CsvSource({
        "data01",
        "data02",
        "data03",
        "data04"
    })
    public void syncFlowTest(String path) throws IOException {
        Stream<SourceRecord> afterSourceRecordStream = createSourceStream(String.format("%s/after.jsonl", path));
        Stream<SourceRecord> beforeSourceRecordStream = createSourceStream(String.format("%s/before.jsonl", path));
        HashMap<String, Integer> map = getExpectedMap(String.format("%s/change.csv", path));
        int expectedTimes = map.get(INSERT) + map.get(UPDATE);
        verifyThatChangeIsCaptured(afterSourceRecordStream, beforeSourceRecordStream, expectedTimes);
    }


    private void verifyThatChangeIsCaptured(Stream<SourceRecord> afterSourceRecordStream, Stream<SourceRecord> beforeSourceRecordStream, int expectedTimes) throws IOException {
        createSyncJson(beforeSourceRecordStream, partition);
        SyncManager syncManager = new SyncManager.Builder()
                .setPartition(partition)
                .setSyncJsonDir(dir)
                .setMoveAndRenamePattern(moveAndRenamePattern)
                .build();

        afterSourceRecordStream.forEach(syncManager::submit);
        syncManager.finish();
        verify(moveAndRenamePattern, times(expectedTimes)).write(any());
    }


    private HashMap<String, Integer> getExpectedMap(String fileName) {
        HashMap<String, Integer> map = new HashMap<>();
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        BufferedReader streamReader = new BufferedReader(new InputStreamReader(is));
        streamReader
                .lines()
                .map(e -> e.split(","))
                .forEach(e -> map.put(e[0], Integer.parseInt(e[1])))

        ;

        map.putIfAbsent(DELETE, 0);
        map.putIfAbsent(INSERT, 0);
        map.putIfAbsent(UPDATE, 0);

        return map;
    }

    private Stream<SourceRecord> createSourceStream(String fileName) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        BufferedReader streamReader =  new BufferedReader(new InputStreamReader(is));
        return streamReader
                .lines()
                .map(e -> new SourceRecord(e, "key"));
    }

    private void createSyncJson(Stream<SourceRecord> sourceRecordStream, long partition) throws IOException {
        List<SyncRecord> syncRecordList = sourceRecordStream
                .map(e -> {
                    SyncRecord record = new SyncRecord();
                    record.khash = e.getKHashStringed();
                    record.hash = e.getHashStringed();
                    return record;
                }).collect(Collectors.toList());
        Gson gson = new GsonBuilder().create();
        FileWriter writer = new FileWriter(new File(dir, String.format("sync%d.json",partition)));
        gson.toJson(syncRecordList, writer);
        writer.flush();
        writer.close();
    }

}
