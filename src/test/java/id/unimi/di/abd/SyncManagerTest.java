package id.unimi.di.abd;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import id.unimi.di.abd.adapter.TargetAdapter;
import id.unimi.di.abd.model.SourceRecord;
import id.unimi.di.abd.model.SyncRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.SubmissionPublisher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@Tag("UnitTest")
public class SyncManagerTest {
    public static final String DELETE = "DELETE";
    public static final String INSERT = "INSERT";
    public static final String UPDATE = "UPDATE";
    @TempDir
    File dir;
    @Mock
    TargetAdapter targetAdapter;
    @Mock
    HashDispatcher hashDispatcher;
    StringWriter stringWriter;


    @BeforeEach
    public void setUp() throws IOException {
        stringWriter = new StringWriter();

        when(hashDispatcher.isValid(any())).thenReturn(true);
        when(hashDispatcher.getPartition()).thenReturn((long) 1);
    }

    @ParameterizedTest
    @CsvSource({
        "data01",
        "data02",
        "data03",
        "data04"
    })
    public void syncFlowTest(String path) throws IOException, InterruptedException {
        createSyncJson(String.format("%s/before.csv", path));
        Stream<SourceRecord> recordStream = createSourceStream(String.format("%s/after.csv", path));
        HashMap<String, Integer> map = getExpectedMap(String.format("%s/change.csv", path));

        SyncManager syncManager = new SyncManager.Builder()
                .setHashDispatcher(hashDispatcher)
                .setSyncJsonDir(dir)
                .setTargetAdapter(targetAdapter)
                .build();

        publishRecords(recordStream, syncManager);
        Thread.sleep(1000);
        verify(targetAdapter, times(map.get(DELETE))).writeLogDelete(any(), any());
        verify(targetAdapter, times(map.get(INSERT))).writeLogInsert(any());
        verify(targetAdapter, times(map.get(UPDATE))).writeLogUpdate(any());

    }

    private void publishRecords(Stream<SourceRecord> recordStream, SyncManager syncManager) {
        SubmissionPublisher<SourceRecord> publisher = new SubmissionPublisher<>();
        publisher.subscribe(syncManager);
        recordStream.forEach(publisher::submit);
        publisher.close();
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

    private Stream<SourceRecord> createSourceStream(String fileName) throws IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        BufferedReader streamReader =  new BufferedReader(new InputStreamReader(is));
        return streamReader
                .lines()
                .map(e -> e.split(","))
                .map(e -> new SourceRecord(e[0],e[1]));
    }

    private void createSyncJson(String fileName) throws IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        BufferedReader streamReader =  new BufferedReader(new InputStreamReader(is));
        List<SyncRecord> syncRecordList = streamReader
                .lines()
                .map(e -> e.split(","))
                .map(e -> {
                    SyncRecord record = new SyncRecord();
                    record.khash = hash(e[0]);
                    record.hash = hash(e[1]);
                    return record;
                }).collect(Collectors.toList());
        long partition = hashDispatcher.getPartition();
        Gson gson = new GsonBuilder().create();
        FileWriter writer = new FileWriter(new File(dir, String.format("sync%d.json", partition)));
        gson.toJson(syncRecordList, writer);
        writer.flush();
        writer.close();
    }

    private String hash(String s) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA256");
            byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }

    private String bytesToHex(byte[] hash){
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if(hex.length() == 1) hexString.append("0");
            hexString.append(hex);
        }
        return hexString.toString();
    }

}
