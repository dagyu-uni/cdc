package id.unimi.di.abd;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import id.unimi.di.abd.adapter.TargetAdapter;
import id.unimi.di.abd.model.SourceRecord;
import id.unimi.di.abd.model.SyncRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.SubmissionPublisher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Tag("UnitTest")
public class SyncManagerTest {
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
        when(targetAdapter.getWriter()).thenReturn(stringWriter);
        when(hashDispatcher.isValid(any())).thenReturn(true);
        when(hashDispatcher.getPartition()).thenReturn((long) 1);
    }

    @Test
    public void loadSyncFileTest() throws IOException {
        createSyncJson("data01/before.csv");
        Stream<SourceRecord> recordStream = createSourceStream("data01/after.csv");
        String expected = getExpectedString("data01/change.json");

        SyncManager syncManager = new SyncManager.Builder()
                .setHashDispatcher(hashDispatcher)
                .setSyncJsonDir(dir)
                .setTargetAdapter(targetAdapter)
                .build();

        publishRecords(recordStream, syncManager);
        assertThat(expected).isEqualTo(stringWriter.toString());


    }

    private void publishRecords(Stream<SourceRecord> recordStream, SyncManager syncManager) {
        SubmissionPublisher<SourceRecord> publisher = new SubmissionPublisher<>();
        recordStream.forEach(publisher::submit);
        try {
            syncManager.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getExpectedString(String fileName) {
        StringBuilder stringBuilder = new StringBuilder();
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        BufferedReader streamReader =  new BufferedReader(new InputStreamReader(is));
        streamReader.lines().forEach(stringBuilder::append);
        return stringBuilder.toString();
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
