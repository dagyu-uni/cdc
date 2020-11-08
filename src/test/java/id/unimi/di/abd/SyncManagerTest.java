package id.unimi.di.abd;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        long partition = 1;
        createSyncJson(String.format("%s/before.csv", path),partition);
        Stream<SourceRecord> recordStream = createSourceStream(String.format("%s/after.csv", path));
        HashMap<String, Integer> map = getExpectedMap(String.format("%s/change.csv", path));

        SyncManager syncManager = new SyncManager.Builder()
                .setPartition(partition)
                .setSyncJsonDir(dir)
                .setMoveAndRenamePattern(moveAndRenamePattern)
                .build();

        recordStream.forEach(syncManager::submit);
        syncManager.finish();

        verify(moveAndRenamePattern, times(map.get(INSERT)))
                .write(any(),eq(SQLop.INSERT));
        verify(moveAndRenamePattern, times(map.get(UPDATE)))
                .write(any(),eq(SQLop.UPDATE));
        verify(moveAndRenamePattern, times(map.get(DELETE)))
                .write(any(), any() ,eq(SQLop.DELETE));

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
                .map(e -> e.split(","))
                .map(e -> new SourceRecord(e[0],e[1]));
    }

    private void createSyncJson(String fileName, long partition) throws IOException {
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
        Gson gson = new GsonBuilder().create();
        FileWriter writer = new FileWriter(new File(dir, String.format("sync%d.json",partition)));
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
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append("0");
            hexString.append(hex);
        }
        return hexString.toString();
    }

}
