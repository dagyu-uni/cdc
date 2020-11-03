package id.unimi.di.abd.model;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SourceRecordTest {

    @ParameterizedTest
    @CsvSource({
        "chiave,valore",
        "città,Milano"
    })
    public void hashTest(String key, String value) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA256");
        byte[] kHash = digest.digest(key.getBytes(StandardCharsets.UTF_8));
        byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
        SourceRecord sourceRecord = new SourceRecord(key,value);
        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(sourceRecord.getKHash()).isEqualTo(kHash);
        softAssertions.assertThat(sourceRecord.getHash()).isEqualTo(hash);
        softAssertions.assertAll();
    }
}
