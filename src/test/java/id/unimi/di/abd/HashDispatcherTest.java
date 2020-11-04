package id.unimi.di.abd;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("UnitTest")
public class HashDispatcherTest {

    @ParameterizedTest
    @CsvSource({
        "0,true",
        "1,false",
        "2,false",
        "3,false",
        "4,false"
    })
    public void simpleTest(long partition, boolean expected){
        int bit = 4;
        HashDispatcher hashDispatcher = new HashDispatcher(bit,partition);
        //00001000
        byte[] hash = {0x0, 0x8};
        assertThat(hashDispatcher.isValid(hash)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
            "0,false",
            "1,true",
            "2,false",
            "3,false",
            "4,false"
    })
    public void moreComplexTest(long partition, boolean expected){
        int bit = 5;
        HashDispatcher hashDispatcher = new HashDispatcher(bit,partition);
        //00001000
        byte[] hash = {0x0, 0x8};
        assertThat(hashDispatcher.isValid(hash)).isEqualTo(expected);
    }

}
