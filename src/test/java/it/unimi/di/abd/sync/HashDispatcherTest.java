package it.unimi.di.abd.sync;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("UnitTest")
public class HashDispatcherTest {

    @ParameterizedTest
    @CsvSource({
        "0,true,8",
        "1,false,8",
        "2,false,8",
        "3,false,8",
        "4,false,8",
        "0,false,9",
        "1,true,9",
        "2,false,9",
        "3,false,9",
        "4,false,9"
    })
    public void simpleTest(long partition, boolean expected, int bit){
        HashDispatcher hashDispatcher = new HashDispatcher(bit);
        //00000000 11111111 00000000 00000000 00000000 00000000 00000000 00000000
        byte[] hash = {0, -1, 0, 0, 0, 0, 0, 0};
        assertThat(hashDispatcher.getPartition(hash) == partition).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
        "255,true,8",
        "511,true,9",
        "1023,true,10"
    })
    public void moreComplexTest2(long partition, boolean expected, int bit){
        HashDispatcher hashDispatcher = new HashDispatcher(bit);
        //11111111 11111111 00000000 00000000 00000000 00000000 00000000 00000000
        byte[] hash = {-1, -1, 0, 0, 0, 0, 0, 0};
        assertThat(hashDispatcher.getPartition(hash) == partition).isEqualTo(expected);
    }


}
