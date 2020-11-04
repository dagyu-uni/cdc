package id.unimi.di.abd;

import java.util.stream.IntStream;
import java.util.stream.Stream;

public class HashDispatcher {

    public HashDispatcher(int bit, long partition) {
    }

    public static Stream<HashDispatcher> generate(int bit) {
        return IntStream.range(0, (int) Math.pow(2,bit)).mapToObj(e -> new HashDispatcher(bit,e));
    }

    public boolean isValid(byte[] hash) {
        return false;
    }

    public long getPartition(){
        return 0;
    }

}
