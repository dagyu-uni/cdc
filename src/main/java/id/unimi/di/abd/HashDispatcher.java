package id.unimi.di.abd;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class HashDispatcher {

    private int bit;
    private long partition;

    public HashDispatcher(int bit, long partition) {
        assert bit <= 64;
        this.bit = bit;
        this.partition = partition;
    }

    public static Stream<HashDispatcher> generate(int bit) {
        return IntStream.range(0, (int) Math.pow(2,bit)).mapToObj(e -> new HashDispatcher(bit,e));
    }

    public boolean isValid(byte[] hash) {
        long shifted_hash = ByteBuffer.wrap(hash).getLong() >> (64 - this.bit);
        return shifted_hash == getPartition();
    }

    public long getPartition(){
        return partition;
    }

}
