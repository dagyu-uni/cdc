package it.unimi.di.abd.sync;

import java.math.BigInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class HashDispatcher {
    private int bit;

    public HashDispatcher(int bit) {
        assert bit <= 64;
        this.bit = bit;
    }

    public static Stream<Long> generate(int bit) {
        return LongStream.range(0, (int) Math.pow(2,bit)).boxed();
    }

    public long getPartition(byte[] hash) {
        int bits = (hash.length * 8) - this.bit;
        BigInteger b = new BigInteger(1,hash).shiftRight(bits);
        return b.longValue();
    }


}
