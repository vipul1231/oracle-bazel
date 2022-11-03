package com.example.ibf;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * Functions for using the One Hashing Bloom Filter (OHBF) [1] scheme with InvertibleBloomFilter. The OHBF scheme
 * computes a single hash function on the element being inserted and then uses k-consecutive prime numbers as divisors
 * with the modulus operator to compute the k independent hash functions for cell indices.
 *
 * <p>In practice, the elements we will be using with InvertibleBloomFilter for database replication contain a hash
 * value, the row hash, so we can use the element value directly (i.e., the identity hash) as the one hash function.
 *
 * <p>[1] lu, Jianyuan & Yang, Tong & Wang, Yi & Dai, Huichen & Jin, Linxiao & Song, Haoyu & Liu, Bin. (2015).
 * One-Hashing Bloom Filter. 10.1109/IWQoS.2015.7404748.
 */
public class OneHashingBloomFilterUtils {
    private OneHashingBloomFilterUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static int totalCellCount(long[] divisors) {
        return Math.toIntExact(Arrays.stream(divisors).sum());
    }

    public static Function<Long, long[]> indexHashes(long[] divisors) {
        long[] offsets = partitionOffsets(divisors);

        return rowHashSum ->
                IntStream.range(0, divisors.length)
                        .mapToLong(i -> Math.abs(rowHashSum % divisors[i]) + offsets[i])
                        .toArray();
    }

    public static long[] resizingDivisors(int k, int smallCellCount, int[] resizingFactors) {
        long[] divisors = new long[k];
        long[] primeDivisors = primeDivisors(k, smallCellCount);
        for (int i = 0; i < k; i++) {
            divisors[i] = primeDivisors[i] * resizingFactors[i];
        }

        return divisors;
    }

    public static long[] primeDivisors(int k, int requestedCellCount) {
        return nPrimeNumbersAfter(k, requestedCellCount / 3 + 1);
    }

    public static long[] partitionOffsets(long[] divisors) {
        return IntStream.range(0, divisors.length).mapToLong(i -> Arrays.stream(divisors).limit(i).sum()).toArray();
    }

    private static long[] nPrimeNumbersAfter(int n, int start) {
        return LongStream.iterate(start, i -> i + 1).filter(OneHashingBloomFilterUtils::isPrime).limit(n).toArray();
    }

    private static boolean isPrime(long number) {
        return LongStream.rangeClosed(2, (int) (Math.sqrt(number))).allMatch(n -> number % n != 0);
    }
}
