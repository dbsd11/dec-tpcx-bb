package io.bigdatabenchmark.v1.driver;

public class BigBenchRandom
{
    long seed;
    double y = Math.pow(2.0D, 64.0D);
    private final double LONG_TO_DOUBLE_2_POW_64_INV = 1.0D / this.y;

    public BigBenchRandom(long seed)
    {
        this.seed = seed;
    }

    public long nextLong()
    {
        this.seed ^= this.seed << 21;
        this.seed ^= this.seed >>> 35;
        this.seed ^= this.seed << 4;
        return this.seed;
    }

    public int nextInt(int n)
    {
        return (int)Math.floor((nextLong() * this.LONG_TO_DOUBLE_2_POW_64_INV + 0.5D) * n);
    }
}
