package com.oltpbenchmark.benchmarks.hot;

import java.util.Random;

public class Partition {
    private int from;
    private int to;
    private int hot;
    
    public Partition(int from, int to, int hot) {
        this.from = from;
        this.to = to;
        this.hot = Math.min(hot, to - from);
    }

    public int nextHot(Random rng) {
        return (hot <= 0) ? this.next(rng) : (rng.nextInt(hot) + from);
    }

    public int nextCold(Random rng) {
        return (to - from - hot <= 0) ? this.next(rng) : (rng.nextInt(to - from - hot) + from + hot);
    }

    public boolean isIncludedIn(int from, int to) {
        return from <= this.from && this.to <= to; 
    }

    public String toString() {
        return "[" + this.from + ", " + this.to + ")";
    }

    private int next(Random rng) {
        return rng.nextInt(to - from) + from;
    }
}
