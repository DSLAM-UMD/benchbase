package com.oltpbenchmark.benchmarks.hot;

import java.util.Optional;
import java.util.Random;

public class Partition {
    private String id;
    private int from;
    private int to;
    private int hot;
    private Optional<Class<?>> idType;

    public Partition(String id, int from, int to, int hot, Optional<Class<?>> idType) {
        this.id = id;
        this.from = from;
        this.to = to;
        this.hot = Math.min(hot, to - from);
        this.idType = idType;
    }

    public Optional<Class<?>> getIdType() {
        return idType;
    }

    public String getStringId() {
        assert idType.isPresent() && idType.get() == String.class;
        return id;
    }

    public int getIntId() {
        assert idType.isPresent() && idType.get() == Integer.class;
        return Integer.parseInt(id);
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
        return this.id + ": [" + this.from + ", " + this.to + ")";
    }

    private int next(Random rng) {
        return rng.nextInt(to - from) + from;
    }
}
