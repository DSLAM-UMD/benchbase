package com.oltpbenchmark.benchmarks.hot;

import java.util.Random;

import com.oltpbenchmark.distributions.CounterGenerator;

enum HotDistribution {
    CLUSTER, // hot keys are clustered together
    EVEN, // hot keys are evenly distributed
}

public class Partition {
    private Object id;
    private int from;
    private int to;
    private int hot;
    private HotDistribution distribution;
    // This is only used for the EVEN distribution.
    // Cache this value to avoid repeated division.
    private int hotInterval;
    private CounterGenerator insertCounter = new CounterGenerator(0);

    public Partition(Object id) {
        this(id, 0, 0, 0, HotDistribution.CLUSTER);
    }

    public Partition(Object id, int from, int to, int hot, HotDistribution distribution) {
        this.id = id;
        this.from = from;
        this.to = to;
        this.hot = Math.min(hot, to - from);
        this.distribution = distribution;
        if (distribution == HotDistribution.EVEN) {
            this.hotInterval = (to - from) / hot;
        }
    }

    public boolean isEmpty() {
        return from >= to;
    }

    public int getFrom() {
        return from;
    }

    public int getTo() {
        return to;
    }

    public Object getId() {
        return id;
    }

    public int nextHot(Random rng) {
        checkEmpty();
        if (hot <= 0) {
            return this.next(rng);
        }
        switch (distribution) {
            case CLUSTER:
                return rng.nextInt(hot) + from;
            case EVEN:
                return rng.nextInt(hot) * hotInterval + from;
            default:
                throw new IllegalStateException("Unknown distribution: " + distribution);
        }
    }

    public int nextCold(Random rng) {
        checkEmpty();
        if (to - from - hot <= 0) {
            return this.next(rng);
        }
        switch (distribution) {
            case CLUSTER:
                return rng.nextInt(to - from - hot) + from + hot;
            case EVEN:
                int intervalPick = rng.nextInt(hotInterval - 1) + 1;
                return rng.nextInt(hot) * hotInterval + from + intervalPick;
            default:
                throw new IllegalStateException("Unknown distribution: " + distribution);
        }
    }

    public int nextLatest(Random rng) {
        checkEmpty();
        int latest = to + this.insertCounter.lastInt();
        int offset = (hot <= 0) ? rng.nextInt(to - from) : rng.nextInt(hot);
        return latest - offset;
    }

    @Override
    public String toString() {
        return this.id + ": [" + this.from + ", " + this.to + ") Insert: " + this.insertCounter.lastInt();
    }

    public void setInsertCounterStartFromMaxKey(int numStrips, int maxKey) {
        int start = (maxKey - to) / numStrips + 1;
        this.insertCounter = new CounterGenerator(start);
    }

    public int nextInsert(int numStrips, int slot) {
        checkEmpty();
        int insertCount = this.insertCounter.nextInt();
        return to + insertCount * numStrips + slot;
    }

    private int next(Random rng) {
        checkEmpty();
        return rng.nextInt(to - from) + from;
    }

    private void checkEmpty() {
        if (isEmpty()) {
            throw new IllegalStateException("Partition is empty");
        }
    }
}