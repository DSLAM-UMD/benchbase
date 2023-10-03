package com.oltpbenchmark.benchmarks.hot;

import java.util.Random;

import com.oltpbenchmark.distributions.CounterGenerator;

public class Partition {
    private Object id;
    private int from;
    private int to;
    private int hot;
    private CounterGenerator insertCounter = new CounterGenerator(0);

    public Partition(Object id, int from, int to, int hot) {
        this.id = id;
        this.from = from;
        this.to = to;
        this.hot = Math.min(hot, to - from);
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
        return (hot <= 0) ? this.next(rng) : (rng.nextInt(hot) + from);
    }

    public int nextCold(Random rng) {
        return (to - from - hot <= 0) ? this.next(rng) : (rng.nextInt(to - from - hot) + from + hot);
    }

    public int nextLatest(Random rng) {
        int latest = to + this.insertCounter.lastInt();
        int offset = (hot <= 0) ? rng.nextInt(to - from) : rng.nextInt(hot);
        return latest - offset;
    }

    public String toString() {
        return this.id + ": [" + this.from + ", " + this.to + ") Insert: " + this.insertCounter.lastInt();
    }

    public void setInsertCounterStartFromMaxKey(int numPartition, int maxKey) {
        int start = (maxKey - to) / numPartition + 1;
        this.insertCounter = new CounterGenerator(start);
    }

    public int nextInsert(int numPartitions, int homePartition) {
        int insertCount = this.insertCounter.nextInt();
        return to + insertCount * numPartitions + homePartition;
    }

    private int next(Random rng) {
        return rng.nextInt(to - from) + from;
    }
}