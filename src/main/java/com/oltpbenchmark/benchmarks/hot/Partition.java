package com.oltpbenchmark.benchmarks.hot;

import java.util.Random;

import com.oltpbenchmark.distributions.CounterGenerator;

public class Partition {
    private Object id;
    private int from;
    private int to;
    private int hot;
    private CounterGenerator insertCounter = new CounterGenerator(0);

    public Partition(Object id) {
        this(id, 0, 0, 0);
    }

    public Partition(Object id, int from, int to, int hot) {
        this.id = id;
        this.from = from;
        this.to = to;
        this.hot = Math.min(hot, to - from);
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
        return (hot <= 0) ? this.next(rng) : (rng.nextInt(hot) + from);
    }

    public int nextCold(Random rng) {
        checkEmpty();
        return (to - from - hot <= 0) ? this.next(rng) : (rng.nextInt(to - from - hot) + from + hot);
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

    public void setInsertCounterStartFromMaxKey(int numSlots, int maxKey) {
        int start = (maxKey - to) / numSlots + 1;
        this.insertCounter = new CounterGenerator(start);
    }

    public int nextInsert(int numSlots, int slot) {
        checkEmpty();
        int insertCount = this.insertCounter.nextInt();
        return to + insertCount * numSlots + slot;
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