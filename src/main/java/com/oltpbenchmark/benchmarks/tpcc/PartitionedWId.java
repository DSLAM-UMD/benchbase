package com.oltpbenchmark.benchmarks.tpcc;

public class PartitionedWId {
    public String partition;
    public int id;

    public PartitionedWId(String partition, int id) {
        this.partition = partition;
        this.id = id;
    }

    @Override
    public String toString() {
        return partition + ":" + id;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof PartitionedWId))
            return false;
        PartitionedWId other = (PartitionedWId) o;
        return partition.equals(other.partition) && id == other.id;
    }
}
