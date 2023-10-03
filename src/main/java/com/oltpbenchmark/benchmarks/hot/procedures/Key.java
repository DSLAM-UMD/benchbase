package com.oltpbenchmark.benchmarks.hot.procedures;

import com.oltpbenchmark.benchmarks.hot.Partition;

public class Key {
  public int name;
  public Partition partition;

  public Key(int name, Partition partition) {
    this.name = name;
    this.partition = partition;
  }

  public Key convertToInsert(int numPartitions, int homePartition) {
    this.name = this.partition.nextInsert(numPartitions, homePartition);
    return this;
  }
}
