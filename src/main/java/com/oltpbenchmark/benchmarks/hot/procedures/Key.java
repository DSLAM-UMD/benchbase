package com.oltpbenchmark.benchmarks.hot.procedures;

public class Key {
  public int name;
  public String partition;

  public Key(int name, String partition) {
    this.name = name;
    this.partition = partition;
  }
}
