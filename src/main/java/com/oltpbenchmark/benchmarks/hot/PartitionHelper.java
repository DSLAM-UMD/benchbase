package com.oltpbenchmark.benchmarks.hot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;

public class PartitionHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionHelper.class);

  private HOTBenchmark benchmark;
  private int partitionSize;

  private Optional<List<Integer>> citusShards;

  public PartitionHelper(HOTBenchmark benchmark) {
    double scaleFactor = benchmark.getWorkloadConfiguration().getScaleFactor();
    int numRecords = (int) Math.round(HOTConstants.RECORD_COUNT * scaleFactor);

    this.benchmark = benchmark;
    this.partitionSize = numRecords / benchmark.numRegions;

    this.citusShards = Optional.empty();
  }

  public void setPartition(PreparedStatement stmt, int row) {
    try {
      DatabaseType dbType = benchmark.getWorkloadConfiguration().getDatabaseType();
      switch (dbType) {
        case CITUS:
          if (this.citusShards.isEmpty()) {
            computeCitusShards();
            LOG.info("Citus shards: {}", this.citusShards.get());
          }
          int shard = row / this.partitionSize;
          stmt.setInt(12, citusShards.get().get(shard));
          break;
        default:
          break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public List<Partition> getPartitions(Table tbl) throws SQLException {
    DatabaseType dbType = benchmark.getWorkloadConfiguration().getDatabaseType();
    String tableName = (dbType.shouldEscapeNames() ? tbl.getEscapedName() : tbl.getName());
    String partitionRangesQuery;
    switch (dbType) {
      case POSTGRES:
        boolean hasRegionColumn = false;
        String checkRegionColumnQuery = String.format("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='pg_class' and column_name='relregion';
            """);

        try (Connection metaConn = benchmark.makeConnection();
            Statement stmt = metaConn.createStatement();
            ResultSet res = stmt.executeQuery(checkRegionColumnQuery)) {
          hasRegionColumn = res.next();
        }

        partitionRangesQuery = String.format("""
              with partitions as (select i.inhrelid as partoid
                                  from pg_inherits i
                                  join pg_class cl on i.inhparent = cl.oid
                                  where cl.relname = '%s'),
                  expressions as (select %s as region
                                      , pg_get_expr(c.relpartbound, c.oid, true) as expression
                                  from partitions pt join pg_catalog.pg_class c on pt.partoid = c.oid)
              select region
                  , (regexp_match(expression, 'FOR VALUES FROM \\((.+)\\) TO \\(.+\\)'))[1] as from_val
                  , (regexp_match(expression, 'FOR VALUES FROM \\(.+\\) TO \\((.+)\\)'))[1] as to_val
              from expressions
              order by region, from_val;
            """, tableName, hasRegionColumn ? "c.relregion" : "0");
        break;
      case CITUS:
        partitionRangesQuery = String.format("""
                select shard, min(ycsb_key) as from_val, max(ycsb_key) as to_val
                from %s
                group by shard
                order by from_val;
            """, tableName);
        break;
      default:
        throw new RuntimeException("Unsupported database type: " + dbType);
    }

    List<Partition> partitions = new ArrayList<Partition>();
    try (Connection metaConn = benchmark.makeConnection();
        Statement stmt = metaConn.createStatement();
        ResultSet res = stmt.executeQuery(partitionRangesQuery)) {
      while (res.next()) {
        partitions.add(new Partition(res.getInt(1), res.getInt(2), res.getInt(3), benchmark.hot));
        LOG.info(partitions.get(partitions.size() - 1).toString());
      }
    }

    return partitions;
  }

  private void computeCitusShards() {
    this.citusShards = Optional.of(new ArrayList<>());
    String sql = String.format("""
          with cand_shards as (
              select
                generate_series(0, 50) as num,
                get_shard_id_for_distribution_column('usertable', generate_series(0, 50)) as shardid
            )
            select nodename, num
            from pg_dist_shard_placement p
            join cand_shards s
            on p.shardid = s.shardid
            order by num;
        """);
    try (Connection metaConn = benchmark.makeConnection();
        Statement stmt = metaConn.createStatement();
        ResultSet res = stmt.executeQuery(sql)) {
      Set<String> nodes = new HashSet<>();
      while (res.next()) {
        String node = res.getString(1);
        if (!nodes.contains(node)) {
          nodes.add(node);
          citusShards.get().add(res.getInt(2));
        }
      }
    } catch (SQLException e) {
      LOG.error(e.getMessage(), e);
    }
  }
}
