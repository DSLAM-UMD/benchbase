package com.oltpbenchmark.benchmarks.hot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;
import com.oltpbenchmark.types.DatabaseType;

public class PartitionHelper {
    private HOTBenchmark benchmark;
    private int numRecords;
    private List<Partition> partitions;

    public PartitionHelper(HOTBenchmark benchmark) throws SQLException {
        WorkloadConfiguration workloadConf = benchmark.getWorkloadConfiguration();
        this.benchmark = benchmark;
        this.numRecords = (int) Math
                .round(YCSBConstants.RECORD_COUNT * workloadConf.getScaleFactor());
        this.partitions = new ArrayList<>();

        try (Connection conn = this.benchmark.makeConnection()) {
            DatabaseType dbType = workloadConf.getDatabaseType();
            switch (dbType) {
                case POSTGRES:
                    computePostgresPartitions(conn);
                    break;
                case MYSQL:
                    computeMysqlPartitions(conn);
                    break;
                case CITUS:
                    computeCitusPartitions(conn);
                    break;
                case YUGABYTEDB:
                    computeYugabytePartitions(conn);
                    break;
                default:
                    throw new RuntimeException("Unsupported database type: " + dbType);
            }
            finalizePartitions(conn);
        }
    }

    public Partition getPartition(int region) {
        return partitions.get(region);
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    private void computePostgresPartitions(Connection conn) throws SQLException {
        appendPartition(0);

        String getPartitionName = String.format("""
                select relname from pg_class
                where relname like '%s_%%' and relkind = 'r'
                order by relregion;
                """, HOTConstants.TABLE_NAME);
        try (Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery(getPartitionName)) {
            while (res.next()) {
                String partitionName = res.getString(1);
                String region = partitionName.substring(partitionName.lastIndexOf('_') + 1);
                appendPartition(region);
            }
        }
        // This is the code to get the partitions of PostgreSQL's partitioned table.
        // We have switched to a schema that use simple tables instead. This code is
        // kept here just in case.
        //
        // @formatter:off
        // boolean hasRegionColumn = false;
        // String checkRegionColumn = """
        //             SELECT column_name
        //             FROM information_schema.columns
        //             WHERE table_name='pg_class' and column_name='relregion';
        //         """;
        // try (Statement stmt = conn.createStatement();
        //         ResultSet res = stmt.executeQuery(checkRegionColumn)) {
        //     hasRegionColumn = res.next();
        // }

        // String getPartitionName = String.format("""
        //           with partitions as (select i.inhrelid as partoid
        //                               from pg_inherits i
        //                               join pg_class cl on i.inhparent = cl.oid
        //                               where lower(cl.relname) = '%s'),
        //               expressions as (select %s as region
        //                                   , pg_get_expr(c.relpartbound, c.oid, true) as expression
        //                               from partitions pt join pg_catalog.pg_class c on pt.partoid = c.oid)
        //           select region
        //               , (regexp_match(expression, 'FOR VALUES IN \\((.+)\\)'))[1] as geo_partition
        //           from expressions
        //           order by region, geo_partition;
        //         """, HOTConstants.TABLE_NAME, hasRegionColumn ? "c.relregion" : "0");
        // try (Statement stmt = conn.createStatement();
        //         ResultSet res = stmt.executeQuery(getPartitionName)) {
        //     while (res.next()) {
        //         appendPartition(hasRegionColumn ? res.getObject(1) : this.partitions.size());
        //     }
        // }
        // @formatter:on
    }

    private void computeMysqlPartitions(Connection conn) throws SQLException {
        appendPartition(0);

        String getPartitionName = String.format("""
                select table_name from information_schema.tables
                where table_name like '%s_%%'
                order by table_name;
                """, HOTConstants.TABLE_NAME);
        try (Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery(getPartitionName)) {
            while (res.next()) {
                String partitionName = res.getString(1);
                String region = partitionName.substring(partitionName.lastIndexOf('_') + 1);
                appendPartition(region);
            }
        }
    }

    // TODO: This code is probably broken
    private void computeCitusPartitions(Connection conn) throws SQLException {
        appendPartition(0);

        String sql = String.format("""
                  with cand_shards as (
                      select
                        generate_series(0, 50) as num,
                        get_shard_id_for_distribution_column('%s', generate_series(0, 50)) as shardid
                    )
                    select nodename, num
                    from pg_dist_shard_placement p
                    join cand_shards s
                    on p.shardid = s.shardid
                    order by num;
                """, HOTConstants.TABLE_NAME);
        try (Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery(sql)) {
            Set<String> nodes = new HashSet<>();
            while (res.next()) {
                String node = res.getString(1);
                if (!nodes.contains(node)) {
                    nodes.add(node);
                    appendPartition(res.getObject(2));
                }
            }
        }
    }

    // TODO: This code is probably broken
    private void computeYugabytePartitions(Connection conn) throws SQLException {
        appendPartition("global");

        String sql = String.format("""
                  with partitions as (select i.inhrelid as partoid
                                      from pg_inherits i
                                      join pg_class cl on i.inhparent = cl.oid
                                      where lower(cl.relname = '%s'),
                      expressions as (select pg_get_expr(c.relpartbound, c.oid, true) as expression
                                      from partitions pt join pg_catalog.pg_class c on pt.partoid = c.oid)
                  select (regexp_match(expression, 'FOR VALUES IN \\(''(.+)''\\)'))[1] as region
                  from expressions;
                """, HOTConstants.TABLE_NAME);
        try (Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery(sql)) {
            while (res.next()) {
                appendPartition(res.getObject(1));
            }
        }
    }

    private void appendPartition(Object id) {
        this.partitions.add(new Partition(id, 0, this.numRecords, this.benchmark.hot));
    }

    private void finalizePartitions(Connection conn) throws SQLException {
        int numInsertionSlots = 1;
        if (this.partitions.size() > 1) {
            // If there are more than 1 partition, make the first partition the global
            // partition, which should not have any data.
            Partition p0 = this.partitions.remove(0);
            this.partitions.add(0, new Partition(p0.getId()));
            numInsertionSlots = this.partitions.size() - 1;
        }

        for (Partition p : this.partitions) {
            if (p.isEmpty()) {
                continue;
            }
            String maxKeySql = String.format("SELECT MAX(ycsb_key) FROM %s_%s;", HOTConstants.TABLE_NAME, p.getId());
            try (PreparedStatement stmt = conn.prepareStatement(maxKeySql)) {
                try (ResultSet res = stmt.executeQuery()) {
                    int maxKey = 0;
                    while (res.next()) {
                        maxKey = res.getInt(1);
                    }
                    p.setInsertCounterStartFromMaxKey(numInsertionSlots, maxKey);
                }
            }
        }
        this.partitions = Collections.unmodifiableList(this.partitions);
    }
}
