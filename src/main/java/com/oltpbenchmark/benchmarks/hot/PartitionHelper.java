package com.oltpbenchmark.benchmarks.hot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
                case YUGABYTE:
                    computePostgresPartitions(conn);
                    break;
                case MYSQL:
                    computeMysqlPartitions(conn);
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
                order by relname;
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

    private void appendPartition(Object id) {
        this.partitions.add(new Partition(id, 0, this.numRecords, this.benchmark.hot, this.benchmark.hotDistribution));
    }

    private void finalizePartitions(Connection conn) throws SQLException {
        // The key space in each partition is divided into different strips. Each strip
        // is spaced out by the number of partitions (e.g. one strip of a 3-partition
        // key space will be 0, 3, 6, ...). Client from partition i will only insert
        // into strip i so that the partitions will not interfere with each other.
        int numInsertionStrips = 1;
        if (this.partitions.size() > 1) {
            // If there are more than 1 partition, make the first partition the global
            // partition, which should not have any data.
            Partition p0 = this.partitions.remove(0);
            this.partitions.add(0, new Partition(p0.getId()));
            numInsertionStrips = this.partitions.size() - 1;
        }

        for (Partition p : this.partitions) {
            if (p.isEmpty()) {
                continue;
            }
            String maxKeySql = String.format("SELECT MAX(ycsb_key) FROM %s_%s;", HOTConstants.TABLE_NAME, p.getId());
            try (PreparedStatement stmt = conn.prepareStatement(maxKeySql);
                    ResultSet res = stmt.executeQuery()) {
                int maxKey = p.getTo();
                while (res.next()) {
                    maxKey = Math.max(maxKey, res.getInt(1));
                }
                p.setInsertCounterStartFromMaxKey(numInsertionStrips, maxKey);
            }
        }
        this.partitions = Collections.unmodifiableList(this.partitions);
    }
}
