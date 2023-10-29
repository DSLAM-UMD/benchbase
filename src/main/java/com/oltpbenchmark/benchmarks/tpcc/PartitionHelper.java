package com.oltpbenchmark.benchmarks.tpcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.types.DatabaseType;

public class PartitionHelper {
    private final int numWarehouses;
    private List<String> partitions;

    public PartitionHelper(TPCCBenchmark benchmark) throws SQLException {
        WorkloadConfiguration workloadConf = benchmark.getWorkloadConfiguration();
        this.numWarehouses = TPCCConfig.configWhseCount * (int) workloadConf.getScaleFactor();
        this.partitions = new ArrayList<>();

        partitions.add(null);

        try (Connection conn = benchmark.makeConnection()) {
            DatabaseType dbType = workloadConf.getDatabaseType();
            switch (dbType) {
                case POSTGRES:
                    computePostgresPartitions(conn);
                    break;
                case MYSQL:
                    computeMysqlPartitions(conn);
                    break;
                default:
                    throw new RuntimeException("Unsupported database type: " + dbType);
            }
        }
    }

    public int getNumWarehouses() {
        return numWarehouses;
    }

    public String getPartition(int region) {
        return partitions.get(region);
    }

    public List<String> getPartitions() {
        return partitions;
    }

    private void computePostgresPartitions(Connection conn) throws SQLException {
        String getPartitionName = String.format("""
                select relname from pg_class
                where relname like '%s_%%' and relkind = 'r'
                order by relregion;
                """, TPCCConstants.TABLENAME_WAREHOUSE);
        try (Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery(getPartitionName)) {
            while (res.next()) {
                String partitionName = res.getString(1);
                String region = partitionName.substring(partitionName.lastIndexOf('_') + 1);
                this.partitions.add(region);
            }
        }
    }

    private void computeMysqlPartitions(Connection conn) throws SQLException {
        String getPartitionName = String.format("""
                select table_name from information_schema.tables
                where table_name like '%s_%%'
                order by table_name;
                """, TPCCConstants.TABLENAME_WAREHOUSE);
        try (Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery(getPartitionName)) {
            while (res.next()) {
                String partitionName = res.getString(1);
                String region = partitionName.substring(partitionName.lastIndexOf('_') + 1);
                this.partitions.add(region);
            }
        }
    }
}
