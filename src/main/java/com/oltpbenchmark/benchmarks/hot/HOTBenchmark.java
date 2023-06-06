/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.hot;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.hot.procedures.ReadModifyWrite;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;

import org.apache.commons.configuration2.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class HOTBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(HOTBenchmark.class);

    /**
     * The length in characters of each field
     */
    protected final int fieldSize;
    protected final int region;
    protected final int numRegions;
    protected final int mrpct;
    protected final int hot;

    public HOTBenchmark(WorkloadConfiguration workConf) {
        super(workConf);

        int fieldSize = HOTConstants.MAX_FIELD_SIZE;
        int region = 0;
        int numRegions = -1;
        int mrpct = 0;
        int hot = 0;

        XMLConfiguration xmlConfig = workConf.getXmlConfig();
        if (xmlConfig != null) {
            if (xmlConfig.containsKey("fieldSize")) {
                fieldSize = Math.min(xmlConfig.getInt("fieldSize"), HOTConstants.MAX_FIELD_SIZE);
            }

            if (xmlConfig.containsKey("region")) {
                /* region is 1-based in the config but 0-based here */
                region = xmlConfig.getInt("region") - 1;
            }

            if (xmlConfig.containsKey("hot")) {
                hot = xmlConfig.getInt("hot");
            }

            if (xmlConfig.containsKey("mrpct")) {
                mrpct = xmlConfig.getInt("mrpct");
            }

            if (xmlConfig.containsKey("numRegions")) {
                numRegions = xmlConfig.getInt("numRegions");
            }
        }

        this.fieldSize = fieldSize;
        if (this.fieldSize <= 0) {
            throw new RuntimeException("Invalid HOT fieldSize '" + this.fieldSize + "'");
        }
        this.numRegions = numRegions;
        this.region = region;
        this.mrpct = mrpct;
        this.hot = hot;
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        try {
            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF PARTITIONS
            Table t = this.getCatalog().getTable("USERTABLE");
            try (Connection metaConn = this.makeConnection();
                    Statement stmt = metaConn.createStatement()) {
                boolean hasRegionColumn = false;
                try (ResultSet res = stmt.executeQuery(checkRegionColumn())) {
                    hasRegionColumn = res.next();
                }
                String partitionRanges = getPartitionRanges(t, hasRegionColumn);
                try (ResultSet res = stmt.executeQuery(partitionRanges)) {
                    List<Partition> partitions = new ArrayList<Partition>();
                    while (res.next()) {
                        partitions.add(new Partition(res.getInt(1), res.getInt(2), res.getInt(3), this.hot));
                        LOG.info(partitions.get(partitions.size() - 1).toString());
                    }

                    for (int i = 0; i < workConf.getTerminals(); ++i) {
                        workers.add(new HOTWorker(this, i, partitions));
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        return workers;
    }

    @Override
    protected Loader<HOTBenchmark> makeLoaderImpl() {
        Optional<List<Integer>> shardNums = Optional.empty();
        if (this.workConf.getDatabaseType() == DatabaseType.CITUS) {
            shardNums = Optional.of(new ArrayList<>());
            String sql = String.format("""
                        with cand_shards as (
                            select
                              generate_series(0, 10) as num,
                              get_shard_id_for_distribution_column('usertable', generate_series(0, 10)) as shardid
                          )
                          select nodename, num
                          from pg_dist_shard_placement p
                          join cand_shards s
                          on p.shardid = s.shardid;
                    """);
            try (Connection metaConn = this.makeConnection();
                    Statement stmt = metaConn.createStatement();
                    ResultSet res = stmt.executeQuery(sql)) {
                Set<String> nodes = new HashSet<>();
                while (res.next()) {
                    String node = res.getString(1);
                    if (!nodes.contains(node)) {
                        nodes.add(node);
                        shardNums.get().add(res.getInt(2));
                    }
                }
            } catch (SQLException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        return new HOTLoader(this, shardNums);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return ReadModifyWrite.class.getPackage();
    }

    private String checkRegionColumn() {
        return String.format("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name='pg_class' and column_name='relregion';
                """);
    }

    private String getPartitionRanges(Table tbl, boolean hasRegionColumn) {
        String tableName = (this.workConf.getDatabaseType().shouldEscapeNames() ? tbl.getEscapedName() : tbl.getName());
        switch (this.workConf.getDatabaseType()) {
            case POSTGRES:
                return String.format("""
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
            case CITUS:
                return String.format("""
                            select shard, min(ycsb_key) as from_val, max(ycsb_key) as to_val
                            from %s
                            group by shard
                        """, tableName);
            default:
                throw new RuntimeException("Unsupported database type: " + this.workConf.getDatabaseType());
        }
    }
}
