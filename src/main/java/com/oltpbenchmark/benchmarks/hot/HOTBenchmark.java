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
import com.oltpbenchmark.benchmarks.hot.procedures.ReadModifyWriteRecord;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import org.apache.commons.configuration2.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HOTBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(HOTBenchmark.class);

    /**
     * The length in characters of each field
     */
    protected final int fieldSize;
    protected final int region;
    protected final int mrpct;
    protected final List<Partition> partitions;

    public HOTBenchmark(WorkloadConfiguration workConf) {
        super(workConf);

        this.partitions = new ArrayList<>();

        int fieldSize = HOTConstants.MAX_FIELD_SIZE;
        int region = 0;
        int hot = 0;
        int mrpct = 0;

        XMLConfiguration xmlConfig = workConf.getXmlConfig();
        if (xmlConfig != null) {
            if (xmlConfig.containsKey("fieldSize")) {
                fieldSize = Math.min(xmlConfig.getInt("fieldSize"), HOTConstants.MAX_FIELD_SIZE);
            }

            if (xmlConfig.containsKey("region")) {
                region = xmlConfig.getInt("region");
            }

            if (xmlConfig.containsKey("hot")) {
                hot = xmlConfig.getInt("hot");
            }

            if (xmlConfig.containsKey("partitions")) {
                List<String> ranges = Arrays.asList(xmlConfig.getString("partitions").split("\\s*;\\s*"));
                for (String range : ranges) {
                    String[] pair = range.split("\\s*,\\s*");
                    if (pair.length != 2) {
                        throw new RuntimeException("Invalid range specification '" + range + "'");
                    }
                    Integer from = Integer.parseInt(pair[0]);
                    Integer to = Integer.parseInt(pair[1]);
                    if (from >= to) {
                        throw new RuntimeException("Empty range [" + from + ", " + to + ")");
                    }
                    this.partitions.add(new Partition(from, to, hot));
                }
            } else {
                this.partitions.add(new Partition(0, HOTConstants.RECORD_COUNT, hot));
            }

            if (xmlConfig.containsKey("mrpct")) {
                mrpct = xmlConfig.getInt("mrpct");
            }
        }

        this.fieldSize = fieldSize;
        if (this.fieldSize <= 0) {
            throw new RuntimeException("Invalid HOT fieldSize '" + this.fieldSize + "'");
        }

        this.region = region;
        if (this.region < 0 || this.region >= this.partitions.size()) {
            throw new RuntimeException("Region '" + this.region + "' is not within number of partitions");
        }

        this.mrpct = mrpct;
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        try {
            // LOADING FROM THE DATABASE IMPORTANT INFORMATION
            // LIST OF USERS
            Table t = this.getCatalog().getTable("USERTABLE");
            String userCount = SQLUtil.getMaxColSQL(this.workConf.getDatabaseType(), t, "ycsb_key");

            try (Connection metaConn = this.makeConnection();
                 Statement stmt = metaConn.createStatement();
                 ResultSet res = stmt.executeQuery(userCount)) {
                int init_record_count = 0;
                while (res.next()) {
                    init_record_count = res.getInt(1);
                }

                for (int i = 0; i < workConf.getTerminals(); ++i) {
                    workers.add(new HOTWorker(this, i, init_record_count + 1));
                }
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        return workers;
    }

    @Override
    protected Loader<HOTBenchmark> makeLoaderImpl() {
        return new HOTLoader(this);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return ReadModifyWriteRecord.class.getPackage();
    }

}
