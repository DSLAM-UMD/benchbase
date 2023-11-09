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
import com.oltpbenchmark.benchmarks.hot.procedures.WorkloadA;

import org.apache.commons.configuration2.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HOTBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(HOTBenchmark.class);

    /**
     * The length in characters of each field
     */
    final int fieldSize;
    final int region;
    final int hot;
    final int keysPerTxn;
    final int maxScanCount;
    final boolean loadAll;

    public HOTBenchmark(WorkloadConfiguration workConf) {
        super(workConf);

        int fieldSize = HOTConstants.MAX_FIELD_SIZE;
        int region = 0;
        int hot = 0;
        int keysPerTxn = 8;
        boolean loadAll = false;
        int maxScanCount = 100;

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

            if (xmlConfig.containsKey("keyspertxn")) {
                keysPerTxn = xmlConfig.getInt("keyspertxn");
            }

            if (xmlConfig.containsKey("maxscancount")) {
                maxScanCount = xmlConfig.getInt("maxscancount");
            }

            if (xmlConfig.containsKey("loadall")) {
                loadAll = xmlConfig.getBoolean("loadall");
            }
        }

        this.fieldSize = fieldSize;
        if (this.fieldSize <= 0) {
            throw new RuntimeException("Invalid HOT fieldSize '" + this.fieldSize + "'");
        }
        this.region = region;
        this.hot = hot;
        this.keysPerTxn = keysPerTxn;
        this.maxScanCount = maxScanCount;
        this.loadAll = loadAll;
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        try {
            PartitionHelper partitionHelper = new PartitionHelper(this);

            for (Partition p : partitionHelper.getPartitions()) {
                LOG.info("Partition - {}", p);
            }

            Partition homePartition = partitionHelper.getPartition(this.region);
            if (homePartition.isEmpty()) {
                return Arrays.asList();
            }

            Partition[] otherPartitions = partitionHelper.getPartitions().stream()
                    .filter(p -> p != homePartition && !p.isEmpty())
                    .toArray(Partition[]::new);

            List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
            for (int i = 0; i < workConf.getTerminals(); ++i) {
                workers.add(new HOTWorker(this, i, homePartition, otherPartitions));
            }
            return workers;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected Loader<HOTBenchmark> makeLoaderImpl() {
        try {
            PartitionHelper partitionHelper = new PartitionHelper(this);
            Partition[] loadedPartitions;

            if (this.loadAll) {
                loadedPartitions = partitionHelper.getPartitions().toArray(Partition[]::new);
            } else {
                loadedPartitions = new Partition[] { partitionHelper.getPartition(this.region) };
            }

            return new HOTLoader(this, loadedPartitions);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return WorkloadA.class.getPackage();
    }
}
