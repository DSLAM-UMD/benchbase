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
import com.oltpbenchmark.benchmarks.hot.procedures.ReadModifyWrite1;

import org.apache.commons.configuration2.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HOTBenchmark extends BenchmarkModule {

    private static final Logger LOG = LoggerFactory.getLogger(HOTBenchmark.class);

    /**
     * The length in characters of each field
     */
    protected final int fieldSize;
    protected final int region;
    protected final int numRegions;
    protected final int hot;
    protected final int loadFrom;
    protected final int loadTo;
    protected final int keysPerTxn;

    public HOTBenchmark(WorkloadConfiguration workConf) {
        super(workConf);

        int fieldSize = HOTConstants.MAX_FIELD_SIZE;
        int region = 0;
        int numRegions = 1;
        int hot = 0;
        int loadFrom = 0;
        int loadTo = -1;
        int keysPerTxn = 8;

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

            if (xmlConfig.containsKey("numregions")) {
                numRegions = xmlConfig.getInt("numregions");
            }

            if (xmlConfig.containsKey("loadfrom")) {
                loadFrom = xmlConfig.getInt("loadfrom");
            }

            if (xmlConfig.containsKey("loadto")) {
                loadTo = xmlConfig.getInt("loadto");
            }

            if (xmlConfig.containsKey("keyspertxn")) {
                keysPerTxn = xmlConfig.getInt("keyspertxn");
            }
        }

        this.fieldSize = fieldSize;
        if (this.fieldSize <= 0) {
            throw new RuntimeException("Invalid HOT fieldSize '" + this.fieldSize + "'");
        }
        this.numRegions = numRegions;
        this.region = region;
        this.hot = hot;
        this.loadFrom = loadFrom;
        this.loadTo = loadTo;
        this.keysPerTxn = keysPerTxn;
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        try {
            List<Partition> partitions = new PartitionHelper(this).getPartitions("USERTABLE");
            for (int i = 0; i < workConf.getTerminals(); ++i) {
                workers.add(new HOTWorker(this, i, partitions));
            }
        } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
        }
        return workers;
    }

    @Override
    protected Loader<HOTBenchmark> makeLoaderImpl() {
        return new HOTLoader(this, new PartitionHelper(this));
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return ReadModifyWrite1.class.getPackage();
    }
}
