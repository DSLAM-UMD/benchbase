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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.hot.procedures.*;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * HOTWorker Implementation
 * I forget who really wrote this but I fixed it up in 2016...
 *
 * @author pavlo
 */
class HOTWorker extends Worker<HOTBenchmark> {

    private final char[] data;
    private final String[] params = new String[HOTConstants.NUM_FIELDS];
    private final String[] results = new String[HOTConstants.NUM_FIELDS];
    private final Partition homePartition;
    private final List<Partition> otherPartitions;
    private final int mrpct;

    private final ReadModifyWriteRecord procReadModifyWriteRecord;

    public HOTWorker(HOTBenchmark benchmarkModule, int id, int init_record_count) {
        super(benchmarkModule, id);
        this.data = new char[benchmarkModule.fieldSize];
        this.homePartition = benchmarkModule.partitions.get(benchmarkModule.region);
        this.otherPartitions = new ArrayList<>();
        for (Partition p : benchmarkModule.partitions) {
            if (p != this.homePartition) {
                this.otherPartitions.add(p);
            }
        }
        this.mrpct = benchmarkModule.mrpct;

        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on 
        // a client machine with not a lot of cores
        this.procReadModifyWriteRecord = this.getProcedure(ReadModifyWriteRecord.class);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

        if (procClass.equals(ReadModifyWriteRecord.class)) {
            readModifyWriteRecord(conn);
        }

        return (TransactionStatus.SUCCESS);
    }

    private void readModifyWriteRecord(Connection conn) throws SQLException {
        int[] keys = new int[4];
        keys[0] = this.homePartition.nextHot(rng());
        keys[1] = this.homePartition.nextCold(rng());

        Partition partition = this.homePartition;
        if (rng().nextInt(100) + 1 <= this.mrpct) {
            int partitionIndex = rng().nextInt(this.otherPartitions.size());
            partition = this.otherPartitions.get(partitionIndex);
        }
        keys[2] = partition.nextHot(rng());
        keys[3] = partition.nextCold(rng());

        this.buildParameters();
        this.procReadModifyWriteRecord.run(conn, keys, this.params, this.results);
    }

    private void buildParameters() {
        for (int i = 0; i < this.params.length; i++) {
            this.params[i] = new String(TextGenerator.randomFastChars(rng(), this.data));
        }
    }
}
