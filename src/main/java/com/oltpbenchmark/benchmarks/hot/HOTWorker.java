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
    private final int keysPerTxn;

    private final ReadModifyWrite procReadModifyWrite;

    public HOTWorker(HOTBenchmark benchmarkModule, int id, List<Partition> partitions) {
        super(benchmarkModule, id);
        this.data = new char[benchmarkModule.fieldSize];
        this.homePartition = partitions.get(benchmarkModule.region);
        this.otherPartitions = new ArrayList<>();
        for (Partition p : partitions) {
            if (p != this.homePartition) {
                this.otherPartitions.add(p);
            }
        }
        this.keysPerTxn = benchmarkModule.keysPerTxn;

        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on
        // a client machine with not a lot of cores.
        // We don't use ReadModifyWrite.class because it is not specified in the
        // benchmark
        // config file. Any of the ReadModifyWriteX classes can be used though and they
        // are
        // all the same.
        this.procReadModifyWrite = this.getProcedure(ReadModifyWrite1.class);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans)
            throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

        if (procClass.equals(ReadModifyWrite1.class)) {
            readModifyWrite(conn, 1);
        } else if (procClass.equals(ReadModifyWrite2.class)) {
            readModifyWrite(conn, 2);
        } else if (procClass.equals(ReadModifyWrite3.class)) {
            readModifyWrite(conn, 3);
        } else if (procClass.equals(ReadModifyWrite4.class)) {
            readModifyWrite(conn, 4);
        }

        return (TransactionStatus.SUCCESS);
    }

    private void readModifyWrite(Connection conn, int numPartitions) throws SQLException {
        if (numPartitions > this.otherPartitions.size() + 1) {
            throw new IllegalArgumentException(String.format(
                    "Number of accessed partitions (%d) cannot be greater than the number of available partitions (%d)",
                    numPartitions, this.otherPartitions.size() + 1));
        }

        // Select the partitions that the txn will accept. The home partition is always
        // included. The other partitions are chosen randomly without replacement
        Partition[] partitions = new Partition[numPartitions];
        partitions[0] = this.homePartition;
        int[] chosenOtherPartitions = rng()
                .ints(0, this.otherPartitions.size())
                .distinct()
                .limit(numPartitions - 1)
                .toArray();
        for (int i = 1; i < numPartitions; i++) {
            partitions[i] = this.otherPartitions.get(chosenOtherPartitions[i - 1]);
        }

        // Select the keys from the partitions
        Key[] keys = new Key[this.keysPerTxn];
        int keyIndex = 0;
        for (int i = 0; i < partitions.length; i++) {
            // Make sure that the keys are evenly distributed across the partitions
            int numKeys = this.keysPerTxn / partitions.length + (i < this.keysPerTxn % partitions.length ? 1 : 0);
            // The first key is always a hot key
            keys[keyIndex++] = new Key(partitions[i].nextHot(rng()), partitions[i]);
            // The rest are cold keys
            for (int j = 1; j < numKeys; j++) {
                keys[keyIndex++] = new Key(partitions[i].nextCold(rng()), partitions[i]);
            }
        }

        this.buildParameters();

        this.procReadModifyWrite.run(conn, keys, this.params, this.results);
    }

    private void buildParameters() {
        for (int i = 0; i < this.params.length; i++) {
            this.params[i] = new String(TextGenerator.randomFastChars(rng(), this.data));
        }
    }
}
