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
import java.util.Map;

import static java.util.Map.entry;
import static java.util.AbstractMap.SimpleEntry;

class HOTWorker extends Worker<HOTBenchmark> {
    private final char[] data;
    private final String[] params = new String[HOTConstants.NUM_FIELDS];
    private final String[] results = new String[HOTConstants.NUM_FIELDS];
    private final Partition homePartition;
    private final Partition[] otherPartitions;
    private final int keysPerTxn;
    private final int maxScanCount;

    private final Map<Class<? extends Procedure>, SimpleEntry<Integer, WorkloadA>> workloadAs;
    private final Map<Class<? extends Procedure>, SimpleEntry<Integer, WorkloadB>> workloadBs;
    private final Map<Class<? extends Procedure>, SimpleEntry<Integer, WorkloadC>> workloadCs;
    private final Map<Class<? extends Procedure>, SimpleEntry<Integer, WorkloadD>> workloadDs;
    private final Map<Class<? extends Procedure>, SimpleEntry<Integer, WorkloadE>> workloadEs;
    private final Map<Class<? extends Procedure>, SimpleEntry<Integer, WorkloadF>> workloadFs;

    public HOTWorker(HOTBenchmark benchmarkModule, int id, Partition homePartition, Partition[] otherPartitions) {
        super(benchmarkModule, id);
        this.data = new char[benchmarkModule.fieldSize];
        this.homePartition = homePartition;
        this.otherPartitions = otherPartitions;
        this.keysPerTxn = benchmarkModule.keysPerTxn;
        this.maxScanCount = benchmarkModule.maxScanCount;

        this.workloadAs = Map.ofEntries(
                makeEntry(WorkloadA1.class, 1),
                makeEntry(WorkloadA2.class, 2),
                makeEntry(WorkloadA3.class, 3),
                makeEntry(WorkloadA4.class, 4),
                makeEntry(WorkloadA5.class, 5),
                makeEntry(WorkloadA6.class, 6));
        this.workloadBs = Map.ofEntries(
                makeEntry(WorkloadB1.class, 1),
                makeEntry(WorkloadB2.class, 2),
                makeEntry(WorkloadB3.class, 3),
                makeEntry(WorkloadB4.class, 4),
                makeEntry(WorkloadB5.class, 5),
                makeEntry(WorkloadB6.class, 6));
        this.workloadCs = Map.ofEntries(
                makeEntry(WorkloadC1.class, 1),
                makeEntry(WorkloadC2.class, 2),
                makeEntry(WorkloadC3.class, 3),
                makeEntry(WorkloadC4.class, 4),
                makeEntry(WorkloadC5.class, 5),
                makeEntry(WorkloadC6.class, 6));
        this.workloadDs = Map.ofEntries(
                makeEntry(WorkloadD1.class, 1),
                makeEntry(WorkloadD2.class, 2),
                makeEntry(WorkloadD3.class, 3),
                makeEntry(WorkloadD4.class, 4),
                makeEntry(WorkloadD5.class, 5),
                makeEntry(WorkloadD6.class, 6));
        this.workloadEs = Map.ofEntries(
                makeEntry(WorkloadE1.class, 1),
                makeEntry(WorkloadE2.class, 2),
                makeEntry(WorkloadE3.class, 3),
                makeEntry(WorkloadE4.class, 4),
                makeEntry(WorkloadE5.class, 5),
                makeEntry(WorkloadE6.class, 6));
        this.workloadFs = Map.ofEntries(
                makeEntry(WorkloadF1.class, 1),
                makeEntry(WorkloadF2.class, 2),
                makeEntry(WorkloadF3.class, 3),
                makeEntry(WorkloadF4.class, 4),
                makeEntry(WorkloadF5.class, 5),
                makeEntry(WorkloadF6.class, 6));
    }

    private <T extends Procedure> Map.Entry<Class<? extends Procedure>, SimpleEntry<Integer, T>> makeEntry(
            Class<? extends T> procClass,
            int numPartitions) {
        return entry(procClass, new SimpleEntry<>(numPartitions, this.getProcedure(procClass)));
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans)
            throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

        // Workload A
        if (this.workloadAs.containsKey(procClass)) {
            SimpleEntry<Integer, WorkloadA> entry = this.workloadAs.get(procClass);
            int numPartitions = entry.getKey();
            WorkloadA workload = entry.getValue();

            this.buildParameters();
            workload.run(conn, selectKeys(numPartitions, false), this.params, this.results, rng());
        }
        // Workload B
        if (this.workloadBs.containsKey(procClass)) {
            SimpleEntry<Integer, WorkloadB> entry = this.workloadBs.get(procClass);
            int numPartitions = entry.getKey();
            WorkloadB workload = entry.getValue();

            this.buildParameters();
            workload.run(conn, selectKeys(numPartitions, false), this.params, this.results, rng());
        }
        // Workload C
        if (this.workloadCs.containsKey(procClass)) {
            SimpleEntry<Integer, WorkloadC> entry = this.workloadCs.get(procClass);
            int numPartitions = entry.getKey();
            WorkloadC workload = entry.getValue();

            this.buildParameters();
            workload.run(conn, selectKeys(numPartitions, false), this.params, this.results, rng());
        }
        // Workload D
        if (this.workloadDs.containsKey(procClass)) {
            SimpleEntry<Integer, WorkloadD> entry = this.workloadDs.get(procClass);
            int numPartitions = entry.getKey();
            WorkloadD workload = entry.getValue();

            this.buildParameters();
            int numStrips = this.otherPartitions.length + 1;
            int slot = Math.max(this.getBenchmark().region - 1, 0);
            workload.run(conn, numStrips, slot, selectKeys(numPartitions, true),
                    this.params,
                    this.results,
                    rng());
        }
        // Workload E
        if (this.workloadEs.containsKey(procClass)) {
            SimpleEntry<Integer, WorkloadE> entry = this.workloadEs.get(procClass);
            int numPartitions = entry.getKey();
            WorkloadE workload = entry.getValue();

            this.buildParameters();
            int numStrips = this.otherPartitions.length + 1;
            int slot = Math.max(this.getBenchmark().region - 1, 0);
            workload.run(conn, numStrips, slot, selectKeys(numPartitions, false),
                    this.maxScanCount,
                    this.params,
                    new ArrayList<>(),
                    rng());
        }
        // Workload F
        if (this.workloadFs.containsKey(procClass)) {
            SimpleEntry<Integer, WorkloadF> entry = this.workloadFs.get(procClass);
            int numPartitions = entry.getKey();
            WorkloadF workload = entry.getValue();

            this.buildParameters();
            workload.run(conn, selectKeys(numPartitions, false), this.params, this.results, rng());
        }
        return (TransactionStatus.SUCCESS);
    }

    private Key[] selectKeys(int numPartitions, boolean latest) {
        if (numPartitions > this.otherPartitions.length + 1) {
            throw new UserAbortException(String.format(
                    "Number of accessed partitions (%d) cannot be greater than the number of available partitions (%d)",
                    numPartitions, this.otherPartitions.length + 1));
        }

        // Select the partitions that the txn will accept. The home partition is always
        // included. The other partitions are chosen randomly without replacement
        Partition[] chosenPartitions = new Partition[numPartitions];
        chosenPartitions[0] = this.homePartition;
        if (this.otherPartitions.length > 0) {
            int[] chosenOtherPartitions = rng()
                    .ints(0, this.otherPartitions.length)
                    .distinct()
                    .limit(numPartitions - 1)
                    .toArray();
            for (int i = 1; i < numPartitions; i++) {
                chosenPartitions[i] = this.otherPartitions[chosenOtherPartitions[i - 1]];
            }
        }

        // Select the keys from the partitions
        Key[] keys = new Key[this.keysPerTxn];
        int keyIndex = 0;
        for (int i = 0; i < chosenPartitions.length; i++) {
            // Make sure that the keys are evenly distributed across the partitions
            int numKeys = this.keysPerTxn / chosenPartitions.length
                    + (i < this.keysPerTxn % chosenPartitions.length ? 1 : 0);
            Partition chosenPartition = chosenPartitions[i];
            // The first key is always a hot/latest key
            if (latest) {
                keys[keyIndex++] = new Key(chosenPartition.nextLatest(rng()), chosenPartition);
            } else {
                keys[keyIndex++] = new Key(chosenPartition.nextHot(rng()), chosenPartition);
            }
            // The rest are cold keys
            for (int j = 1; j < numKeys; j++) {
                keys[keyIndex++] = new Key(chosenPartition.nextCold(rng()), chosenPartition);
            }
        }

        return keys;
    }

    private void buildParameters() {
        for (int i = 0; i < this.params.length; i++) {
            this.params[i] = new String(TextGenerator.randomFastChars(rng(), this.data));
        }
    }
}
