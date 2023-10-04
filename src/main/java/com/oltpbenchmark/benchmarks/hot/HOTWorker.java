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
    private final Partition[] otherPartitions;
    private final int keysPerTxn;

    private final WorkloadA workloadA;
    private final static Class<?>[] workloadAs = new Class[] {
            WorkloadA.class,
            WorkloadA1.class,
            WorkloadA2.class,
            WorkloadA3.class,
            WorkloadA4.class,
            WorkloadA5.class,
            WorkloadA6.class
    };
    private final WorkloadB workloadB;
    private final static Class<?>[] workloadBs = new Class[] {
            WorkloadB.class,
            WorkloadB1.class,
            WorkloadB2.class,
            WorkloadB3.class,
            WorkloadB4.class,
            WorkloadB5.class,
            WorkloadB6.class
    };
    private final WorkloadC workloadC;
    private final static Class<?>[] workloadCs = new Class[] {
            WorkloadC.class,
            WorkloadC1.class,
            WorkloadC2.class,
            WorkloadC3.class,
            WorkloadC4.class,
            WorkloadC5.class,
            WorkloadC6.class
    };
    private final WorkloadD workloadD;
    private final static Class<?>[] workloadDs = new Class[] {
            WorkloadD.class,
            WorkloadD1.class,
            WorkloadD2.class,
            WorkloadD3.class,
            WorkloadD4.class,
            WorkloadD5.class,
            WorkloadD6.class
    };
    private final WorkloadE workloadE;
    private final static Class<?>[] workloadEs = new Class[] {
            WorkloadE.class,
            WorkloadE1.class,
            WorkloadE2.class,
            WorkloadE3.class,
            WorkloadE4.class,
            WorkloadE5.class,
            WorkloadE6.class
    };
    private final WorkloadF workloadF;
    private final static Class<?>[] workloadFs = new Class[] {
            WorkloadF.class,
            WorkloadF1.class,
            WorkloadF2.class,
            WorkloadF3.class,
            WorkloadF4.class,
            WorkloadF5.class,
            WorkloadF6.class
    };

    public HOTWorker(HOTBenchmark benchmarkModule, int id, Partition homePartition, Partition[] otherPartitions) {
        super(benchmarkModule, id);
        this.data = new char[benchmarkModule.fieldSize];
        this.homePartition = homePartition;
        this.otherPartitions = otherPartitions;
        this.keysPerTxn = benchmarkModule.keysPerTxn;

        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on
        // a client machine with not a lot of cores.
        // We don't use ReadModifyWrite.class because it is not specified in the
        // benchmark
        // config file. Any of the ReadModifyWriteX classes can be used though and they
        // are
        // all the same.
        this.workloadA = this.getProcedure(WorkloadA1.class);
        this.workloadB = this.getProcedure(WorkloadB1.class);
        this.workloadC = this.getProcedure(WorkloadC1.class);
        this.workloadD = this.getProcedure(WorkloadD1.class);
        this.workloadE = this.getProcedure(WorkloadE1.class);
        this.workloadF = this.getProcedure(WorkloadF1.class);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans)
            throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

        // Workload A
        for (int involvedPartitions = 1; involvedPartitions < workloadAs.length; involvedPartitions++) {
            if (procClass.equals(workloadAs[involvedPartitions])) {
                this.buildParameters();
                this.workloadA.run(conn, selectKeys(involvedPartitions, false), this.params, this.results, rng());
            }
        }
        // Workload B
        for (int involvedPartitions = 1; involvedPartitions < workloadBs.length; involvedPartitions++) {
            if (procClass.equals(workloadBs[involvedPartitions])) {
                this.buildParameters();
                this.workloadB.run(conn, selectKeys(involvedPartitions, false), this.params, this.results, rng());
            }
        }
        // Workload C
        for (int involvedPartitions = 1; involvedPartitions < workloadCs.length; involvedPartitions++) {
            if (procClass.equals(workloadCs[involvedPartitions])) {
                this.buildParameters();
                this.workloadC.run(conn, selectKeys(involvedPartitions, false), this.params, this.results, rng());
            }
        }
        // Workload D
        for (int involvedPartitions = 1; involvedPartitions < workloadDs.length; involvedPartitions++) {
            if (procClass.equals(workloadDs[involvedPartitions])) {
                this.buildParameters();
                int numPartitions = this.otherPartitions.length + 1;
                int homePartition = this.getBenchmark().region - 1;
                this.workloadD.run(conn, numPartitions, homePartition, selectKeys(involvedPartitions, true),
                        this.params,
                        this.results,
                        rng());
            }
        }
        // Workload E
        for (int involvedPartitions = 1; involvedPartitions < workloadEs.length; involvedPartitions++) {
            if (procClass.equals(workloadEs[involvedPartitions])) {
                this.buildParameters();
                int numPartitions = this.otherPartitions.length + 1;
                int homePartition = this.getBenchmark().region - 1;
                this.workloadE.run(conn, numPartitions, homePartition, selectKeys(involvedPartitions, false),
                        this.params,
                        new ArrayList<>(),
                        rng());
            }
        }
        // Workload F
        for (int involvedPartitions = 1; involvedPartitions < workloadFs.length; involvedPartitions++) {
            if (procClass.equals(workloadFs[involvedPartitions])) {
                this.buildParameters();
                this.workloadF.run(conn, selectKeys(involvedPartitions, false), this.params, this.results, rng());
            }
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
        int[] chosenOtherPartitions = rng()
                .ints(0, this.otherPartitions.length)
                .distinct()
                .limit(numPartitions - 1)
                .toArray();
        for (int i = 1; i < numPartitions; i++) {
            chosenPartitions[i] = this.otherPartitions[chosenOtherPartitions[i - 1]];
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
