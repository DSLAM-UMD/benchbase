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
import com.oltpbenchmark.types.DatabaseType;
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

    private final DatabaseType dbType;
    private final char[] data;
    private final String[] params = new String[HOTConstants.NUM_FIELDS];
    private final String[] results = new String[HOTConstants.NUM_FIELDS];
    private final Partition homePartition;
    private final List<Partition> otherPartitions;
    private final int mrpct;

    private final ReadModifyWrite procReadModifyWrite;
    private final RMWLocalRORemote procRMWLocalRORemote;

    public HOTWorker(HOTBenchmark benchmarkModule, int id, List<Partition> partitions) {
        super(benchmarkModule, id);
        this.dbType = benchmarkModule.getWorkloadConfiguration().getDatabaseType();
        this.data = new char[benchmarkModule.fieldSize];
        this.homePartition = partitions.get(benchmarkModule.region);
        this.otherPartitions = new ArrayList<>();
        for (Partition p : partitions) {
            if (p != this.homePartition) {
                this.otherPartitions.add(p);
            }
        }
        this.mrpct = benchmarkModule.mrpct;

        // This is a minor speed-up to avoid having to invoke the hashmap look-up
        // everytime we want to execute a txn. This is important to do on
        // a client machine with not a lot of cores
        this.procReadModifyWrite = this.getProcedure(ReadModifyWrite.class);
        this.procRMWLocalRORemote = this.getProcedure(RMWLocalRORemote.class);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans)
            throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

        if (procClass.equals(ReadModifyWrite.class)) {
            readModifyWrite(conn);
        } else if (procClass.equals(RMWLocalRORemote.class)) {
            rmwLocalRORemote(conn);
        }

        return (TransactionStatus.SUCCESS);
    }

    private void readModifyWrite(Connection conn) throws SQLException {
        Key[] keys = new Key[4];
        keys[0] = new Key(this.homePartition.nextHot(rng()), homePartition.getId());
        keys[1] = new Key(this.homePartition.nextCold(rng()), homePartition.getId());

        Partition partition = this.homePartition;
        if (rng().nextInt(100) + 1 <= this.mrpct) {
            int partitionIndex = rng().nextInt(this.otherPartitions.size());
            partition = this.otherPartitions.get(partitionIndex);
        }
        keys[2] = new Key(partition.nextHot(rng()), partition.getId());
        keys[3] = new Key(partition.nextCold(rng()), partition.getId());

        this.buildParameters();
        boolean withShard = this.dbType == DatabaseType.CITUS;
        this.procReadModifyWrite.run(conn, withShard, keys, this.params, this.results);
    }

    private void rmwLocalRORemote(Connection conn) throws SQLException {
        int[] local_keys = new int[2];
        local_keys[0] = this.homePartition.nextHot(rng());
        local_keys[1] = this.homePartition.nextCold(rng());

        int[] remote_keys = new int[2];
        Partition partition = this.homePartition;
        if (rng().nextInt(100) + 1 <= this.mrpct) {
            int partitionIndex = rng().nextInt(this.otherPartitions.size());
            partition = this.otherPartitions.get(partitionIndex);
        }
        remote_keys[0] = partition.nextHot(rng());
        remote_keys[1] = partition.nextCold(rng());

        this.buildParameters();
        this.procRMWLocalRORemote.run(conn, local_keys, remote_keys, this.params, this.results);
    }

    private void buildParameters() {
        for (int i = 0; i < this.params.length; i++) {
            this.params[i] = new String(TextGenerator.randomFastChars(rng(), this.data));
        }
    }
}
