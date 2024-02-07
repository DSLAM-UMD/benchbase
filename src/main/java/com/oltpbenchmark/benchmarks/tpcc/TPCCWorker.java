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

package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.procedures.TPCCProcedure;
import com.oltpbenchmark.types.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TPCCWorker extends Worker<TPCCBenchmark> {

    private static final Logger LOG = LoggerFactory.getLogger(TPCCWorker.class);

    private final Integer region;
    private final PartitionedWId terminalWarehouseID;
    /**
     * Forms a range [lower, upper] (inclusive).
     */
    private final int terminalDistrictLowerID;
    private final int terminalDistrictUpperID;
    private final Random gen = new Random();

    private final PartitionHelper partitions;

    public TPCCWorker(TPCCBenchmark benchmarkModule, int id,
            PartitionedWId terminalWarehouseID, int terminalDistrictLowerID,
            int terminalDistrictUpperID, PartitionHelper partitions) {
        super(benchmarkModule, id);
        
        this.region = benchmarkModule.region;
        this.terminalWarehouseID = terminalWarehouseID;
        this.terminalDistrictLowerID = terminalDistrictLowerID;
        this.terminalDistrictUpperID = terminalDistrictUpperID;

        this.partitions = partitions;
    }

    @Override
    protected void initialize() {
        this.getErrors().extendKeyNames("deadlock", "region", "validation");
    }

    private final Pattern deadlockPattern = Pattern.compile("deadlock");
    private final Pattern regionPattern = Pattern.compile("Region ([0-9]+)");
    private final Pattern validationPattern = Pattern.compile("out-of-date (index|table|tuple)");

    protected List<String> parseError(TransactionType txnType, SQLException ex, boolean willRetry) {
        List<String> errors = super.parseError(txnType, ex, willRetry);
        String message = ex.getMessage();

        // Check whether this error is a deadlock
        Matcher deadlockMatcher = deadlockPattern.matcher(message);
        Boolean deadlockFound = deadlockMatcher.find();
        errors.add(deadlockFound.toString());

        // Check the region this error comes from
        Matcher regionMatcher = regionPattern.matcher(message);
        if (regionMatcher.find()) {
            // Add region
            errors.add(regionMatcher.group(1));

            // Add the type of validation error
            if (deadlockFound) {
                errors.add("deadlock");
            } else {
                Matcher validationMatcher = validationPattern.matcher(message);
                if (validationMatcher.find()) {
                    errors.add(validationMatcher.group(1));
                } else {
                    errors.add("null");
                }
            }
        } else {
            // Error happens in the local region
            errors.add(this.region.toString());
            errors.add("null");
        }
        
        return errors;
    }

    /**
     * Executes a single TPCC transaction of type transactionType.
     */
    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction)
            throws UserAbortException, SQLException {
        try {
            TPCCProcedure proc = (TPCCProcedure) this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn, gen, terminalWarehouseID, partitions,
                    terminalDistrictLowerID, terminalDistrictUpperID, this);
        } catch (ClassCastException ex) {
            // fail gracefully
            LOG.error("We have been invoked with an INVALID transactionType?!", ex);
            throw new RuntimeException("Bad transaction type = " + nextTransaction);
        }
        return (TransactionStatus.SUCCESS);
    }

    @Override
    protected long getPreExecutionWaitInMillis(TransactionType type) {
        // TPC-C 5.2.5.2: For keying times for each type of transaction.
        return type.getPreExecutionWait();
    }

    @Override
    protected long getPostExecutionWaitInMillis(TransactionType type) {
        // TPC-C 5.2.5.4: For think times for each type of transaction.
        long mean = type.getPostExecutionWait();

        float c = this.getBenchmark().rng().nextFloat();
        long thinkTime = (long) (-1 * Math.log(c) * mean);
        if (thinkTime > 10 * mean) {
            thinkTime = 10 * mean;
        }

        return thinkTime;
    }

}
