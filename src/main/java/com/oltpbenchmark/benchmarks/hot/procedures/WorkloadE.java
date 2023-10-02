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

package com.oltpbenchmark.benchmarks.hot.procedures;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

public class WorkloadE extends BasicProcedures {
    public void run(Connection conn, Key[] keys, int count, String[] vals, List<String[]> results, Random rng)
            throws SQLException {
        for (Key k : keys) {
            if (rng.nextInt(100) < 95) {
                scan(conn, k, count, results);
                System.out.printf("%d(S-%s) ", k.name, k.partition.getId());
            } else {
                insert(conn, k, vals);
                System.out.printf("%d(I-%s) ", k.name, k.partition.getId());
            }
        }
        System.out.println();
    }
}
