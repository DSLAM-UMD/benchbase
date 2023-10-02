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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.hot.HOTConstants;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.oltpbenchmark.benchmarks.hot.HOTConstants.TABLE_NAME;

class BasicProcedures extends Procedure {
    protected void insert(Connection conn, Key key, String[] vals) throws SQLException {
        try (PreparedStatement stmt = this.prepareInsertStmt(conn, key, vals)) {
            stmt.executeUpdate();
        }
    }

    protected void read(Connection conn, Key key, String[] results) throws SQLException {
        try (PreparedStatement stmt = this.prepareReadStmt(conn, key)) {
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    for (int i = 0; i < HOTConstants.NUM_FIELDS; i++) {
                        results[i] = r.getString(i + 1);
                    }
                }
            }
        }
    }

    protected void update(Connection conn, Key key, String[] fields) throws SQLException {
        try (PreparedStatement stmt = this.prepareUpdateStmt(conn, key, fields)) {
            stmt.executeUpdate();
        }
    }

    protected void scan(Connection conn, Key start, int count, List<String[]> results) throws SQLException {
        try (PreparedStatement stmt = this.prepareScanStmt(conn, start, count, results)) {
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    String[] data = new String[YCSBConstants.NUM_FIELDS];
                    for (int i = 0; i < data.length; i++) {
                        data[i] = r.getString(i + 1);
                    }
                    results.add(data);
                }
            }
        }
    }

    protected void readModifyWrite(Connection conn, Key key, String[] fields,
            String[] results)
            throws SQLException {
        try (PreparedStatement stmt = this.prepareReadStmt(conn, key)) {
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    for (int i = 0; i < HOTConstants.NUM_FIELDS; i++) {
                        results[i] = r.getString(i + 1);
                    }
                }
            }
        }
        try (PreparedStatement stmt = this.prepareUpdateStmt(conn, key, fields)) {
            stmt.executeUpdate();
        }
    }

    public final SQLStmt insertStmt = new SQLStmt(
            "INSERT INTO " + TABLE_NAME + " VALUES (?,?,?,?,?,?,?,?,?,?,?)");

    public PreparedStatement prepareInsertStmt(Connection conn, Key key, String[] vals) throws SQLException {
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, insertStmt);
        stmt.setInt(1, key.name);
        for (int i = 0; i < vals.length; i++) {
            stmt.setString(i + 2, vals[i]);
        }
        return stmt;
    }

    private final static SQLStmt readStmt = new SQLStmt(
            "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=?");
    private final static SQLStmt readStmtWithGeoPartition = new SQLStmt(
            "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=? and GEO_PARTITION=?");

    private PreparedStatement prepareReadStmt(Connection conn, Key key) throws SQLException {
        Optional<Class<?>> partitionIdType = key.partition.getIdType();
        SQLStmt chosenReadStmt = partitionIdType.isPresent() ? readStmtWithGeoPartition : readStmt;
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, chosenReadStmt, key.name);
        if (partitionIdType.isPresent()) {
            if (partitionIdType.get().equals(Integer.class)) {
                stmt.setInt(2, key.partition.getIntId());
            } else if (partitionIdType.get().equals(String.class)) {
                stmt.setString(2, key.partition.getStringId());
            }
        }
        return stmt;
    }

    private final SQLStmt updateStmt = new SQLStmt(
            "UPDATE " + TABLE_NAME + " SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
                    "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?");
    private final SQLStmt updateStmtWithGeoPartition = new SQLStmt(
            "UPDATE " + TABLE_NAME + " SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
                    "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=? and GEO_PARTITION=?");

    private PreparedStatement prepareUpdateStmt(Connection conn, Key key, String[] fields) throws SQLException {
        Optional<Class<?>> partitionIdType = key.partition.getIdType();
        SQLStmt chosenUpdateStmt = partitionIdType.isPresent() ? updateStmtWithGeoPartition : updateStmt;
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, chosenUpdateStmt, key.name);
        stmt.setInt(11, key.name);
        if (partitionIdType.isPresent()) {
            if (partitionIdType.get().equals(Integer.class)) {
                stmt.setInt(12, key.partition.getIntId());
            } else if (partitionIdType.get().equals(String.class)) {
                stmt.setString(12, key.partition.getStringId());
            }
        }
        for (int i = 0; i < fields.length; i++) {
            stmt.setString(i + 1, fields[i]);
        }
        return stmt;
    }

    private final SQLStmt scanStmt = new SQLStmt(
            "SELECT * FROM " + TABLE_NAME + " WHERE YCSB_KEY > ? AND YCSB_KEY < ?");

    private PreparedStatement prepareScanStmt(Connection conn, Key start, int count, List<String[]> results)
            throws SQLException {
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, scanStmt, start.name,
                start.name + count);
        return stmt;
    }
}
