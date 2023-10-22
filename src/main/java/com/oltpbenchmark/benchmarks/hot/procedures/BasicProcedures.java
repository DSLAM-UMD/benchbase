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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

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
                    for (int i = 0; i < results.length; i++) {
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
        try (PreparedStatement stmt = this.prepareScanStmt(conn, start, count)) {
            try (ResultSet r = stmt.executeQuery()) {
                while (r.next()) {
                    ResultSetMetaData meta = r.getMetaData();
                    String[] data = new String[meta.getColumnCount()];
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
                    for (int i = 0; i < results.length; i++) {
                        results[i] = r.getString(i + 1);
                    }
                }
            }
        }
        try (PreparedStatement stmt = this.prepareUpdateStmt(conn, key, fields)) {
            stmt.executeUpdate();
        }
    }

    public PreparedStatement prepareInsertStmt(Connection conn, Key key, String[] vals) throws SQLException {
        String tableName = TABLE_NAME + "_" + key.partition.getId();
        SQLStmt insertStmt = new SQLStmt(
                "INSERT INTO " + tableName + " VALUES (?,?,?,?,?,?,?,?,?,?,?)");
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, insertStmt, key.name);
        for (int i = 2; i <= 11; i++) {
            stmt.setString(i, vals[i - 2]);
        }
        return stmt;
    }

    private PreparedStatement prepareReadStmt(Connection conn, Key key) throws SQLException {
        String tableName = TABLE_NAME + "_" + key.partition.getId();
        SQLStmt readStmt = new SQLStmt(
                "SELECT * FROM " + tableName + " where YCSB_KEY=?");

        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, readStmt, key.name);
        return stmt;
    }

    private PreparedStatement prepareUpdateStmt(Connection conn, Key key, String[] fields) throws SQLException {
        String tableName = TABLE_NAME + "_" + key.partition.getId();
        SQLStmt updateStmt = new SQLStmt(
                "UPDATE " + tableName + " SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
                        "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?");
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, updateStmt);
        for (int i = 1; i <= 10; i++) {
            stmt.setString(i, fields[i - 1]);
        }
        stmt.setInt(11, key.name);
        return stmt;
    }

    private PreparedStatement prepareScanStmt(Connection conn, Key start, int count)
            throws SQLException {
        String tableName = TABLE_NAME + "_" + start.partition.getId();
        // Selecting all columns results in too much data to be transferred over the
        // network and causes a bottleneck there. This obscure the evaluation of the
        // database systems themselves, so we aggregate the result.
        SQLStmt scanStmt = new SQLStmt(
                "SELECT YCSB_KEY, "
                        + "LENGTH(FIELD1) + "
                        + "LENGTH(FIELD2) + "
                        + "LENGTH(FIELD3) + "
                        + "LENGTH(FIELD4) + "
                        + "LENGTH(FIELD5) + "
                        + "LENGTH(FIELD6) + "
                        + "LENGTH(FIELD7) + "
                        + "LENGTH(FIELD8) + "
                        + "LENGTH(FIELD9) + "
                        + "LENGTH(FIELD10) AS TOTAL FROM "
                        + tableName + " WHERE YCSB_KEY >= ? AND YCSB_KEY < ?");
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, scanStmt, start.name,
                start.name + count);
        return stmt;
    }
}
