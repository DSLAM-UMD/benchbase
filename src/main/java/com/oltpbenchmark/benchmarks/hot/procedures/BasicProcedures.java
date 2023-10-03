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
        try (PreparedStatement stmt = this.prepareScanStmt(conn, start, count)) {
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
            "INSERT INTO " + TABLE_NAME + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");

    public PreparedStatement prepareInsertStmt(Connection conn, Key key, String[] vals) throws SQLException {
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, insertStmt, key.name);
        for (int i = 2; i <= 11; i++) {
            stmt.setString(i, vals[i - 2]);
        }
        stmt.setObject(12, key.partition.getId());
        return stmt;
    }

    private final static SQLStmt readStmt = new SQLStmt(
            "SELECT * FROM " + TABLE_NAME + " where YCSB_KEY=? and GEO_PARTITION=?");

    private PreparedStatement prepareReadStmt(Connection conn, Key key) throws SQLException {
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, readStmt, key.name);
        stmt.setObject(2, key.partition.getId());
        return stmt;
    }

    private final SQLStmt updateStmt = new SQLStmt(
            "UPDATE " + TABLE_NAME + " SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
                    "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=? and GEO_PARTITION=?");

    private PreparedStatement prepareUpdateStmt(Connection conn, Key key, String[] fields) throws SQLException {
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, updateStmt);
        for (int i = 1; i <= 10; i++) {
            stmt.setString(i, fields[i - 1]);
        }
        stmt.setInt(11, key.name);
        stmt.setObject(12, key.partition.getId());
        return stmt;
    }

    private final SQLStmt scanStmt = new SQLStmt(
            "SELECT * FROM " + TABLE_NAME + " WHERE YCSB_KEY >= ? AND YCSB_KEY < ? and GEO_PARTITION=?");

    private PreparedStatement prepareScanStmt(Connection conn, Key start, int count)
            throws SQLException {
        PreparedStatement stmt = BasicProcedures.this.getPreparedStatement(conn, scanStmt, start.name,
                start.name + count);
        stmt.setObject(3, start.partition.getId());
        return stmt;
    }
}
