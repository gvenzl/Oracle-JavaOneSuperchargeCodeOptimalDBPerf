package com.gvenzl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class RowByRow {

    private static final int rows = 10000;

    public static void main(String[] args) throws SQLException, InterruptedException {

        System.out.println("Test set based inserts with " + rows + " rows...");
        insertRowByRow(rows);
        System.out.println("Sleep for " + Utils.getPause() + " seconds.");
        Thread.sleep(Utils.getPause() * 1000);
        insertSetBased(rows);
    }

    private static void insertRowByRow(int rows) throws SQLException {
        Utils.resetTable();
        Connection conn = Utils.getConnection();

        PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO TEST (id, text, created_tms, last_upd_tms) VALUES (?,?,SYSDATE,SYSDATE)");

        long start = System.currentTimeMillis();

        for(int i = 1 ; i <= rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "This is the row with the value of " + i);
            stmt.executeUpdate();
        }
        conn.commit();

        long end = System.currentTimeMillis();
        System.out.println("Elapsed time(ms) for row by row insert: " + (end-start));
        stmt.close();

    }

    private static void insertSetBased(int rows) throws SQLException {
        Utils.resetTable();
        Connection conn = Utils.getConnection();

        PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO TEST (id, text, created_tms, last_upd_tms) VALUES (?,?,SYSDATE,SYSDATE)");

        long start = System.currentTimeMillis();

        for(int i=1;i<=rows;i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "This is the row with the value of " + i);
            stmt.addBatch();
        }
        stmt.executeBatch();
        conn.commit();

        long end = System.currentTimeMillis();
        System.out.println("Elapsed time(ms) for set based insert: " + (end-start));
        stmt.close();

    }
}
