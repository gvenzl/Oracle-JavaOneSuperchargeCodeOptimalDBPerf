package com.gvenzl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class Commit {

    public static void main(String[] args) throws SQLException, InterruptedException {

        int rows = 10000;
        System.out.println("Test with " + rows + " rows commit after every row...");
        sqlWithCommitEveryRow(rows);
        System.out.println("Sleep for " + Utils.getPause() + " seconds.");
        Thread.sleep(Utils.getPause() * 1000L);
        System.out.println("Test with " + rows + " rows commit at the end...");
        sqlWithCommitAtEnd(rows);
    }

    private static void sqlWithCommitEveryRow(int rows) throws SQLException {
        Utils.resetTable();
        Connection conn = Utils.getConnection();

        PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO TEST (id, text, created_tms, last_upd_tms) " +
                        "VALUES (?,?,SYSDATE,SYSDATE)");

        long start = System.currentTimeMillis();

        for(int i = 1; i <= rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "This is the row with the value of " + i);
            stmt.executeUpdate();
            conn.commit();
        }

        long end = System.currentTimeMillis();
        System.out.println("Elapsed time(ms) for commit after every row: " + (end-start));
        stmt.close();

    }

    private static void sqlWithCommitAtEnd(int rows) throws SQLException {
        Utils.resetTable();
        Connection conn = Utils.getConnection();

        PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO TEST (id, text, created_tms, last_upd_tms) " +
                        "VALUES (?,?,SYSDATE,SYSDATE)");

        long start = System.currentTimeMillis();

        for(int i = 1; i <= rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "This is the row with the value of " + i);
            stmt.executeUpdate();
        }
        conn.commit();

        long end = System.currentTimeMillis();
        System.out.println("Elapsed time(ms) for commit at the end: " + (end-start));
        stmt.close();

    }
}
