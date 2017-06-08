package com.gvenzl.rowbyrow;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.gvenzl.DBUtils;

public class RowByRow {

	private static int pause = 5;
	
	public static void main(String[] args) throws SQLException, IOException, InterruptedException {
		
		int rows = 10000;
		System.out.println("Test set based inserts with " + rows + " rows...");
		insertRowByRow(rows);
		System.out.println("Sleep for " + pause + " seconds.");
		Thread.sleep(pause*1000);
		insertSetBased(rows);
	}
		
	public static void insertRowByRow(int rows) throws SQLException {
		DBUtils.resetTable();
		Connection conn = DBUtils.getConnection();

		PreparedStatement stmt = conn.prepareStatement(
			"INSERT INTO TEST (id, text, created_tms, last_upd_tms) VALUES (?,?,SYSDATE,SYSDATE)");
		
		long start = System.currentTimeMillis();
		
		for(int i=1;i<=rows;i++) {
			stmt.setInt(1, i);
			stmt.setString(2, "This is the row with the value of " + i);
			stmt.executeUpdate();
		}
		conn.commit();
		
		long end = System.currentTimeMillis();
		System.out.println("Elapsed time(ms) for row by row insert: " + (end-start));
		stmt.close();
		
	}

	public static void insertSetBased(int rows) throws SQLException {
		DBUtils.resetTable();
		Connection conn = DBUtils.getConnection();
		
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
