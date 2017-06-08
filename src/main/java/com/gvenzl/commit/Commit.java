package com.gvenzl.commit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.gvenzl.DBUtils;

public class Commit {
	
	private static int pause = 5;
	
	public static void main(String[] args) throws SQLException, IOException, InterruptedException {
		
		int rows = 10000;
		System.out.println("Test commit with " + rows + " rows...");
		sqlWithCommitEveryRow(rows);
		System.out.println("Sleep for " + pause + " seconds.");
		Thread.sleep(pause*1000);
		sqlWithCommitAtEnd(rows);
	}
		
	public static void sqlWithCommitEveryRow(int rows) throws SQLException, IOException {
		DBUtils.resetTable();
		Connection conn = DBUtils.getConnection();

		PreparedStatement stmt = conn.prepareStatement(
			"INSERT INTO TEST (id, text, created_tms, last_upd_tms) VALUES (?,?,SYSDATE,SYSDATE)");
		
		long start = System.currentTimeMillis();
		
		for(int i=1;i<=rows;i++) {
			stmt.setInt(1, i);
			stmt.setString(2, "This is the row with the value of " + i);
			stmt.executeUpdate();
			conn.commit();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Elapsed time(ms) for commit after every row: " + (end-start));
		stmt.close();
		
	}

	public static void sqlWithCommitAtEnd(int rows) throws SQLException, IOException {
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
		System.out.println("Elapsed time(ms) for commit at the end: " + (end-start));
		stmt.close();
		
	}
}
