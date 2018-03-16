package com.gvenzl.hardparsing;

import com.gvenzl.DBUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class HardParsing {

	private static final int pause = 5;
	private static final int rows = 10000;
	
	public static void main(String[] args) throws SQLException, InterruptedException {

		System.out.println("Test hard and soft parsing selecting " + rows + " rows...");
		selectHardParse(rows);
		System.out.println("Sleep for " + pause + " seconds.");
		Thread.sleep(pause * 1000);
		selectSoftParse(rows);
	}
	
	private static void selectHardParse(int rows) throws SQLException {
		DBUtils.resetSharedPool();
		Connection conn = DBUtils.getConnection();
		
		long start = System.currentTimeMillis();
		
		for(int i = 1; i <= rows; i++) {
			PreparedStatement stmt = conn.prepareStatement(
					"SELECT text FROM TEST WHERE id = " + i);
			ResultSet rslt = stmt.executeQuery();
			rslt.next();
			// Fetch the column value to include fetching time
			rslt.getString(1);
			rslt.close();
			stmt.close();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Elapsed time(ms) Hard Parse selects: " + (end-start));
	}

	private static void selectSoftParse(int rows) throws SQLException {
		DBUtils.resetSharedPool();
		Connection conn = DBUtils.getConnection();
		
		long start = System.currentTimeMillis();

		PreparedStatement stmt = conn.prepareStatement(
				"SELECT text FROM TEST WHERE id = ?");
		for(int i = 1; i <= rows; i++) {
			stmt.setInt(1, i);
			ResultSet rslt = stmt.executeQuery();
			rslt.next();
			// Fetch the column value to include fetching time
			rslt.getString(1);
			rslt.close();
		}
		stmt.close();

		long end = System.currentTimeMillis();
		System.out.println("Elapsed time(ms) for soft parse selects: " + (end-start));
	}
	
}
