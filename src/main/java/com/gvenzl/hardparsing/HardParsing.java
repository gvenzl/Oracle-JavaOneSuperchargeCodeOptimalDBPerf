package com.gvenzl.hardparsing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.gvenzl.DBUtils;

public class HardParsing {

	public static void main(String[] args) throws SQLException {
	
		int rows = 1000;
		System.out.println("Test hard and soft parsing selecting " + rows + " rows...");
		selectHardParse(rows);
		selectSoftParse(rows);
	}
	
	public static void selectHardParse(int rows) throws SQLException {
		DBUtils.resetSharedPool();
		Connection conn = DBUtils.getConnection();
		
		long start = System.currentTimeMillis();
		
		for(int i=1;i<=rows;i++) {
			PreparedStatement stmt = conn.prepareStatement(
					"SELECT text FROM TEST WHERE id = " + i);
			ResultSet rslt = stmt.executeQuery();
			rslt.next();
			// Fetch the column value to include fetching time
			rslt.getString(1);
			stmt.close();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Elapsed time(ms) for row by row insert: " + (end-start));
	}

	public static void selectSoftParse(int rows) throws SQLException {
		DBUtils.resetSharedPool();
		Connection conn = DBUtils.getConnection();
		
		long start = System.currentTimeMillis();
		
		for(int i=1;i<=rows;i++) {
			PreparedStatement stmt = conn.prepareStatement(
					"SELECT text FROM TEST WHERE id = ?");
			stmt.setInt(1, i);
			ResultSet rslt = stmt.executeQuery();
			rslt.next();
			// Fetch the column value to include fetching time
			rslt.getString(1);
			stmt.close();
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Elapsed time(ms) for row by row insert: " + (end-start));
	}
	
}
