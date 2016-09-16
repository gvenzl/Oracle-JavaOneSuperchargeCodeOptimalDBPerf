package com.gvenzl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import oracle.jdbc.pool.OracleDataSource;

public class DBUtils {
	
	private static Connection conn;
	private static Connection sysConn;
	
	public static Connection getSysConnection() throws SQLException {
		if (null == sysConn || sysConn.isClosed()) {
			OracleDataSource ods = new OracleDataSource();
			ods.setURL("jdbc:oracle:thin:sys as sysdba/oracle@//localhost:1521/ORCLCDB");
			sysConn = ods.getConnection();
			sysConn.setAutoCommit(false);
		}
		return sysConn;
	}
	
	public static Connection getConnection() throws SQLException {
		if (null == conn || conn.isClosed()) {
			OracleDataSource ods = new OracleDataSource();
			ods.setURL("jdbc:oracle:thin:test/test@//localhost:1521/ORCLPDB1");
			conn = ods.getConnection();
			conn.setAutoCommit(false);
		}
		createTestTable(conn);
		return conn;
	}

	private static void createTestTable(Connection conn) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(
			"CREATE TABLE test (id NUMBER, text VARCHAR2(255), created_tms DATE, last_upd_tms DATE)");
		try {
			stmt.execute();
		}
		catch (SQLException e) {
			// ORA-00955: name is already used by an existing object
			if (e.getErrorCode() != 955) {
				throw e;
			}
		}
	}

	public static void resetTable() throws SQLException {
		getConnection().prepareStatement("TRUNCATE TABLE TEST").execute();
	}
	
	public static void resetSharedPool() throws SQLException {
		getSysConnection().prepareStatement("ALTER SYSTEM FLUSH SHARED_POOL").execute();
	}
}
