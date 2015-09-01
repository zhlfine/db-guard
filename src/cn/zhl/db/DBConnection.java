package cn.zhl.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConnection {
	static Logger logger = LoggerFactory.getLogger(DBConnection.class);

	private Connection connection = null;
	private DBType dbType = null;

	private boolean inTransaction = false;

	public DBConnection(DBType dbType, String url, String username, String password) throws SQLException {
		this.dbType = dbType;

		connection = dbType.getConnection(url, username, password);
		connection.setAutoCommit(false);
	}

	public DBType getDBType() {
		return dbType;
	}

	public boolean isValid() {
		try {
			return connection.isValid(5);
		} catch (SQLException e) {
			logger.warn("DB connection is invalid", e);
			return false;
		}
	}

	public Connection getUnderlyingConnection() {
		return connection;
	}

	public Statement createStatement() throws SQLException {
		if (connection == null) {
			return null;
		}
		return connection.createStatement();
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		if (connection == null) {
			return null;
		}
		return connection.prepareStatement(sql);
	}

	public void startTransaction() {
		if (inTransaction) {
			throw new DBException("Nested transaction is not supported");
		}
		if (connection != null) {
			inTransaction = true;
		}
	}

	public void commit() throws SQLException {
		if (connection != null) {
			if (inTransaction) {
				connection.commit();
				inTransaction = false;
			}
		}
	}

	public void rollback() throws SQLException {
		if (connection != null) {
			if (inTransaction) {
				connection.rollback();
				inTransaction = false;
			}
		}
	}

	public void close() throws SQLException {
		if (connection == null) {
			return;
		}

		logger.debug("Closing DB connection");
		try {
			rollback();
		} finally {
			Connection tmp = connection;
			connection = null;
			tmp.close();
		}
	}

}
