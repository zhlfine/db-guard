package cn.zhl.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConnectionPool {
	static Logger logger = LoggerFactory.getLogger(DBConnectionPool.class);

	private String dbId;
	private DBType dbType;
	private DBInfo dbInfo;

	private List<DBConnection> allConnections = new ArrayList<DBConnection>();
	private LinkedBlockingQueue<DBConnection> idleConnections = new LinkedBlockingQueue<DBConnection>();

	public DBConnectionPool(String dbId, DBType dbType, DBInfo dbInfo) {
		this.dbId = dbId;
		this.dbType = dbType;
		this.dbInfo = dbInfo;
	}

	public String getDbId() {
		return dbId;
	}

	public synchronized DBConnection getConnection() {
		DBConnection conn = idleConnections.poll();
		if (conn == null) {
			if (allConnections.size() < dbInfo.getPoolSize()) {
				try {
					logger.info("Creating DB connection: {} {}/****", dbInfo.getUrl(), dbInfo.getUsername());
					conn = new DBConnection(dbType, dbInfo.getUrl(), dbInfo.getUsername(), dbInfo.getPassword());			
					allConnections.add(conn);
				} catch (SQLException e) {
					throw new DBException("Fail to create DB connection", e);
				}
			} else {
				try {
					conn = idleConnections.take();
				} catch (InterruptedException e) {
					logger.warn("Fail to get DB connection from pool");
				}
			}
		}
		return conn;
	}

	public synchronized void returnConnection(DBConnection conn) {
		idleConnections.offer(conn);
	}

	public synchronized void closeAllCollections() {
		for (DBConnection conn : allConnections) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.warn("Fail to close DB connection", e);
			}
		}
		allConnections.clear();
		idleConnections.clear();
	}

	public synchronized String getStatus() {
		return "DB " + dbId + ", total " + allConnections.size() + ", idle " + idleConnections.size();
	}

}
