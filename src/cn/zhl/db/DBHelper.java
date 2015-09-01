package cn.zhl.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBHelper {
	static Logger logger = LoggerFactory.getLogger(DBHelper.class);

	public static int executeUpdate(DBContext ctx, String sql) throws SQLException {
		int result;
		Statement stmt = null;
		try {
			logger.debug("SQL: {}", sql);
			stmt = ctx.getConnection().createStatement();
			result = stmt.executeUpdate(sql);
			logger.debug("SQL: {} rows affected", result);
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (stmt != null) stmt.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}
		return result;
	}
  
	public static void batchUpdate(DBContext ctx, List<String> sqls) throws SQLException {
		Statement stmt = null;
		try {
			if(logger.isDebugEnabled()){
				for (int i = 0; i < sqls.size(); i++) {
					String sql = sqls.get(i);
					logger.debug("SQL {}: {}", i, sql);
				}
			}
			stmt = ctx.getConnection().createStatement();
			for (String sql : sqls) {	
				stmt.addBatch(sql);
			}
			int[] result = stmt.executeBatch();
			if(logger.isDebugEnabled()){
				logger.debug("SQL: rows affected {}", Arrays.toString(result));
			}
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (stmt != null) stmt.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}
	}
	
	public static void batchUpdate(PreparedStatement stmt, List<Object[]> argsList) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("SQL: {}", stmt.toString());
			for (Object[] args : argsList) {
				logger.debug("SQL args: {}", Arrays.toString(args));
			}
		}
		for (Object[] args : argsList) {
			stmt.clearParameters();
			for (int i = 0; i < args.length; i++) {
				Object value = args[i];
				stmt.setObject(i + 1, value);
			}
			stmt.addBatch();
		}
		int[] result = stmt.executeBatch();
		if (logger.isDebugEnabled()) {
			logger.debug("SQL: rows affected {}", Arrays.toString(result));
		}
	}

	public static int executePreparedInsert(PreparedStatement stmt) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("SQL: {}", stmt.toString());
		}
		int result = stmt.executeUpdate();
		logger.debug("SQL: {} rows inserted", result);
		return result;
	}
	
	public static int executePreparedInsert(PreparedStatement stmt, Object[] args) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("SQL: {}, args: {}", stmt.toString(), Arrays.toString(args));
		}
		for (int i = 0; i < args.length; i++) {
			Object value = args[i];
			stmt.setObject(i + 1, value);
		}
		int result = stmt.executeUpdate();
		logger.debug("SQL: {} rows inserted", result);
		return result;
	}

	public static int executePreparedUpdate(PreparedStatement stmt) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("SQL: {}", stmt.toString());
		}
		int result = stmt.executeUpdate();
		logger.debug("SQL: {} rows affected", result);
		return result;
	}
	
	public static int executePreparedUpdate(PreparedStatement stmt, Object[] args) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("SQL: {}, args: {}", stmt.toString(), Arrays.toString(args));
		}
		for (int i = 0; i < args.length; i++) {
			Object value = args[i];
			stmt.setObject(i + 1, value);
		}
		int result = stmt.executeUpdate();
		logger.debug("SQL: {} rows affected", result);
		return result;
	}

	public static int updateBlobValue(DBContext ctx, String sql, Object object) throws SQLException {
		int result;
		
		PreparedStatement stmt = null;
		ObjectOutputStream objectOutputStream = null;
		ByteArrayOutputStream byteArrayOutputStream = null;
		ByteArrayInputStream byteArrayInputStream = null;
		try {
			stmt = ctx.getConnection().prepareStatement(sql);
			
			// Insert the blob.
			byteArrayOutputStream = new ByteArrayOutputStream();
			objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			objectOutputStream.writeObject(object);
			objectOutputStream.flush();
			byte[] bytes = byteArrayOutputStream.toByteArray();
			byteArrayInputStream = new ByteArrayInputStream(bytes);
			stmt.setBinaryStream(1, byteArrayInputStream, bytes.length);

			logger.debug("SQL: {}", sql);
			result = stmt.executeUpdate();
			logger.debug("SQL: {} rows affected", result);
		} catch (SQLException e) {
			throw e;
		} catch (IOException e) {
			throw new SQLException(e);
		} finally {
			try {
				if (objectOutputStream != null) objectOutputStream.close();
				if (byteArrayOutputStream != null) byteArrayOutputStream.close();
				if (byteArrayInputStream != null) byteArrayInputStream.close();
				if (stmt != null) stmt.close();
			} catch (IOException e) {
				throw new SQLException(e);
			} catch (SQLException e) {
				throw e;
			}
		}

		return result;
	}

	public static <T> List<T> select(DBObjectBuilder<T> objBuilder, PreparedStatement stmt) throws SQLException {
		ArrayList<T> beans = new ArrayList<T>();

		ResultSet rs = null;
		try {
			logger.debug("SQL: {}", stmt.toString());
			rs = stmt.executeQuery();
			while (rs.next()) {
				beans.add(objBuilder.buildFromResultSet(rs));
			}
			logger.debug("SQL: {} rows returned", beans.size());
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (rs != null) rs.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}

		return beans;
	}
  
	public static <T> List<T> select(DBContext ctx, DBObjectBuilder<T> objBuilder, String sql) throws SQLException {
		ArrayList<T> beans = new ArrayList<T>();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			logger.debug("SQL: {}", sql);
			stmt = ctx.getConnection().createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				beans.add(objBuilder.buildFromResultSet(rs));
			}
			logger.debug("SQL: {} rows returned", beans.size());
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (rs != null) rs.close();
				if (stmt != null) stmt.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}

		return beans;
	}

	public static <T> T selectUnique(DBContext ctx, DBObjectBuilder<T> objBuilder, String sql) throws SQLException {
		List<T> beans = select(ctx, objBuilder, sql);
		if (beans.size() == 1) {
			return beans.get(0);
		} else if (beans.size() > 1) {
			throw new SQLException("More than one rows returned");
		}
		return null;
	}

	public static <T> T selectUnique(DBObjectBuilder<T> objBuilder, PreparedStatement stmt) throws SQLException {
		List<T> beans = select(objBuilder, stmt);
		if (beans.size() == 1) {
			return beans.get(0);
		} else if (beans.size() > 1) {
			throw new SQLException("More than one rows returned");
		}
		return null;
	}

	public static Object selectValue(DBContext ctx, String sql) throws SQLException {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			logger.debug("SQL: {}", sql);
			stmt = ctx.getConnection().createStatement();
			rs = stmt.executeQuery(sql);

			if (rs.getMetaData().getColumnCount() > 1) {
				throw new SQLException("More than one columns returned");
			}

			if (!rs.next()) {
				logger.debug("SQL: 0 rows returned");
				return null;
			}

			Object result = rs.getObject(1);

			if (rs.next()) {
				throw new SQLException("More than one rows returned");
			}

			logger.debug("SQL: result {}", result);
			return result;
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (rs != null) rs.close();
				if (stmt != null) stmt.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}
	}
  
	public static Object selectValue(PreparedStatement stmt) throws SQLException {
		ResultSet rs = null;
		try {
			logger.debug("SQL: {}", stmt.toString());
			rs = stmt.executeQuery();

			if (rs.getMetaData().getColumnCount() > 1) {
				throw new SQLException("More than one columns returned");
			}

			if (!rs.next()) {
				logger.debug("SQL: 0 rows returned");
				return null;
			}

			Object result = rs.getObject(1);

			if (rs.next()) {
				throw new SQLException("More than one rows returned");
			}

			logger.debug("SQL: result {}", result);
			return result;
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (rs != null) rs.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}
	}

	public static int count(DBContext ctx, String sql) throws SQLException {
		Object obj = selectValue(ctx, sql);
		if (obj == null){
			return 0;
		}
		
		if (obj instanceof Number) {
			return ((Number) obj).intValue();
		} else {
			throw new RuntimeException("Cannot cast " + obj + " to int");
		}
	}
  
	public static int count(PreparedStatement stmt) throws SQLException {
		Object obj = selectValue(stmt);
		if (obj == null){
			return 0;
		}
		
		if (obj instanceof Number) {
			return ((Number) obj).intValue();
		} else {
			throw new RuntimeException("Cannot cast " + obj + " to int");
		}
	}

	public static List<Map<String, Object>> selectMap(DBContext ctx, String sql) throws SQLException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

		Statement stmt = null;
		ResultSet rs = null;
		try {
			logger.debug("SQL: {}", sql);
			stmt = ctx.getConnection().createStatement();
			rs = stmt.executeQuery(sql);
			ResultSetMetaData metaData = rs.getMetaData();

			while (rs.next()) {
				HashMap<String, Object> map = new HashMap<String, Object>();
				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					String columnName = metaData.getColumnLabel(i);
					map.put(columnName, rs.getObject(i));
				}
				result.add(map);
			}
			logger.debug("SQL: {} rows returned", result.size());
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (rs != null) rs.close();
				if (stmt != null) stmt.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}

		return result;
	}
  
	public static List<Map<String, Object>> selectMap(PreparedStatement stmt) throws SQLException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

		ResultSet rs = null;
		try {
			logger.debug("SQL: {}", stmt.toString());

			rs = stmt.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();

			while (rs.next()) {
				HashMap<String, Object> map = new HashMap<String, Object>();

				for (int i = 1; i <= metaData.getColumnCount(); i++) {
					String columnName = metaData.getColumnLabel(i);
					map.put(columnName, rs.getObject(i));
				}
				result.add(map);
			}
			logger.debug("SQL: {} rows returned", result.size());
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (rs != null) rs.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}
		return result;
	}
  
	public static List<String> getTableNames(DBContext ctx, String tableNamePattern) throws SQLException {
		List<String> result = new ArrayList<String>();
		
		ResultSet rs = null;
		try {
			Connection conn = ctx.getConnection().getUnderlyingConnection();
			rs = conn.getMetaData().getTables(null, null, tableNamePattern, new String[] {"TABLE"});
			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				result.add(tableName);
			}
			
			return result;
		} catch (SQLException e) {
			throw e;
		} finally {
			try {
				if (rs != null) rs.close();
			} catch (SQLException e) {
				logger.warn("SQL: error", e);
			}
		}
	}

}
