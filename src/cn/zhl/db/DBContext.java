package cn.zhl.db;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DBContext {
	private static ThreadLocal<DBConnection> connection = new ThreadLocal<DBConnection>();
	private static ThreadLocal<DBContext> context = new ThreadLocal<DBContext>();

	private static List<DBType> supportedDBTypes = new ArrayList<DBType>();
	private static Map<String, DBType> dbTypes = new HashMap<String, DBType>();
	private static Map<String, DBInfo> dbInfos = new HashMap<String, DBInfo>();
	private static Map<String, DBConnectionPool> connectionPools = new HashMap<String, DBConnectionPool>();
	private static Map<String, GenericDAO<?>> daoInstances = new HashMap<String, GenericDAO<?>>();

	static {
		supportedDBTypes.add(new DBType.Postgres());
		supportedDBTypes.add(new DBType.MySQL());
		supportedDBTypes.add(new DBType.Oracle());
	}
	
	public static String DEFAULTDB = "default";
	
	private String dbId = null;
	
	public DBContext(String dbId) {
		this.dbId = dbId;
	}

	public DBContext() {
		this(DEFAULTDB);
	}

	public void registerDBInfo(DBInfo dbInfo) {
		dbInfos.put(getDbId(), dbInfo);
	}
	
	public String getDbId() {
		return dbId;
	}

	public DBInfo getDBInfo() {
		DBInfo dbInfo = dbInfos.get(dbId);
		if (dbInfo == null) {
			throw new RuntimeException("DB " + dbId + " is not registered");
		}
		return dbInfo;
	}

	public synchronized DBType getDBType() {
		DBType dbType = dbTypes.get(dbId);
		if (dbType == null) {
			DBInfo dbInfo = getDBInfo();
			for (DBType type : supportedDBTypes) {
				if (type.isDriverMatch(dbInfo.getDriver())) {
					dbType = type;
					dbTypes.put(dbId, type);
					break;
				}
			}
			if(dbType == null){
				throw new RuntimeException("Unsupported DB type " + dbInfo.getDriver());
			}
		}
		return dbType;
	}
	
	private DBConnection getConnectionFromPool() {
		DBConnectionPool pool = null;
		synchronized (connectionPools) {
			pool = connectionPools.get(dbId);
			if (pool == null) {
				pool = new DBConnectionPool(dbId, getDBType(), getDBInfo());
				connectionPools.put(dbId, pool);
			}
		}
		return pool.getConnection();
	}
	
	private void returnConnectionToPool(DBConnection conn){
		DBConnectionPool pool = null;
		synchronized (connectionPools) {
			pool = connectionPools.get(dbId);
		}
		if (pool != null) {
			pool.returnConnection(conn);
		}else{
			throw new RuntimeException("Connection pool " + dbId + " is not found");
		}
	}

	public static DBContext getContext() {
		DBContext ctx = context.get();
		if (ctx == null) {
			throw new NullPointerException("Out of transaction");
		}
		return ctx;
	}

	public DBConnection getConnection() {
		return getConnection(false);
	}

	private DBConnection getConnection(boolean create) {
		DBConnection conn = connection.get();
		if (conn == null && create) {
			conn = getConnectionFromPool();
			connection.set(conn);
		}
		return conn;
	}

	private void returnConnection() {
		DBConnection conn = connection.get();
		if (conn != null && conn.isValid()) {
			returnConnectionToPool(conn);
		}
		connection.set(null);
	}

	public void startTransaction() {
		context.set(this);
		DBConnection conn = getConnection(true);
		if (conn == null) {
			throw new DBException("Fail to get DB connection");
		}
		conn.startTransaction();
	}

	public void commitTransaction() {
		context.set(null);
		try {
			DBConnection conn = getConnection(false);
			if (conn != null) {
				conn.commit();
			}
		} catch (SQLException e) {
			throw new DBException(e);
		} finally {
			returnConnection();
		}
	}

	public void rollbackTransaction() {
		context.set(null);
		try {
			DBConnection conn = getConnection(false);
			if (conn != null) {
				conn.rollback();
			}
		} catch (SQLException e) {
			throw new DBException(e);
		} finally {
			returnConnection();
		}
	}
	
	public void closeAllConnections() {
		DBConnectionPool pool = null;
		synchronized (connectionPools) {
			pool = connectionPools.get(dbId);
		}
		if (pool != null) {
			pool.closeAllCollections();
		}
	}
	
	public String getConnectionPoolStatus(){
		DBConnectionPool pool = null;
		synchronized (connectionPools) {
			pool = connectionPools.get(dbId);
		}
		return pool.getStatus();
	}
	
	@SuppressWarnings("unchecked")
	public static <T> GenericDAO<T> getDAO(Class<T> beanClass){
		synchronized (daoInstances) {
			String key = beanClass.getName();
			GenericDAO<T> dao = (GenericDAO<T>)daoInstances.get(key);
			if(dao == null){
				String daoClass = beanClass.getName()+"$DB";
				try {
					dao = (GenericDAO<T>)Class.forName(daoClass).newInstance();
					daoInstances.put(key, dao);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}	
			return dao;
		}
	}
	
	public static <T> void registerDBObject(Class<T> beanClass){
		getDAO(beanClass);
	}
	
	private static void caculateDAODependency(List<GenericDAO<?>> result, Collection<GenericDAO<?>> rawList){
		for(GenericDAO<?> dao : rawList){
			List<GenericDAO<?>> dependentDAOes = dao.getDependentDAOes();
			if(dependentDAOes.size() > 0){
				Iterator<GenericDAO<?>> iter = dependentDAOes.iterator();
				while(iter.hasNext()){
					GenericDAO<?> dependent = iter.next();
					if(result.contains(dependent)){
						iter.remove();
					}
				}
				if(dependentDAOes.size() > 0){
					caculateDAODependency(result, dependentDAOes);
				}
			}
			if(!result.contains(dao)){
				result.add(dao);
			}
		}
	}
	
	public void generateSchema(File file){
		List<GenericDAO<?>> list = new ArrayList<GenericDAO<?>>();
		caculateDAODependency(list, daoInstances.values());
		
		FileWriter out = null;
		try{
			out = new FileWriter(file);		
			Iterator<GenericDAO<?>> iter = list.iterator();	
			while(iter.hasNext()){
				GenericDAO<?> dao = iter.next();
				String ddl = dao.getDDL(this);
				out.write(ddl);
				out.write("\r\n\r\n");
			}
		}catch(Exception e){
			throw new RuntimeException(e);
		}finally{
			if(out != null){
				try {out.close();} catch (IOException e) {}
			}
		}
	}
	
	public void rebuildDatabase(){
		List<GenericDAO<?>> list = new ArrayList<GenericDAO<?>>();
		caculateDAODependency(list, daoInstances.values());
		
		startTransaction();
		try{
			for(int i = list.size()-1; i >=0; i--){
				GenericDAO<?> dao = list.get(i);
				if(dao.isTableExisting(this)){
					dao.dropTable(this);
				}
			}
			for(int i = 0; i < list.size(); i++){
				GenericDAO<?> dao = list.get(i);
				dao.createTable(this);
			}
			commitTransaction();
		}catch(Exception e){
			rollbackTransaction();
			throw new RuntimeException(e);
		}
	}
	
	public Object getTxService(Object serviceImpl){
		DBTxHandler txHandler = new DBTxHandler(serviceImpl, this);
		return Proxy.newProxyInstance(serviceImpl.getClass().getClassLoader(), serviceImpl.getClass().getInterfaces(), txHandler);
	}

}