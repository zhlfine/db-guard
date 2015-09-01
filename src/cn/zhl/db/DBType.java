package cn.zhl.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public abstract class DBType {

    public Connection getConnection(String url, String username, String password)throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }
    
    public boolean isDriverMatch(String driver) {
    	String key = getDriverKey();
		if (driver.indexOf(key) != -1) {
			try {
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
			return true;
		}
		return false;
	}
    
    public abstract String limit(String sql, int limit, int offset);
    
    public abstract String getStringType(int length);
    public abstract String getIntType();
    public abstract String getLongType();
    public abstract String getDoubleType();
    public abstract String getBooleanType();
    public abstract String getDateType();
    
    protected abstract String getDriverKey();
    public abstract String getValueInSQL(Date date);
    public String getValueInSQL(boolean v)	{ return v ? "1" : "0"; }
    
    
    public static class MySQL extends DBType {   
        public String limit(String sql, int limit, int offset){
            return sql + " limit " + limit + ", " + offset;
        }

    	protected String getDriverKey() { return "mysql"; };
    	
    	public String getStringType(int length){ return (length==0 ? "text" : "varchar("+length+")"); }
    	public String getIntType()		{ return "int"; }
        public String getLongType()		{ return "bigint"; }
        public String getDoubleType()	{ return "double"; }
        public String getBooleanType()	{ return "tinyint(1)"; }
        public String getDateType()		{ return "datetime"; }
        
        public String getValueInSQL(Date date){
    		String str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    		return "str_to_date('" + str + "', '%Y-%m-%d %H:%i:%s')";
        }
    }
    
    public static class Oracle extends DBType {
        public String limit(String sql, int limit, int offset){
            int startPoint = offset+1;
            int endPoint = offset + limit;
            return  "select * from ( " +
            		    "select tt.*, ROWNUM rn from (" + sql + ") tt " +
            				"where ROWNUM <= " + endPoint +
            		") where rn >= " + startPoint;
        }
        
        protected String getDriverKey() { return "Oracle"; };
    	
    	public String getStringType(int length){ return (length==0 ? "varchar2(4000)" : "varchar2("+length+")"); }
    	public String getIntType()		{ return "number(10)"; }
        public String getLongType()		{ return "number(19)"; }
        public String getDoubleType()	{ return "number(19,2)"; }
        public String getBooleanType()	{ return "number(1)"; }
        public String getDateType()		{ return "timestamp"; }
        
        public String getValueInSQL(Date date){
    		String str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    		return "to_date('" + str + "', 'yyyy-mm-dd hh24:mi:ss')";
        }
    }
    
    public static class Postgres extends DBType {
    	public String limit(String sql, int limit, int offset){
            return sql + " limit " + limit + " offset " + offset;
        }
    	
    	protected String getDriverKey() { return "postgresql"; };
    	public String getValueInSQL(boolean v)	{ return v ? "TRUE" : "FALSE"; }
    	
    	public String getStringType(int length){ return (length==0 ? "text" : "varchar("+length+")"); }
    	public String getIntType()		{ return "integer"; }
        public String getLongType()		{ return "bigint"; }
        public String getDoubleType()	{ return "float8"; }
        public String getBooleanType()	{ return "boolean"; }
        public String getDateType()		{ return "timestamp"; }
        
        public String getValueInSQL(Date date){
    		String str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    		return "'" + str + "'::timestamp";
        }
    }
    
}
