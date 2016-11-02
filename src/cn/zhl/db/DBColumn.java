package cn.zhl.db;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;

import cn.zhl.db.DBCondition.Oper;
import net.sf.cglib.proxy.Enhancer;

public abstract class DBColumn {

	private String fieldName;
	private String columnName;
	private boolean isPK;
	private boolean useSequence;
	
	public DBColumn(String fieldName, String columnName, boolean isPK, boolean useSequence){
		this.fieldName = fieldName;
		this.columnName = columnName;
		this.isPK = isPK;
		this.useSequence = useSequence;
	}
	public DBColumn(String fieldName, boolean isPK, boolean useSequence){
		this(fieldName, fieldName.toLowerCase(), isPK, useSequence);
	}
	
	public String getFieldName(){
		return fieldName;
	}
	
	public String getColumnName(){
		return columnName;
	}
	
	public boolean useSequence(){
		return useSequence;
	}
	
	public boolean isPK(){
		return isPK;
	}

	public void validate(Object obj) throws DBException{
		if(obj == null) return;
		
		if(isPK){
			Object value = getValue(obj);
			if(value == null && !useSequence){
				throw new DBException("The PK cannot be null");
			}
		}
	}
	
	protected Method getMethod(Class<?> beanClass){
		String methodName = "get" + Character.toTitleCase(getFieldName().charAt(0)) + getFieldName().substring(1);
		try {
			return beanClass.getMethod(methodName);
		} catch (NoSuchMethodException e) {
		}
		return null;
	}
	
	protected Method setMethod(Class<?> beanClass){
		String methodName =  "set" + Character.toTitleCase(getFieldName().charAt(0)) + getFieldName().substring(1);
		Class<?> valueType = getValueType();
		try {
			return beanClass.getMethod(methodName, valueType);
		} catch (NoSuchMethodException e) {
		}
		return null;
	}

	public void setValue(Object obj, Object value){
		try {
			FieldUtils.writeField(obj, getFieldName(), value, true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public Object getValue(Object obj){
		if(obj == null) return null;
		
		try {
			return FieldUtils.readField(obj, getFieldName(), true);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	protected void appendDDL(DBContext ctx, StringBuilder buffer){
		buffer.append("    ");
		if(columnName.length() < 15){
			buffer.append(rightPadding(columnName, 15));
		}else{
			buffer.append(columnName);
		}
		buffer.append(" ").append(getTypeInSQL(ctx));
	}
	
	public void appendOrder(StringBuilder buffer, DBOrder order){
		buffer.append(" order by ");
		buffer.append(columnName).append(" ").append(order.getOrder());
	}	
	
	public void appendColumnName(StringBuilder buffer, String seperator){
		buffer.append(columnName);
	}
	
	public void appendColumnValue(DBContext ctx, StringBuilder buffer, Object value, String seperator){
		String strValue = getValueInSQL(ctx, value);
		buffer.append(strValue);
	}

	protected void appendWhereCondition(DBContext ctx, StringBuilder buffer, Object value, Oper oper){
		if(value == null){
    		if(Oper.Equal == oper){
    			buffer.append(columnName).append(" is null");
    		}else if(Oper.NotEqual == oper){
    			buffer.append(columnName).append(" is not null");
    		}else{
    			throw new RuntimeException("Invalid SQL condition: " + columnName + " " + oper.getOperator() + " null");
    		}
    	}else{
    		String str = getValueInSQL(ctx, value);
    		buffer.append(columnName).append(oper.getOperator()).append(str);
    	}
	}
	
	protected void appendSetCondition(DBContext ctx, StringBuilder buffer, Object value, Oper oper){
		String columnName = this.getColumnName();
		if(value == null){
    		buffer.append(columnName).append(Oper.Equal.getOperator()).append("null");
    	}else{
    		String str = getValueInSQL(ctx, value);
    		buffer.append(columnName).append(Oper.Equal.getOperator()).append(str);
    	}
	}

	public void copyValue(Object from, Object to){
		Object value = getValue(from);
		setValue(to, value);
	}
		
	protected boolean setValueFromResultSet(Object obj, ResultSet rs) throws SQLException{
		Object value = rs.getObject(this.getColumnName());
		if(value != null){
			try {
				FieldUtils.writeField(obj, getFieldName(), value, true);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return true;
		}else{
			return false;
		}
	}
	
	public abstract Object valueOf(Object value);
	public abstract Class<?> getValueType();
	
	public abstract String getTypeInSQL(DBContext ctx);
	public abstract String getValueInSQL(DBContext ctx, Object value);
	
	public DBCondition greaterThan(Object value)	{return new DBCondition(this, value, Oper.Greater);}   
    public DBCondition greaterEqual(Object value)	{return new DBCondition(this, value, Oper.GreaterEqual);}  
    public DBCondition lessThan(Object value)		{return new DBCondition(this, value, Oper.Less);} 
    public DBCondition lessEqual(Object value)		{return new DBCondition(this, value, Oper.LessEqual);} 
    public DBCondition equalsTo(Object value)		{return new DBCondition(this, value, Oper.Equal);}
    public DBCondition notEqual(Object value)		{return new DBCondition(this, value, Oper.NotEqual);}
    public DBCondition like(Object value)			{return new DBCondition(this, value, Oper.Like);}
    public DBCondition notLike(Object value)		{return new DBCondition(this, value, Oper.NotLike);}
    
    private static String rightPadding(String str, int length){
    	String format = "%1$-" + length +"s";
    	return String.format(format, str);
    }
    
	public static class LIST<T> extends DBColumn{
		private Class<T> beanClass;
		private DBColumn fkColumn;
		private boolean preFetch;
		private GenericDAO<T> dao;
		
		public LIST(Class<T> beanClass, String name, DBColumn fkColumn) {
			this(beanClass, name, fkColumn, false);
		}
		public LIST(Class<T> beanClass, String name, DBColumn fkColumn, boolean preFetch) {
			super(name, false, false);
			this.beanClass = beanClass;
			this.fkColumn = fkColumn;
			this.preFetch = preFetch;
			this.dao = DBContext.getDAO(beanClass);
		}

		public Object valueOf(Object value) {
			if(value == null) return null;
			
			if(value instanceof List){
				return value;
			}else{
				throw new RuntimeException("Cannot cast "+value+" to List");
			}
		}
		
		public Class<?> getValueType() {
			return List.class;
		}

		@SuppressWarnings("unchecked")
		protected void setListValue(Object obj){
			List<T> list = null;
					
			if(preFetch){
				DBCondition condition = fkColumn.equalsTo(obj);
				list = dao.query(DBContext.getContext(), condition);
			}else{
				Enhancer enhancer = new Enhancer();
				enhancer.setSuperclass(ArrayList.class);
				enhancer.setCallback(new DBListProxy<T>(obj, beanClass, getFieldName(), fkColumn));
				list = (List<T>)enhancer.create();
			}
			try {
				FieldUtils.writeField(obj, getFieldName(), list, true);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		public void copyValue(Object from, Object to){
			@SuppressWarnings("unchecked")
			List<T> list = (List<T>)getValue(from);
			if(list != null){
				List<T> cloneList = new ArrayList<T>();
				for(int i = 0; i < list.size(); i++){
					T bean = list.get(i);
					T cloneBean = dao.cloneEntity(bean);
					cloneList.add(cloneBean);
				}
				setValue(to, cloneList);
			}
		}

		protected boolean setValueFromResultSet(Object obj, ResultSet rs) throws SQLException {
			throw new DBException("It's a bug if code run here");
		}
		public void appendColumnValue(DBContext ctx, StringBuilder buffer, Object value, String seperator) {
			throw new DBException("It's a bug if code run here");
		}
		protected void appendWhereCondition(DBContext ctx, StringBuilder buffer, Object value, Oper oper) {
			throw new DBException("It's a bug if code run here");
		}
		protected void appendSetCondition(DBContext ctx, StringBuilder buffer, Object value, Oper oper) {
			throw new DBException("It's a bug if code run here");
		}
		protected void appendDDL(DBContext ctx, StringBuilder buffer) {
			throw new DBException("It's a bug if code run here");
		}
		public String getTypeInSQL(DBContext ctx) {
			throw new DBException("It's a bug if code run here");
		}
		public String getValueInSQL(DBContext ctx, Object value) {
			throw new DBException("It's a bug if code run here");
		}
	}
	
	
	public static class OBJECT<T> extends DBColumn{
		private Class<T> beanClass;
		private GenericDAO<T> dao;
		private DBColumn pkColumn;
		private boolean preFetch;
		
		public OBJECT(Class<T> beanClass, String name) {
			super(name, false, false);
			this.preFetch = false;
			init(beanClass);
		}
		public OBJECT(Class<T> beanClass, String name, boolean preFetch) {
			super(name, false, false);
			this.preFetch = preFetch;
			init(beanClass);
		}
		public OBJECT(Class<T> beanClass, String name, boolean preFetch, boolean isPK) {
			super(name, isPK, false);
			this.preFetch = preFetch;
			init(beanClass);
		}
		public OBJECT(Class<T> beanClass, String name, String columnName) {
			super(name, columnName, false, false);
			this.preFetch = false;
			init(beanClass);
		}
		public OBJECT(Class<T> beanClass, String name, String columnName, boolean preFetch) {
			super(name, columnName, false, false);
			this.preFetch = preFetch;
			init(beanClass);
		}
		public OBJECT(Class<T> beanClass, String name, String columnName, boolean preFetch, boolean isPK) {
			super(name, columnName, isPK, false);
			this.preFetch = preFetch;
			init(beanClass);
		}
		
		private void init(Class<T> beanClass){
			this.beanClass = beanClass;
			this.dao = DBContext.getDAO(beanClass);
			this.pkColumn = dao.getPKColumn();
		}
		
		public Class<T> getReferedClass(){
			return beanClass;
		}
		
		public String getReferedTableName(){
			return dao.getTableName();
		}
		
		public DBColumn getReferedColumn(){
			return pkColumn;
		}

		public void validate(Object obj) throws DBException{
			if(obj == null) return;
			
			Object value = getValue(obj);
			pkColumn.validate(value);
		}
		
		public void copyValue(Object from, Object to){
			Object value = getValue(from);
			if(value != null){
				@SuppressWarnings("unchecked")
				Object cloneValue = dao.cloneEntity((T)value);
				setValue(to, cloneValue);
			}
		}

		public void appendColumnValue(DBContext ctx, StringBuilder buffer, Object value, String seperator){
			Object primitiveValue = null;
			if(value != null){
				primitiveValue = pkColumn.getValue(value);
			}
			pkColumn.appendColumnValue(ctx, buffer, primitiveValue, seperator);
		}

		public Class<T> getValueType(){
			return beanClass;
		}

		@SuppressWarnings("unchecked")
		protected boolean setValueFromResultSet(Object obj, ResultSet rs) throws SQLException{		
			Object value = rs.getObject(this.getColumnName());
			if(value != null){
				try {
					T bean = dao.newEntityInstance();
					FieldUtils.writeField(bean, dao.getPKColumn().getFieldName(), value, true);

					if(preFetch){
						Class<T> beanClass = dao.getEnitityClass();
						bean = DBContext.getDAO(beanClass).queryByPK(DBContext.getContext(), bean);
					}else{
						Enhancer enhancer = new Enhancer();
						enhancer.setSuperclass(beanClass);
						enhancer.setCallback(new DBObjectProxy<T>(pkColumn, obj, bean, getFieldName()));
						bean = (T)enhancer.create();
					}

					FieldUtils.writeField(obj, getFieldName(), bean, true);
					
					return true;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}else{
				return false;
			}
		}

		public Object valueOf(Object value) {
			if(value == null) return null;
			
			if(beanClass.isInstance(value)){
				return value;
			}else{
				throw new RuntimeException("Cannot cast "+value+" to "+beanClass.getSimpleName());
			}
		}

		public String getTypeInSQL(DBContext ctx) {
			return pkColumn.getTypeInSQL(ctx);
		}

		public String getValueInSQL(DBContext ctx, Object value) {
			return pkColumn.getValueInSQL(ctx, value);
		}
	}
	
	
	public static abstract class PrimitiveColumn extends DBColumn{	

		public PrimitiveColumn(String name, String columnName, boolean isPK, boolean useSequence){
			super(name, columnName, isPK, useSequence);
		}
		public PrimitiveColumn(String name, boolean isPK, boolean useSequence){
			super(name, isPK, useSequence);
		}

		public String getValueInSQL(DBContext ctx, Object value){
			if(value == null){
				return "null";
			}else{
				return value.toString();
			}
	    }
	}
    
	public static class STRING extends PrimitiveColumn{
		private int length;
		
		public STRING(String name, String columnName, int length, boolean isPK) {
			super(name, columnName, isPK, false);
			this.length = length;
		}
		public STRING(String name, String columnName, int length) {
			super(name, columnName, false, false);
			this.length = length;
		}
		public STRING(String name, int length, boolean isPK) {
			super(name, isPK, false);
			this.length = length;
		}
		public STRING(String name, int length) {
			super(name, false, false);
			this.length = length;
		}
		
		public Class<?> getValueType(){
			return String.class;
		}
		
		public Object valueOf(Object value){
			if(value == null) return null;
			if(value instanceof String){
				return value;
			}else{
				return value.toString();
			}
		}
		
		public void validate(Object obj) throws DBException{
			super.validate(obj);
			if(length > 0){
				String value = (String)getValue(obj);
				if(value != null && value.length() > length){
					throw new DBException("length of column " + getFieldName() +" exceed the limit");
				}
			}
		}
		
		public String getTypeInSQL(DBContext ctx){
			return ctx.getDBType().getStringType(length);
		}
		
		public String getValueInSQL(DBContext ctx, Object value){
			if(value == null){
				return "null";
			}else{
				return "'" + value.toString() + "'";
			}
        }
	}
	
	public static class INT_O extends INT{
		public INT_O(String name) {super(name);}	
		public INT_O(String name, boolean isPK) {super(name, isPK);}
		public INT_O(String name, boolean isPK, boolean isSeq) {super(name, isPK, isSeq);}
		public INT_O(String name, String columnName) {super(name, columnName);}
		public INT_O(String name, String columnName, boolean isPK) {super(name, columnName, isPK);}
		public INT_O(String name, String columnName, boolean isPK, boolean isSeq) {super(name, columnName, isPK, isSeq);}
		
		public Class<?> getValueType(){
			return Integer.class;
		}
	}
	
	public static class INT extends PrimitiveColumn{
		public INT(String name) {super(name, false, false);}
		public INT(String name, boolean isPK) {super(name, isPK, false);}
		public INT(String name, boolean isPK, boolean isSeq) {super(name, isPK, isSeq);}
		public INT(String name, String columnName, boolean isPK) {super(name, columnName, isPK, false);}
		public INT(String name, String columnName) {super(name, columnName, false, false);}
		public INT(String name, String columnName, boolean isPK, boolean isSeq) {super(name, columnName, isPK, isSeq);}
		
		public Class<?> getValueType(){
			return int.class;
		}
		
		public Object valueOf(Object value){
			if(value == null) return null;
			if(value instanceof Integer){
				return value;
			}else if(value instanceof Number){
				return ((Number)value).intValue();
			}
			return Integer.valueOf(value.toString());
		}
		
		public String getTypeInSQL(DBContext ctx){
			return ctx.getDBType().getIntType();
		}
	}
	
	public static class LONG_O extends LONG{
		public LONG_O(String name) {super(name);}	
		public LONG_O(String name, boolean isPK) {super(name, isPK);}
		public LONG_O(String name, boolean isPK, boolean isSeq) {super(name, isPK, isSeq);}
		public LONG_O(String name, String columnName) {super(name, columnName);}
		public LONG_O(String name, String columnName, boolean isPK) {super(name, columnName, isPK);}
		public LONG_O(String name, String columnName, boolean isPK, boolean isSeq) {super(name, columnName, isPK, isSeq);}
		
		public Class<?> getValueType(){
			return Long.class;
		}
	}
	
	public static class LONG extends PrimitiveColumn{
		public LONG(String name) {super(name, false, false);}
		public LONG(String name, boolean isPK) {super(name, isPK, false);}
		public LONG(String name, boolean isPK, boolean isSeq) {super(name, isPK, isSeq);}
		public LONG(String name, String columnName, boolean isPK) {super(name, columnName, isPK, false);}
		public LONG(String name, String columnName) {super(name, columnName, false, false);}
		public LONG(String name, String columnName, boolean isPK, boolean isSeq) {super(name, columnName, isPK, isSeq);}
		
		public Class<?> getValueType(){
			return long.class;
		}
		
		public Object valueOf(Object value){
			if(value == null) return null;
			if(value instanceof Long){
				return value;
			}else if(value instanceof Number){
				return ((Number)value).longValue();
			}
			return Long.valueOf(value.toString());
		}
		
		public String getTypeInSQL(DBContext ctx){
			return ctx.getDBType().getLongType();
		}
    }
	
	public static class DOUBLE_O extends DOUBLE{
		public DOUBLE_O(String name) {super(name);}	
		public DOUBLE_O(String name, boolean isPK) {super(name, isPK);}
		public DOUBLE_O(String name, String alias, boolean isPK) {super(name, alias, isPK);}
		
		public Class<?> getValueType(){
			return Double.class;
		}
	}
	
	public static class DOUBLE extends PrimitiveColumn{
		public DOUBLE(String name) {super(name, false, false);}
		public DOUBLE(String name, boolean isPK) {super(name, isPK, false);}
		public DOUBLE(String name, String columnName, boolean isPK) {super(name, columnName, isPK, false);}
		
		public Class<?> getValueType(){
			return double.class;
		}
		
		public Object valueOf(Object value){
			if(value == null) return null;
			if(value instanceof Double){
				return value;
			}else if(value instanceof Number){
				return ((Number)value).doubleValue();
			}
			return Double.valueOf(value.toString());
		}
		
		public String getTypeInSQL(DBContext ctx){
			return ctx.getDBType().getDoubleType();
		}
    }
	
	public static class BOOLEAN_O extends BOOLEAN{
		public BOOLEAN_O(String name) {super(name);}
		public BOOLEAN_O(String name, boolean isPK) {super(name, isPK);}
		public BOOLEAN_O(String name, String alias, boolean isPK) {super(name, alias, isPK);}
		
		public Class<?> getValueType(){
			return Boolean.class;
		}
	}
	
	public static class BOOLEAN extends PrimitiveColumn{
		public BOOLEAN(String name) {super(name, false, false);}
		public BOOLEAN(String name, boolean isPK) {super(name, isPK, false);}
		public BOOLEAN(String name, String columnName, boolean isPK) {super(name, columnName, isPK, false);}
		
		public Class<?> getValueType(){
			return boolean.class;
		}
		
		public Object valueOf(Object value){
			if(value == null) return null;
			if(value instanceof Boolean){
				return value;
			}else if(value instanceof Number){
				int i = ((Number)value).intValue();
				if(i == 0){
					return Boolean.FALSE;
				}else if(i == 1){
					return Boolean.TRUE;
				}else{
					throw new RuntimeException("Cannot cast "+value+" to Boolean");
				}
			}else if(value instanceof String){
				if("true".equalsIgnoreCase((String)value)){
					return Boolean.TRUE;
				}else if("false".equalsIgnoreCase((String)value)){
					return Boolean.FALSE;
				}else{
					throw new RuntimeException("Cannot cast "+value+" to Boolean");
				}
			}else{
				throw new RuntimeException("Cannot cast "+value+" to Boolean");
			}
		}
		
		protected Method getMethod(Class<?> beanClass){
			String methodName = "is" + Character.toTitleCase(getFieldName().charAt(0)) + getFieldName().substring(1);
			try {
				return beanClass.getMethod(methodName);
			} catch (NoSuchMethodException e) {
			}
			return null;
		}
		
		public String getTypeInSQL(DBContext ctx){
			return ctx.getDBType().getBooleanType();
		}
		
		public String getValueInSQL(DBContext ctx, Object value){
        	Boolean v = (Boolean)value;
        	return ctx.getDBType().getValueInSQL(v.booleanValue());
        }
	}
	
	public static class DATE extends PrimitiveColumn{
		public DATE(String name) {super(name, false, false);}
		public DATE(String name, boolean isPK) {super(name, isPK, false);}
		public DATE(String name, String columnName, boolean isPK) {super(name, columnName, isPK, false);}
		
		public Class<?> getValueType(){
			return Date.class;
		}
		
		public Object valueOf(Object value){
			if(value == null) return null;
			if(value instanceof Date){
				return value;
			}else if(value instanceof java.sql.Date){
				return new Date(((java.sql.Date)value).getTime());
			}else if(value instanceof Long){
				return new Date((Long)value);
			}else{
				throw new RuntimeException("Cannot cast "+value+" to Date");
			}
		}
		
		public String getTypeInSQL(DBContext ctx){
			return ctx.getDBType().getDateType();
		}
		
		public String getValueInSQL(DBContext ctx, Object value){
        	Date v = (Date)value;
        	return ctx.getDBType().getValueInSQL(v);
        }
	}
	
	@SuppressWarnings("rawtypes")
	public static class ENUM<T extends Enum> extends PrimitiveColumn{
		private Class<T> clazz;

		public ENUM(Class<T> clazz, String name) {
			super(name, name, false, false);
			this.clazz = clazz;
		}
		
		public Class<?> getValueType(){
			return clazz;
		}
		
		@SuppressWarnings("unchecked")
		public Object valueOf(Object value){
			if(value == null) return null;
			if(value instanceof String){
				return Enum.valueOf((Class<T>) clazz, (String)value);
			}else if(clazz.equals(value.getClass())){
				return value;
			}else{
				throw new RuntimeException("Cannot cast "+value+" to " + clazz.getName());
			}
		}
		
		protected boolean setValueFromResultSet(Object obj, ResultSet rs) throws SQLException{
			Object value = rs.getObject(this.getColumnName());
			if(value != null){
				try {
					Object enumValue = valueOf(value);
					FieldUtils.writeField(obj, getFieldName(), enumValue, true);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return true;
			}else{
				return false;
			}
		}
		
		public String getTypeInSQL(DBContext ctx){
			return ctx.getDBType().getStringType(30); 
		}
		
		public String getValueInSQL(DBContext ctx, Object value){
			if(value == null){
				return "null";
			}else{
				return "'" + value.toString() + "'";
			}
        }
	}
	
}
