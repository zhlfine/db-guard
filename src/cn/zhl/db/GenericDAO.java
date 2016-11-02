package cn.zhl.db;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.zhl.db.annotation.Table;

public abstract class GenericDAO<T> implements DBObjectBuilder<T> {
	static Logger logger = LoggerFactory.getLogger(GenericDAO.class);
	
	private Class<T> entityClass;
	private DBColumn[] columns;
	private DBColumn.LIST<?>[] listColumns;
	
	public GenericDAO(){
		this.entityClass = getEntityClass(getClass());
		initColumnDefs();
	}
	
	@SuppressWarnings("unchecked")
	protected static <E> Class<E> getEntityClass(Class<?> daoClass){
		Class<E> entityClass = null;
		TypeVariable<?>[] types = daoClass.getTypeParameters();
		if(types.length > 0){
			Type[] bounds = types[0].getBounds();
			if(bounds.length > 0){
				if(bounds[0] instanceof ParameterizedType){
					entityClass =  (Class<E>)((ParameterizedType)bounds[0]).getRawType();
				}else{
					entityClass =  (Class<E>)bounds[0]; 
				}
			}
		}
		if(entityClass == null){
			Type type = daoClass.getGenericSuperclass();
			Type[] args = ((ParameterizedType)type).getActualTypeArguments();   
			entityClass =  (Class<E>)args[0]; 
		}
		return entityClass;
	}
	
	private void initColumnDefs(){
		try {
			List<DBColumn> cols = new ArrayList<DBColumn>();
			List<DBColumn.LIST<?>> listCols = new ArrayList<DBColumn.LIST<?>>();
			Field[] fields = getClass().getFields();
			for(Field field : fields){	
				if(DBColumn.class.isAssignableFrom(field.getType())){
					Object obj = field.get(null);
					if(obj != null){
						if(obj instanceof DBColumn.LIST){
							listCols.add((DBColumn.LIST<?>)obj);
						}else{
							cols.add((DBColumn)obj);
						}
					}
				}
			}
			columns = cols.toArray(new DBColumn[cols.size()]);
			listColumns = listCols.toArray(new DBColumn.LIST<?>[listCols.size()]);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public Class<T> getEnitityClass(){
		return entityClass;
	}
	
	public String getTableName(){
		Table table = this.getClass().getAnnotation(Table.class);
		if(table != null){
			return table.value();
		}else{
			return entityClass.getSimpleName().toLowerCase();
		}
	}
	
	public DBColumn[] getDBColumns(){
		return columns;
	}
	
	public DBColumn getPKColumn(){
		DBColumn pk = null;
		for(DBColumn col : getDBColumns()){
      		if(col.isPK()){
      			if(pk == null){
      				pk = col;
      			}else{
      				throw new RuntimeException("Composite primary key is not supported: " + this.getClass().getName());
      			}
			}
		}
		if(pk == null){
			throw new RuntimeException("primary key is not defined in " + this.getClass().getName());
		}
		
		return pk;
	}
	
	public DBColumn.LIST<?>[] getListColumns(){
		return listColumns;
	}
	
	public DBColumn.OBJECT<?>[] getFKColumns(){
		List<DBColumn.OBJECT<?>> fks = new ArrayList<DBColumn.OBJECT<?>>();
		for(DBColumn col : getDBColumns()){
      		if(col instanceof DBColumn.OBJECT<?>){
      			fks.add((DBColumn.OBJECT<?>)col);
			}
		}
		return fks.toArray(new DBColumn.OBJECT<?>[fks.size()]);
	}
	
	protected T newEntityInstance(){
		try {
			return entityClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public T cloneEntity(T bean){
		T clone = newEntityInstance();
		for(DBColumn column : columns){
			column.copyValue(bean, clone);
		}
		for(DBColumn.LIST<?> listColumn : listColumns){
			listColumn.copyValue(bean, clone);
		}
		return clone;
	}
	
	public boolean isTableExisting(DBContext ctx){
		String tableName = getTableName();
		try {
			List<String> tableNames = DBHelper.getTableNames(ctx, getTableName());
			int i = 0;
			for(String name : tableNames){
				if(name.equalsIgnoreCase(tableName)){
					i++;
				}
			}
			if(i == 0){
				return false;
			}else if(i == 1){
				return true;
			}else{
				throw new DBException("Multiple tables match the name "+tableName);
			}
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}
	
	public void createTable(DBContext ctx){
		try {
			String ddl = getDDL(ctx);
			DBHelper.executeUpdate(ctx, ddl);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}
	
	public void dropTable(DBContext ctx){
		String sql = "drop table "+getTableName();
		try {
			DBHelper.executeUpdate(ctx, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}
	
	public List<GenericDAO<?>> getDependentDAOes(){
		List<GenericDAO<?>> list = new ArrayList<GenericDAO<?>>();
		for(DBColumn.OBJECT<?> fk : getFKColumns()){
			Class<?> beanClass = fk.getReferedClass();
			GenericDAO<?> dao = DBContext.getDAO(beanClass);
			if(dao != this){
				list.add(dao);
			}
		}
		return list;
	}
	
	public String getDDL(DBContext ctx){
		StringBuilder buf = new StringBuilder();
		buf.append("create table ").append(getTableName()).append("(\r\n");
		for(int i = 0; i < getDBColumns().length; i++){
			getDBColumns()[i].appendDDL(ctx, buf);

			if(i < getDBColumns().length-1){
				buf.append(",");
			}else{
				DBColumn pk = getPKColumn();
				buf.append(",\r\n");
				buf.append("    primary key(");
				pk.appendColumnName(buf, ",");
				buf.append(")");

				DBColumn.OBJECT<?>[] fks = getFKColumns();
				if(fks.length > 0){
					buf.append(",\r\n");
					for(int j = 0; j < fks.length; j++){
						buf.append("    foreign key(");
						fks[j].appendColumnName(buf, ",");
						buf.append(") references ");
						buf.append(fks[j].getReferedTableName());
						buf.append("(");
						
						DBColumn col = fks[j].getReferedColumn();
						col.appendColumnName(buf, ",");
						
						buf.append(")");
						
						if(j < fks.length-1){
							buf.append(",\r\n");
						}
					}
				}
			}
			buf.append("\r\n");
		}
		buf.append(");");
		return buf.toString();
	}
	
	public void validate(T bean) throws DBException{
		for(DBColumn column : getDBColumns()){
			column.validate(bean);
		}
	}
	
	public T buildFromResultSet(ResultSet rs) throws SQLException {
		T bean = newEntityInstance();
		for(DBColumn column : getDBColumns()){
			column.setValueFromResultSet(bean, rs);
		}
		for(DBColumn.LIST<?> listColumn : getListColumns()){
			listColumn.setListValue(bean);
		}
		return bean;
	}
	
	public int insert(DBContext ctx, T bean) throws DBException{
		validate(bean);
		
		DBColumn[] columns = getDBColumns();
		StringBuilder buffer = new StringBuilder();
		buffer.append("insert into ").append(getTableName()).append("(");
		for(int i = 0; i < columns.length; i++){
			columns[i].appendColumnName(buffer, ",");
			if(i < columns.length-1){
				buffer.append(",");
			}
		}
		buffer.append(") values(");
		for(int i = 0; i < columns.length; i++){
			Object value = columns[i].getValue(bean);
			if(columns[i].useSequence()){
				int seq = Sequence.getInstance().next(ctx, getTableName(), columns[i]);
				value = columns[i].valueOf(new Integer(seq));
				columns[i].setValue(bean, value);
			}
			columns[i].appendColumnValue(ctx, buffer, value, ",");

			if(i < columns.length-1){
				buffer.append(",");
			}
		}
		buffer.append(")");

		String sql = buffer.toString();
		try {
			return DBHelper.executeUpdate(ctx, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}
	
	public int update(DBContext ctx, T bean) throws DBException{
		validate(bean);
		
		List<DBColumn> pks = new ArrayList<DBColumn>();
		List<DBColumn> columns = new ArrayList<DBColumn>();
		for(DBColumn column : getDBColumns()){
      		if(column.isPK()){
				pks.add(column);
			}else{
				columns.add(column);
			}
		}
		
		if(pks.size() == 0){
			throw new DBException("No Primary Key");
		}

		StringBuilder buffer = new StringBuilder();
      	buffer.append("update ").append(getTableName()).append(" set ");
      	for(int i = 0; i < columns.size(); i++){
      		DBColumn column = columns.get(i);
			Object value = column.getValue(bean);
			column.equalsTo(value).appendSetCondition(ctx, buffer);
			if(i < columns.size()-1){
				buffer.append(",");
			}
		}
      	buffer.append(" where ");
      	for(int i = 0; i < pks.size(); i++){
      		DBColumn column = pks.get(i);
			Object value = column.getValue(bean);
			column.equalsTo(value).appendWhereCondition(ctx, buffer);
			if(i < pks.size()-1){
				buffer.append(" and ");
			}
		}
      	
      	String sql = buffer.toString();
		try {
			return DBHelper.executeUpdate(ctx, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}
	
	public int updateFields(DBContext ctx, DBField pkField, DBField[] fields) throws DBException{
		StringBuilder buffer = new StringBuilder();
      	buffer.append("update ").append(getTableName()).append(" set ");
      	for(int i = 0; i < fields.length; i++){
      		DBField field = fields[i];
			field.getColumn().equalsTo(field.getValue()).appendSetCondition(ctx, buffer);
			if(i < fields.length-1){
				buffer.append(",");
			}
		}
      	buffer.append(" where ");
		pkField.getColumn().equalsTo(pkField.getValue()).appendWhereCondition(ctx, buffer);
      	
      	String sql = buffer.toString();
		try {
			return DBHelper.executeUpdate(ctx, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}
	
	public List<T> query(DBContext ctx){
		return query(ctx, null, null, 0, 0);
	}
	
	public List<T> query(DBContext ctx, DBOrder order, int limit){
		return query(ctx, null, order, limit, 0);
	}
	
	public List<T> query(DBContext ctx, DBOrder order, int limit, int offset){
		return query(ctx, null, null, limit, offset);
	}
	
	public List<T> query(DBContext ctx, DBCondition condition){
		return query(ctx, condition, null, 0, 0);
	}
	
	public List<T> query(DBContext ctx, DBCondition condition, DBOrder order){
		return query(ctx, condition, order, 0, 0);
	}
	
	public List<T> query(DBContext ctx, DBCondition condition, DBOrder order, int limit){
		return query(ctx, condition, order, limit, 0);
	}
	
	public List<T> query(DBContext ctx, DBCondition condition, DBOrder order, int limit, int offset){
		DBColumn[] columns = getDBColumns();
		
		StringBuilder buf = new StringBuilder();
		buf.append("select ");
		for(int i = 0; i < columns.length; i++){
			columns[i].appendColumnName(buf, ",");
			if(i < columns.length-1){
				buf.append(",");
			}
		}
		buf.append(" from ").append(getTableName());
		
		if(condition != null){
			buf.append(" where ");
			condition.appendWhereCondition(ctx, buf);
		}
		
		if(order != null){
			order.getDBColumn().appendOrder(buf, order);
		}

		String sql = buf.toString();
		if(limit > 0){
			sql = ctx.getDBType().limit(sql, limit, offset);
		}
		
		try {
			return DBHelper.select(ctx, this, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}
	
	public T queryUnique(DBContext ctx, DBCondition condition){
		List<T> list = query(ctx, condition);
		if (list.size() == 0) {
			return null;
		}else if (list.size() == 1) {
			return list.get(0);
		}else{
			throw new DBException("there are more than one rows in the result");
		}
	}

	
	public T queryByPK(DBContext ctx, T bean){
		DBColumn pk = this.getPKColumn();	
		if(pk == null){
			throw new DBException("No Primary Key");
		}
		Object pkValue = pk.getValue(bean);
		DBCondition condition = new DBCondition(pk, pkValue);		
		return queryUnique(ctx, condition);
	}
	
	public int delete(DBContext ctx){
		return delete(ctx, null);
	}
	
	public int deleteByPK(DBContext ctx, T bean){
		DBColumn pk = this.getPKColumn();	
		if(pk == null){
			throw new DBException("No Primary Key");
		}
		Object pkValue = pk.getValue(bean);
		DBCondition condition = new DBCondition(pk, pkValue);		
		return delete(ctx, condition);
	}
	
	public int delete(DBContext ctx, DBCondition condition){
		StringBuilder buffer = new StringBuilder();
		buffer.append("delete from ").append(getTableName());
		
		if(condition != null){
			buffer.append(" where ");
			condition.appendWhereCondition(ctx, buffer);
		}
		
		String sql = buffer.toString();
		try {
			return DBHelper.executeUpdate(ctx, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}
	
	public int count(DBContext ctx){
		return count(ctx, null);
	}
	
	public int count(DBContext ctx, DBCondition condition){
		StringBuilder buffer = new StringBuilder();
        buffer.append("select count(*) from ").append(getTableName());
        
        if(condition != null){
			buffer.append(" where ");
			condition.appendWhereCondition(ctx, buffer);
		}
        
        String sql = buffer.toString();
        try {
			return DBHelper.count(ctx, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}

	public List<T> nativeQuery(DBContext ctx, String sql) {
		try {
			return DBHelper.select(ctx, this, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}

	public int nativeUpdate(DBContext ctx, String sql) {
		try {
			return DBHelper.executeUpdate(ctx, sql);
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}

	public Object nativeQuerySingleValue(DBContext ctx, String sql) {
		try {
			return DBHelper.selectValue(ctx, sql);
		} catch (SQLException e) {
			throw new DBException(sql, e);
		}
	}

}
