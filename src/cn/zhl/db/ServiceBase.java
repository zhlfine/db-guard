package cn.zhl.db;

import java.util.List;

public interface ServiceBase {

	<T> List<T> query(Class<T> beanClass);
	<T> List<T> query(Class<T> beanClass, DBOrder order, int limit);
	<T> List<T> query(Class<T> beanClass, DBOrder order, int limit, int offset);
	<T> List<T> query(Class<T> beanClass, DBCondition condition);  
	<T> List<T> query(Class<T> beanClass, DBCondition condition, DBOrder order, int limit);
	<T> List<T> query(Class<T> beanClass, DBCondition condition, DBOrder order, int limit, int offset);	
	<T> T queryUnique(Class<T> beanClass, DBCondition condition);
	<T> T queryByPK(T bean);
	
	<T> int delete(Class<T> beanClass);
	<T> int delete(Class<T> beanClass, DBCondition condition);
	<T> int delete(T bean);
	
	<T> int update(T bean);
	<T> int insert(T bean);
	
	<T> int count(Class<T> beanClass);
	<T> int count(Class<T> beanClass, DBCondition condition);
	
	<T> boolean isTableExisting(Class<T> beanClass);
	
}
