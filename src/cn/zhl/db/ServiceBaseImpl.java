package cn.zhl.db;

import java.util.List;

public class ServiceBaseImpl implements ServiceBase {

	public <T> List<T> query(Class<T> beanClass) {
		return DBContext.getDAO(beanClass).query(DBContext.getContext());
	}

	public <T> List<T> query(Class<T> beanClass, DBOrder order, int limit) {
		return DBContext.getDAO(beanClass).query(DBContext.getContext(), order, limit);
	}

	public <T> List<T> query(Class<T> beanClass, DBOrder order, int limit, int offset) {
		return DBContext.getDAO(beanClass).query(DBContext.getContext(), order, limit, offset);
	}

	public <T> List<T> query(Class<T> beanClass, DBCondition condition) {
		return DBContext.getDAO(beanClass).query(DBContext.getContext(), condition);
	}

	public <T> List<T> query(Class<T> beanClass, DBCondition condition, DBOrder order, int limit) {
		return DBContext.getDAO(beanClass).query(DBContext.getContext(), condition, order, limit);
	}

	public <T> List<T> query(Class<T> beanClass, DBCondition condition, DBOrder order, int limit, int offset) {
		return DBContext.getDAO(beanClass).query(DBContext.getContext(), condition, order, limit, offset);
	}

	public <T> T queryUnique(Class<T> beanClass, DBCondition condition) {
		return DBContext.getDAO(beanClass).queryUnique(DBContext.getContext(), condition);
	}

	public <T> T queryByPK(T bean) {
		@SuppressWarnings("unchecked")
		Class<T> beanClass = (Class<T>)bean.getClass();
		return (T)DBContext.getDAO(beanClass).queryByPK(DBContext.getContext(), bean);
	}

	public <T> int delete(Class<T> beanClass) {
		return DBContext.getDAO(beanClass).delete(DBContext.getContext());
	}

	public <T> int delete(Class<T> beanClass, DBCondition condition) {
		return DBContext.getDAO(beanClass).delete(DBContext.getContext(), condition);
	}

	public <T> int delete(T bean) {
		@SuppressWarnings("unchecked")
		Class<T> beanClass = (Class<T>)bean.getClass();
		return DBContext.getDAO(beanClass).deleteByPK(DBContext.getContext(), bean);
	}

	public <T> int update(T bean) {
		@SuppressWarnings("unchecked")
		Class<T> beanClass = (Class<T>)bean.getClass();
		return DBContext.getDAO(beanClass).update(DBContext.getContext(), bean);
	}

	public <T> int insert(T bean) {
		@SuppressWarnings("unchecked")
		Class<T> beanClass = (Class<T>)bean.getClass();
		return DBContext.getDAO(beanClass).insert(DBContext.getContext(), bean);
	}
	
	public <T> int count(Class<T> beanClass){
		return DBContext.getDAO(beanClass).count(DBContext.getContext());
	}
	
	public <T> int count(Class<T> beanClass, DBCondition condition){
		return DBContext.getDAO(beanClass).count(DBContext.getContext(), condition);
	}
	
	public <T> boolean isTableExisting(Class<T> beanClass){
		return DBContext.getDAO(beanClass).isTableExisting(DBContext.getContext());
	}

}
