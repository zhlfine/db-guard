package cn.zhl.db;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class DBListProxy<T> implements MethodInterceptor {
	private Object proxyParent;
	private Class<T> beanClass;
	private String fieldName;
	private DBColumn fkColumn;
	
	private boolean loaded = false;
	private List<T> list = null;
	
	public DBListProxy(Object proxyParent, Class<T> beanClass, String fieldName, DBColumn fkColumn){
		this.proxyParent = proxyParent;
		this.beanClass = beanClass;
		this.fieldName = fieldName;
		this.fkColumn = fkColumn;
	}

	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		if(!loaded){
			DBCondition condition = fkColumn.equalsTo(proxyParent);
			list = DBContext.getDAO(beanClass).query(DBContext.getContext(), condition);

			try {
				Field field = obj.getClass().getDeclaredField(fieldName);
				field.setAccessible(true);     
		        field.set(obj, list);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			loaded = true;
		}
		
		return method.invoke(list, args);
	}

}
