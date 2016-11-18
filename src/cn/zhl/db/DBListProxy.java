package cn.zhl.db;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class DBListProxy<T> implements MethodInterceptor {
	private Object proxyParent;
	private Class<T> beanClass;
	private String fieldName;
	private DBCondition condition;
	
	private boolean loaded = false;
	private List<T> list = null;
	
	public DBListProxy(Object proxyParent, Class<T> beanClass, String fieldName, DBCondition condition){
		this.proxyParent = proxyParent;
		this.beanClass = beanClass;
		this.fieldName = fieldName;
		this.condition = condition;
	}

	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		if(!loaded){
			list = DBContext.getDAO(beanClass).query(DBContext.getContext(), condition);
			FieldUtils.writeField(proxyParent, fieldName, list, true);
			loaded = true;
		}
		return method.invoke(list, args);
	}

}
