package cn.zhl.db;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class DBObjectProxy<T> implements MethodInterceptor {
	private Object proxyParent;
	private String fieldName;
	private T bean;
	private List<Method> pkMethods;
	
	private boolean loaded = false;
	private T realBean;
	
	public DBObjectProxy(DBColumn[] pkColumns, Object proxyParent, T bean, String fieldName){
		this.proxyParent = proxyParent;
		this.bean = bean;
		this.fieldName = fieldName;
		this.pkMethods = getPKMethods(pkColumns, bean.getClass());
	}
	
	private List<Method> getPKMethods(DBColumn[] columns, Class<?> beanClass){
		List<Method> methods = new ArrayList<Method>();
		for(DBColumn col : columns){
			Method getMethod = col.getMethod(beanClass);
			if(getMethod != null){
				methods.add(getMethod);
			}
			Method setMethod = col.setMethod(beanClass);
			if(setMethod != null){
				methods.add(setMethod);
			}
		}
		return methods;
	}

	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		if(!loaded){
			boolean pkMethod = false;
			for(Method lazyMethod : pkMethods){
				if(lazyMethod.equals(method)){
					pkMethod = true;
					break;
				}
			}
			
			if(pkMethod){
				return method.invoke(bean, args);
			}else{	
				@SuppressWarnings("unchecked")
				Class<T> beanClass = (Class<T>)bean.getClass();
				realBean = DBContext.getDAO(beanClass).queryByPK(DBContext.getContext(), bean);
				
				Field field = proxyParent.getClass().getDeclaredField(fieldName);
				field.setAccessible(true);     
		        field.set(proxyParent, realBean);
		        
				loaded = true;
			}
		}
		
		return method.invoke(realBean, args);
	}

}
