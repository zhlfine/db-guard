package cn.zhl.db;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class DBTxHandler implements InvocationHandler {
	private Object service;
	private DBContext ctx;
	
	public DBTxHandler(Object service, DBContext ctx){
		this.service = service;
		this.ctx = ctx;
	}

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Object result = null;
		ctx.startTransaction();
		try{
			result = method.invoke(service, args);
			ctx.commitTransaction();
		}catch(Throwable e){
			ctx.rollbackTransaction();
			throw e;
		}
		return result;
	}

}
