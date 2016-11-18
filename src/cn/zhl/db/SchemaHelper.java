package cn.zhl.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;

import cn.zhl.db.annotation.DAO;

public class SchemaHelper {

	public static void scanPackage(String beanPackage){
		Reflections reflections = new Reflections(ClasspathHelper.forPackage(beanPackage), new TypeAnnotationsScanner());
		Set<Class<?>> beanClasses = reflections.getTypesAnnotatedWith(DAO.class);
		for(Class<?> beanClazz : beanClasses){
			DBContext.registerDBObject(beanClazz);
		}
	}
	
	public static void writeSchema(File file){
		new DBContext().generateSchema(file);
	}
	
	public static void execute(DBContext ctx, File file){
		BufferedReader reader = null;
		try{
			ctx.startTransaction();
			
			StringBuilder buffer = new StringBuilder();
			reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			while(line != null){
				buffer.append(line);
				
				if(line.trim().endsWith(";")){
					DBHelper.executeUpdate(ctx, buffer.toString());
					buffer.setLength(0);
				}
				
				line = reader.readLine();
			}		
			ctx.commitTransaction();
		}catch(Exception e){
			ctx.rollbackTransaction();
			throw new RuntimeException(e);
		}finally{
			if(reader != null){
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
	public static void initDB(DBContext ctx){
		File file = new File("schema.sql");
		writeSchema(file);
		execute(ctx, file);
	}
	
}
