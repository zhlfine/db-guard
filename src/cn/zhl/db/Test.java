package cn.zhl.db;

import cn.zhl.db.annotation.DAO;

public class Test {
	
	public static void main(String[] args){
		DBContext ctx = new DBContext();
		ctx.registerDBInfo(new DBInfo("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres", 3));
		
//		DBContext.registerDBObject(Sample.class);
//		ctx.generateSchema(new File("test.sql"));
	
		ServiceBase svc = (ServiceBase)ctx.getTxService(new ServiceBaseImpl());
		Sample sample = svc.queryUnique(Sample.class, Sample.DB.COLUMN_ID.equalsTo("1"));

//		ctx.startTransaction();
//		try{
//			Sample sample = DBContext.getDAO(Sample.class).queryUnique(ctx, Sample.DB.COLUMN_ID.equalsTo("1"));
//			
//			ctx.commitTransaction();
//		}catch(Exception e){
//			ctx.rollbackTransaction();
//		}
	}
	
	@DAO(SampleDAO.class)
	public static class Sample {
		private String id;
		private String name;
		
		public static class DB<T extends Sample> extends GenericDAO<T>{
			public static DBColumn COLUMN_ID       = new DBColumn.STRING("id", 10, true);
			public static DBColumn COLUMN_NAME     = new DBColumn.STRING("name", 20);
		}
	}
	
	public static class SampleDAO extends GenericDAO<Sample>{
		public static DBColumn COLUMN_ID       = new DBColumn.STRING("id", 10, true);
		public static DBColumn COLUMN_NAME     = new DBColumn.STRING("name", 20);
	}
}
