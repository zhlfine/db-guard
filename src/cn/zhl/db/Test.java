package cn.zhl.db;

import java.io.File;

public class Test {
	
	public static void main(String[] args){
		DBContext ctx = new DBContext();
		ctx.registerDBInfo(new DBInfo("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres", 3));
		
		DBContext.registerDBObject(Menu.class);
		ctx.generateSchema(new File("test.sql"));
		
		ServiceBase svc = (ServiceBase)ctx.getTxService(new ServiceBaseImpl());
		Menu menu = svc.queryUnique(Menu.class, Menu.DB.COLUMN_ID.equalsTo("1"));
		
		menu.descr = "hello";
		svc.update(menu);
	}
	
	public static class Menu{
		private String id;
		private String name;
		private Menu parent;
		private String descr;
		
		public static class DB<T extends Menu> extends GenericDAO<T>{
			public static DBColumn COLUMN_ID       = new DBColumn.STRING("id", 10, true);
			public static DBColumn COLUMN_NAME     = new DBColumn.STRING("name", 20);
			public static DBColumn COLUMN_PARENT   = new DBColumn.OBJECT<Menu>(Menu.class, "parent");
			public static DBColumn COLUMN_DESCR    = new DBColumn.STRING("descr", 64);
		}

		public String getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Menu getParent() {
			return parent;
		}

		public String getDescr() {
			return descr;
		}
		
	}
}
