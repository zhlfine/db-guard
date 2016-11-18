package cn.zhl.db;

public abstract class DBOrder {

	public DBColumn column;
	public String order;
	
	public DBColumn getDBColumn(){
		return column;
	}
	
	public String getOrder(){
		return order;
	}
	
	public DBOrder(DBColumn column, String order){
		this.column = column;
		this.order = order;
	}
	
	public static class Asc extends DBOrder{
		public Asc(DBColumn column){
			super(column, "asc");
		}
	}
	
	public static class Desc extends DBOrder{
		public Desc(DBColumn column){
			super(column, "desc");
		}
	}
}
