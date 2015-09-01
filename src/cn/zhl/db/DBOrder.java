package cn.zhl.db;

public abstract class DBOrder {

	public DBColumn.PrimitiveColumn column;
	public String order;
	
	public DBColumn.PrimitiveColumn getDBColumn(){
		return column;
	}
	
	public String getOrder(){
		return order;
	}
	
	public DBOrder(DBColumn.PrimitiveColumn column, String order){
		this.column = column;
		this.order = order;
	}
	
	public static class Asc extends DBOrder{
		public Asc(DBColumn.PrimitiveColumn column){
			super(column, "asc");
		}
	}
	
	public static class Desc extends DBOrder{
		public Desc(DBColumn.PrimitiveColumn column){
			super(column, "desc");
		}
	}
}
