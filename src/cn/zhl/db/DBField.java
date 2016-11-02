package cn.zhl.db;

public class DBField {

	private DBColumn column;
	private Object value;
	
	public DBField(DBColumn column, Object value){
		this.column = column;
		this.value = value;
	}

	public DBColumn getColumn() {
		return column;
	}

	public void setColumn(DBColumn column) {
		this.column = column;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}
	
	
}
