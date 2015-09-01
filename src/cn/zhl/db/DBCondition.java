package cn.zhl.db;

import java.util.List;

public class DBCondition {
	
	public static enum Oper{
    	Equal("="),
    	NotEqual("!="),
    	Greater(">"),
    	GreaterEqual(">="),
    	Less("<"),
    	LessEqual("<="),
    	Like(" like "),
    	NotLike(" not like ");
    	
    	private String oper;
    	private Oper(String oper){
    		this.oper = oper;
    	};
    	
    	public String getOperator(){
    		return oper;
    	}
    }
	
    private DBColumn[] columns;
    private Oper oper;
    private Object[] values;
    
    public DBCondition(){}
    public DBCondition(DBColumn column, Object value){
		this(column, value, Oper.Equal);
	}
    public DBCondition(DBColumn column, Object value, Oper oper){
		this(new DBColumn[]{column}, new Object[]{value});
		this.oper = oper;
	}
    
    public DBCondition(DBColumn[] columns, Object[] values){
        this.columns = columns;
        this.values = values;
        this.oper = Oper.Equal;
        
        for(int i = 0; i < columns.length; i++){
        	values[i] = columns[i].valueOf(values[i]);
        }
    }

    public DBCondition(List<DBColumn> cols, Object obj){
    	this.columns = cols.toArray(new DBColumn[cols.size()]);
		
		Object[] values = new Object[this.columns.length];
		for(int i = 0; i < this.columns.length; i++){
			values[i] = this.columns[i].getValue(obj);
		}
		this.values = values;
		this.oper = Oper.Equal;
	}

	public DBCondition(DBColumn[] columns, Object obj){
		this.columns = columns;
        
		Object[] values = new Object[columns.length];
		for(int i = 0; i < columns.length; i++){
			values[i] = columns[i].getValue(obj);
		}
		this.values = values;
		this.oper = Oper.Equal;
	}
	
	protected String getConditionType(){
		return "and";
	}
	
	protected int getElementCount(){
		return 1;
	}
    
    public void appendWhereCondition(DBContext ctx, StringBuilder buffer){
		for(int i = 0; i < columns.length; i++){
			DBColumn column = columns[i];
    		Object value = values[i];
    		column.appendWhereCondition(ctx, buffer, value, oper);

			if(i < columns.length-1){
				buffer.append(" and ");
			}
		}
    }
    
    public void appendSetCondition(DBContext ctx, StringBuilder buffer){
    	for(int i = 0; i < columns.length; i++){
    		DBColumn column = columns[i];
    		Object value = values[i];
    		column.appendSetCondition(ctx, buffer, value, oper);
    		
			if(i < columns.length-1){
				buffer.append(",");
			}
		}
    }
    
    public DBCondition and(DBCondition... conditions){
    	DBCondition[] arr = new DBCondition[conditions.length+1];
    	arr[0] = this;
    	System.arraycopy(conditions, 0, arr, 1, conditions.length);
    	return new DBCondition.AND(arr);
    }
    
    public DBCondition or(DBCondition... conditions){
    	DBCondition[] arr = new DBCondition[conditions.length+1];
    	arr[0] = this;
    	System.arraycopy(conditions, 0, arr, 1, conditions.length);
    	return new DBCondition.OR(arr);
    }
    
    private static class CompositeCondition extends DBCondition{
    	private DBCondition[] conditions;
    	private String symbol;
    	
    	public CompositeCondition(DBCondition[] conditions, String symbol){
    		this.conditions = conditions;
    		this.symbol = symbol;
    	}
    	
    	protected String getConditionType(){
    		return symbol;
    	}
    	
    	protected int getElementCount(){
    		return conditions.length;
    	}
    	
    	public void appendWhereCondition(DBContext ctx, StringBuilder buffer){
    		StringBuilder tmpBuf = new StringBuilder(); 		
    		for(int i = 0; i < conditions.length; i++){
    			if(!symbol.equals(conditions[i].getConditionType()) && conditions[i].getElementCount() > 1){
    				tmpBuf.append("(");
    	        	conditions[i].appendWhereCondition(ctx, tmpBuf);
    	        	tmpBuf.append(")");
    			}else{
	        		conditions[i].appendWhereCondition(ctx, tmpBuf);
    			}
        		
        		buffer.append(tmpBuf.toString());
        		tmpBuf.setLength(0);
        		
        		if(i < conditions.length-1){
    				buffer.append(" ").append(symbol).append(" ");
    			}
    		}
    	}
    	
    	public void appendSetCondition(DBContext ctx, StringBuilder buffer){
    		throw new RuntimeException("Invalid operation");
    	}
    }
    
    public static class AND extends CompositeCondition{
    	public AND(DBCondition[] conditions){
    		super(conditions, "and");
    	}
    }
    
    public static class OR extends CompositeCondition{
    	public OR(DBCondition[] conditions){
    		super(conditions, "or");
    	}
    }
 
}
