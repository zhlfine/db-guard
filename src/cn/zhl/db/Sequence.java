package cn.zhl.db;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class Sequence {
	private static Sequence instance = new Sequence();
	
	private Sequence(){}
	public static Sequence getInstance(){
		return instance;
	}
	
	private Map<String, Map<String, Integer>> seqMap = new HashMap<String, Map<String, Integer>>();

	public synchronized int next(DBContext ctx, String table, DBColumn column){
		Map<String, Integer> map = seqMap.get(table);
		if(map == null){
			map = new HashMap<String, Integer>();
			seqMap.put(table, map);
		}
		
		Integer seq = map.get(column.getFieldName());
		if(seq == null){
			seq = getMax(ctx, table, column);
		}
		
		seq += 1;
		map.put(column.getFieldName(), seq);
		
		return seq;
	}
	
	private Integer getMax(DBContext ctx, String table, DBColumn column){
		StringBuilder buffer = new StringBuilder();
		buffer.append("select max(");
		column.appendColumnName(buffer, ",");
		buffer.append(") from ").append(table);
		
		String sql = buffer.toString();
		try {
			Number num = (Number)DBHelper.selectValue(ctx, sql);
			if(num == null){
				return 0;
			}else{
				return num.intValue();
			}
		} catch (SQLException e) {
			throw new DBException(e);
		}
	}

}
