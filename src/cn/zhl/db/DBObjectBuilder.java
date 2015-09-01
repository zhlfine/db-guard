package cn.zhl.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface DBObjectBuilder<T> {
	
	T buildFromResultSet(ResultSet rs) throws SQLException;
	
}
