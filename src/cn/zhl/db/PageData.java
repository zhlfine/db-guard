package cn.zhl.db;

import java.util.ArrayList;
import java.util.List;

public class PageData<T> {

	private int index;
	private int limit;
	private int total;
	private List<T> data = new ArrayList<T>();
	
	public PageData(int index, int limit, int total, List<T> data){
		this.index = index;
		this.limit = limit;
		this.total = total;
		this.data = data;
	}
	
	public int getBegin(){
		return (index - 1) * limit + 1;
	}
	
	public int getEnd(){
		return getBegin() + getSize() - 1;
	}
	
	public int getSize(){
		return data.size();
	}

	public int getIndex() {
		return index;
	}

	public int getLimit() {
		return limit;
	}

	public int getTotal() {
		return total;
	}

	public List<T> getData() {
		return data;
	}
	
}
