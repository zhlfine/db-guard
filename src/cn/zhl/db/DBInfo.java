package cn.zhl.db;

public class DBInfo {

	private String driver;
	private String url;
	private String username;
	private String password;
	private int poolSize;

	public DBInfo(String driver, String url, String username, String password, int poolSize) {
		this.driver = driver;
		this.url = url;
		this.username = username;
		this.password = password;
		this.poolSize = poolSize;
	}

	public String getDriver() {
		return driver;
	}

	public String getUrl() {
		return url;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public int getPoolSize() {
		return poolSize;
	}

}
