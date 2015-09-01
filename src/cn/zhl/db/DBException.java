package cn.zhl.db;

public class DBException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public DBException(String message) {
    super(message);
  }

  public DBException(Throwable throwable) {
    super(throwable);
  }
  
  public DBException(String message, Throwable throwable) {
      super(message, throwable);
    }
  
}
