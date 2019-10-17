package sql;

public class SQLException extends RuntimeException {
  
  public SQLException(){
    super();
  }
  public SQLException(String x){
    super(x);
  }
  public SQLException(String x, Exception cause){
    super(x, cause);
  }
  
}
