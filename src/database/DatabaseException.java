package database;

public class DatabaseException extends RuntimeException{
  
  public DatabaseException(){
    super();
  }
  public DatabaseException(String x){
    super(x);
  }
}
