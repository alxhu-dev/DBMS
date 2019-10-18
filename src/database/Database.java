package database;

import sql.SQL;
import table.Table;

import java.io.FileNotFoundException;
import java.util.HashMap;

public class Database {
  
  private String root;
  private HashMap<String, Table> tables;
  
  public String getRoot(){
    return root;
  }
  
  public Database(String path){
    root = path + "/";
    tables = new HashMap<>();
  }
  
  public Table getTable(String name) throws DatabaseException {
    if(tables.containsKey(name)){
      return tables.get(name);
    }
    try {
      Table table = Table.Static.load(root + name);
      tables.put(name, table);
      return table;
    } catch (FileNotFoundException e) {
      throw new DatabaseException("Cannot load Table \"" + name + "\"");
    }
  }
  
  public Iterable<String> getAllTableNames(){
    return tables.keySet();
  }
  
  //TODO make sql and some way of returning a map
  //TODO make DatabaseConnection class and table.query()
  public SQL query(String query){
    throw new UnsupportedOperationException();
  }
}
