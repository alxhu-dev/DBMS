package util;

import table.Table;
import table.Type;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class Message {

  public static Table messageToTable(String pathMessages, String name){
    try {
      LinkedHashMap<String, Type> fieldCat = new LinkedHashMap<>();
      fieldCat.put("date", new Type("varchar(8)"));
      fieldCat.put("time", new Type("varchar(5)"));
      fieldCat.put("sender", new Type("varchar(30)"));
      fieldCat.put("message", new Type("varchar(1023)"));
      HashMap<String, Object> settings = new HashMap<>();
      settings.put("compressed", true);
      Table table = new Table(name, new LinkedList<>(), fieldCat, settings);
  
      String[] messages = Functions.File.Static.loadStrings(pathMessages);
      List<Table.Entry> content = new LinkedList<>();
      for (String s : messages) {
        Functions.Message message = new Functions.Message(s);
        String newEntry = message.getDate() + ";" + message.getTime() + ";";
        if (message.getType() == Functions.Message.Static.TYPE.NULL) {
          throw new IllegalArgumentException("Cannot get Type of Message \"" + s + "\"");
        }
        newEntry += message.getSender() + ";";
        String messageString = message.getContent();
        messageString = Functions.String.replace(messageString, ';', (char) 6);
        newEntry += messageString;
        table.addEntry(newEntry);
      }
      return table;
    }
    catch(Exception e){
      System.err.println("Error in file " + name);
      e.printStackTrace();
      throw new RuntimeException();
    }
  }
  public static void messageToTable(String pathMessages, String pathTable, Void v){
    Table.Static.save(messageToTable(pathMessages, pathTable));
  }
  
  public static void allMessagesToTables(){
    java.util.List<java.io.File> files = Functions.File.Static.getAllFiles("../_Files/chat/");
    for (java.io.File file : files) {
      new Functions.File(file.toString()).load().clean().setPath("../_Files/temp.txt").save();
      Message.messageToTable("../_Files/temp.txt", "chat/" + file.toString().substring(15), null);
    }
  }
}
