package main;

import Functions.Performance;

public class Main {

  @SuppressWarnings("RedundantThrows")
  public static void main(String[] args) throws Exception {
    Performance timer = new Performance().start();
    Performance.staticStart();
    
    //Convert Message File to SQL File
    //util.Message.messageToTable("../_Files/chat/mcgroupface/comp.txt", "mcgroupface", null);
    
    
  
  
    //Functions.Console.println(new SQL().query("SELECT * FROM mcgroupface WHERE sender COIC KEVIN AND sender NEIC KEVIN AND date CO 07.18").size());
    //Functions.Console.println(new SQL().query("SELECT * FROM mcgroupface WHERE sender COIC KEVIN AND sender NEIC KEVIN"));
    //Functions.Console.println(new SQL().query("SELECT * FROM * WHERE date EQ 01.01.2019 AND time EQ 00:00"));
    //Functions.Console.println(new SQL().query("SELECT * FROM *"));
    //new SQL().query("SELECT * FROM *").setName("tempAllFile").save();
    //Functions.Console.println(new SQL().query("SELECT sender FROM allFilesUniqueSorted.txt GROUP BY sender"));
    //Table result = new SQL().query("SELECT sender date FROM chat/mcgroupface/comp.txt GROUP BY sender ORDER BY sum DESC LIMIT 5");
    //Table result = new SQL().query("SELECT time FROM allFilesUniqueSorted.txt GROUP BY time ORDER BY sum ASC LIMIT 5");
    //Table result = new SQL().query("INSERT INTO sap/mara VALUES 6 \"Test Alkohol\"");
    //Table result = new SQL().query("DELETE FROM sap/mara WHERE matnr EQ 6");
    //Table result = new SQL().query("SELECT * FROM inventar ORDER BY index DESC");
//    Table result = new SQL().query("SELECT sender message FROM chat/chillbest/comp.txt WHERE message coic love");
//    result.query("group by sender order by sum desc LIMIT 10");
    
    
    
    
//    Functions.Console.println(result);
    
//    Table table = Table.empty();
//    table.setFields(new LinkedList<>(){{
//      add("matnr");
//      add("makt");
//    }});
//    table.loadIn("sap/mara");
    
    //Table.Static.load("chat/chillbest/a.txt").addTable("chat/chillbest/b.txt").addTable("chat/chillbest/c.txt").setName("chat/chillbest/comp.txt").save();
    //Message.allMessagesToTables();
    
//    Table.Static.load("chat/chillbest/comp.txt").addTableThreaded("chat/chillbest/comp.txt");
//    boolean a = true;
//    Map<String, Object> hash = new HashMap<>();
//    hash.put("test", "test");
//    for(int i = 0 ; i < 300000 ; i ++){
//      if(hash.containsKey("test") && hash.get("test").equals("false")){
//        int b = 5 * 5;
//      }
////      if(a){
////        int b = 5 * 5;
////      }
//    }
    
    //Functions.Console.println(Table.Static.getName("Root/Folder/file.txt"));
    
    
    Functions.Console.println("Total Execution time: " + timer.stop());
  }
}
