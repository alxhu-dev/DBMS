package sql;

import Functions.Performance;
import database.Database;
import database.DatabaseException;
import table.Table;
import table.Type;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//TODO Features to add:
// CREATE TABLE table_name (
//    column1 datatype,
//    column2 datatype,
//    column3 datatype,
//   ....
// );
// DROP TABLE tableName
// GROUP TO MAP instead of GROUP BY to save the result in a map, to acces from charts

@SuppressWarnings("unused")
public class SQL {
  final private static boolean printDebug = false;
  private static void performancePrint(String x){
    if(printDebug) {
      Functions.Console.println(x);
    }
  }
  
  private Database parentDB;
  public Table table;

  public SQL(String query, Database parentDB){
    this.parentDB = parentDB;
    this.table = Table.empty();
    //TODO FIX?
    List<String> tokenizedString = Functions.String.tokenize(query, new HashMap<>(){{
      put('"', '"');
      put('\'', '\'');
      put(' ', ' ');
      put('(', ')');
      put(',', ',');
    }});
    List<List<java.lang.String>> tokens = Functions.SQL.tokenize(Functions.Array.toArray(tokenizedString), new ArrayList<>(){{
      Class[] classes = SQL.class.getDeclaredClasses();
      for (Class clazz : classes) {
        if (clazz.getName().split("\\$", 2)[1].toUpperCase().equals(query.split(" ", 2)[0].toUpperCase())) {
          List<Method> methods = new LinkedList<>(Arrays.asList(clazz.getDeclaredMethods()));
          methods.removeIf(method -> method.getName().toUpperCase().endsWith("INIT") || method.getName().toUpperCase().contains("LAMBDA"));
          List<String> methodNames = methods.parallelStream().map(method -> method.getName().split("\\(", 2)[0].split("\\.", 2)[0].toUpperCase()).collect(Collectors.toList());
          addAll(methodNames);
          break;
        }
      }
    }});
    try {
      table = (Table)Arrays.stream(
          Arrays.stream(SQL.class.getDeclaredClasses())
              .parallel().filter(clazz -> clazz.getName().split("\\$", 2)[1].toUpperCase().equals(query.split(" ", 2)[0].toUpperCase())).findFirst().get()
              .getDeclaredMethods()).filter(method -> method.getName().toUpperCase().endsWith("INIT")).findFirst().get().invoke(null, tokens);
    } catch (IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
    }

  }
  public static SQL query(String query, Database parentDB) {
    return new SQL(query, parentDB);
  }
  @Deprecated
  public static Table commitQuery(String query, Database parentDB){
    SQL sql = new SQL(query, parentDB);
    sql.commitWork();
    return sql.table;
  }

  private class Select{
    private boolean hardLimit = false;
    private Table table;
    protected Table init(List<List<String>> args){
      table = Table.empty();
      for(List<String> list: args){
        try {
          Method method = Select.class.getDeclaredMethod(list.get(0).toLowerCase(), LinkedList.class);
          method.invoke(null,  new LinkedList<>(list));
          performancePrint("Meantime " + method.getName() + ": " + Performance.getSubTime());
        } catch(NoSuchMethodException e) {
          throw new SQLException("Function " + list.get(0).toUpperCase() + " nonexistent");
        } catch(IllegalAccessException e){
          e.printStackTrace();
        } catch(InvocationTargetException e) {
          Throwable cause = e.getCause();
          if(cause instanceof DatabaseException){
            throw (DatabaseException)cause;
          }
          e.getCause().printStackTrace();
        }
      }
      commitHooks.add(() -> table.save());
      return table;
    }
    private void select(LinkedList<String> args){
      args.removeFirst();
      switch (args.getFirst().toUpperCase()) {
        case "SINGLE" -> {
          hardLimit = true;
          args.removeFirst();
        }
        case "UNIQUE" -> {
          table.setUnique(true);
          args.removeFirst();
        }
      }
      table.setFields(Functions.List.subList(args, 1, args.size()));
    }
    private void from(LinkedList<String> args){
      args.removeFirst();
      table.setName(args.getFirst());
      if(args.size() != 1){
        table.setName("tables");
      }
      if(args.size() > 3 && args.get(args.size()-2).toUpperCase().equals("AS")){
        table.setName(args.getLast());
        args.removeLast();
        args.removeLast();
      }
      if(args.size() == 1 && args.getFirst().equals("*")){
        args.removeFirst();
        parentDB.getAllTableNames().forEach(args::add);
      }
      args.forEach((arg) -> table.addTableThreaded(parentDB.getTable(arg)));
    }
    private void fromnew(LinkedList<String> args){
      args.removeFirst();
      Map<String, String> asNames = new HashMap<>();
      if(Functions.List.containsIgnoreCase(args, "as")){
        for(int i = 1 ; i < args.size() - 1 ; i ++){
          if(args.get(i).equalsIgnoreCase("as")){
            if(asNames.containsKey(args.get(i+1))){
              throw new SQLException("Cannot have same alias for different tables");
            }
            asNames.put(args.get(i+1),args.get(i-1));
            args.remove(i);
            args.remove(i);
            i -= 2;
          }
        }
      }
      table.setName(args.getFirst());
      table.addTable(args.getFirst());
      args = args.parallelStream().map(element -> {
        if (element.contains("~")) {
          if(asNames.containsKey(Functions.String.splitOnce(element, '~')[0])){
            return asNames.get(Functions.String.splitOnce(element, '~')[0]) + element.substring(element.indexOf('~'));
          }
        }
        return element;
      }).collect(Collectors.toCollection(LinkedList::new));
      if(Functions.List.containsIgnoreCase(args, "join")){
        for(int i = 1 ; i < args.size() - 1 ; i ++){
          if(args.get(i).toLowerCase().equals("join")){
            List<String> conditions = new LinkedList<>();
            if(!args.get(i+2).equalsIgnoreCase("on")){
              throw new SQLException("Conditions for Join needed");
            }
            int nextDex = args.size();
            for(int j = i + 3 ; j < args.size() - 1 ; j ++){
              if(args.get(j).equalsIgnoreCase("join")){
                nextDex = j - 1;
              }
            }
            conditions = Functions.List.subList(args, i + 3, nextDex);
            try {
              join(table, parentDB.getTable(args.get(i + 1)), args.get(i - 1), conditions);
            } catch (DatabaseException e) {
              throw new SQLException("Table \"" + args.getFirst() + "\" cannot be found");
            }
          }
        }
      }
    }
    private void where(LinkedList<String> args){
      try {
        List<String> conditions = Functions.List.subList(args, 1, args.size());
        if(hardLimit){
          Optional<Table.Entry> optionalEntry = table.getContent().parallelStream().filter(entry -> entry.meetsCondition(conditions)).findFirst();
          table.getContent().clear();
          optionalEntry.ifPresent(entry -> table.getContent().add(entry));
        }
        table.getContent().removeIf(entry -> !entry.meetsCondition(conditions));
      } catch(Exception e){
        e.printStackTrace();
      }
    }
    private void limit(LinkedList<String> args){
      if(args.size() != 2){
        throw new RuntimeException("Too many arguments for \"Limit\"");
      }
      table.setContent(Functions.List.subList(table.getContent(), 0, Integer.parseInt(args.get(1))));
    }
    //TODO Use stream grouping
    private void group(LinkedList<String> args){
      args.removeFirst();
      if(args.getFirst().toUpperCase().equals("BY")){
        args.removeFirst();
      }
      if(args.isEmpty()){
        return;
      }
      LinkedHashMap<String, Type> fieldCat = new LinkedHashMap<>();
      args.parallelStream().forEach((token) -> {
        fieldCat.put(token, table.getFieldCat().get(token));
      });
  Performance.printSubTime("init");
      if(fieldCat.containsKey("sum")){
        throw new IllegalArgumentException("Table \"" + table.getName() + "\" has field \"sum\"");
      }
      fieldCat.put("sum", new Type("int(32)"));
      table.getContent().parallelStream().forEach(entry -> entry.newFieldCat(fieldCat));
  Performance.printSubTime("newFieldCat");
      table.setFieldCat(fieldCat);
  Performance.printSubTime("setFieldCat");
      ConcurrentHashMap<Table.Entry, Integer> concurrentGroupedMap = new ConcurrentHashMap<>();
      List<Spliterator<Table.Entry>> spliterators = new LinkedList<>();
      spliterators.add(table.getContent().spliterator());
      IntStream.range(1, Runtime.getRuntime().availableProcessors()).forEach((a) -> {
        spliterators.add(spliterators.get(0).trySplit());
      });
      spliterators.removeIf(Objects::isNull);
  Performance.printSubTime("createSpliterators");
  //TODO Fill different maps in threads and collect them together
      LinkedList<Thread> threads = new LinkedList<>();
      spliterators.forEach((spliterator) ->{
        threads.add(new Thread(new Functions.ConsumerRunnable(){
          public void run(){
            //noinspection unchecked
            ((Spliterator<Table.Entry>) this.object).forEachRemaining((a) -> {
              concurrentGroupedMap.putIfAbsent(a, 0);
              concurrentGroupedMap.compute(a, (key, value) -> Functions.Integer.increment(value));
            });
          }
        }.useObjectThenRun(spliterator)));
        threads.getLast().start();
      });
      threads.parallelStream().forEach((thread) -> {
        try {
          thread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });
  Performance.printSubTime("spliteratorThreads");
      table.getContent().clear();
      concurrentGroupedMap.forEach((key, value) -> {
        key.set("sum", value);
        table.addEntry(key);
      });
    }

    private void order(LinkedList<String> args){
      args.removeFirst();
      if(args.getFirst().toUpperCase().equals("BY")) {
        args.removeFirst();
      }
      final boolean desc = args.getLast().toUpperCase().equals("DESC");
      if (args.getLast().toUpperCase().equals("DESC")
       || args.getLast().toUpperCase().equals("ASC")) {
        args.removeLast();
      }
      if(args.size() == 1 && args.get(0).toUpperCase().equals("LENGTH")){
        table.setContent(table.getContent().stream().sorted(Comparator.comparing(entry -> entry.toString().length())).collect(Collectors.toList()));
      } else {
        args.parallelStream().forEach((field) -> {
          table.setContent(table.getContent().stream().sorted(Comparator.comparing(entry -> entry.get(field).toString().hashCode() * (desc ? -1 : 1))).collect(Collectors.toList()));
        });
      }
    }
  }

  private class Delete{
    private Table table = Table.empty();
    protected Table init(List<List<String>> args){
      table = Table.empty();
      for(List<String> list: args){
        try {
          Method method = Delete.class.getDeclaredMethod(list.get(0).toLowerCase(), LinkedList.class);
          method.invoke(null,  new LinkedList<>(list));
          performancePrint("Meantime " + method.getName() + ": " + Performance.getSubTime());
        } catch(NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
          e.printStackTrace();
        }
      }
      commitHooks.add(() -> table.save());
      return table;
    }

    private void delete(LinkedList<String> args){
      args.removeFirst();
      if(!args.isEmpty()){
        throw new SQLException("Delete command must not have Parameters");
      }
    }

    private void from(LinkedList<String> list){
      if(list.size() != 2){
        throw new SQLException("Only one Table identifier allowed in DELETE statement");
      }
      list.removeFirst();
      table = SQL.query("SELECT * FROM " + list.get(0), parentDB).table;
    }

    private void where(LinkedList<String> list){
      list.removeFirst();
      table.getContent().removeIf(element -> {
        return element.meetsCondition(list);
      });
    }
  }

  private class Insert{
    private Table table = Table.empty();
    private List<String> fieldList = new LinkedList<>();
    protected Table init(List<List<String>> args){
      table = Table.empty();
      for(List<String> list: args){
        try {
          Method method = Insert.class.getDeclaredMethod(list.get(0).toLowerCase(), LinkedList.class);
          method.invoke(null,  new LinkedList<>(list));
          performancePrint("Meantime " + method.getName() + ": " + Performance.getSubTime());
        } catch(NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
          e.printStackTrace();
        }
      }
      commitHooks.add(() -> table.save());
      return table;
    }

    private void insert(LinkedList<String> args){
      args.removeFirst();
      if(!args.isEmpty()){
        throw new SQLException("Insert command must not have Parameters");
      }
    }

    private void into(LinkedList<String> args){
      args.removeFirst();
      String arg = Functions.List.concat(args).trim();
      if(!arg.contains("(")){
        if(arg.contains(" ")){
          throw new SQLException("Data can only be inserted into one Table");
        }
        try {
          table = parentDB.getTable(arg);
        } catch (DatabaseException e) {
          throw new SQLException("Table \"" + args.getFirst() + "\" cannot be found");
        }
        fieldList.addAll(table.getFieldList());
      } else {
        try {
          table = parentDB.getTable(Functions.String.splitOnce(arg, '(')[0]);
        } catch (DatabaseException e) {
          throw new SQLException("Table \"" + args.getFirst() + "\" cannot be found");
        }
        if(arg.charAt(arg.length()-1) != ')'){
          throw new SQLException("Invalid paranthesis in INTO clause");
        }
        //TODO Maybe make more appealing?
        fieldList.addAll(
          Arrays.asList(
            Functions.String.clean(
              Functions.String.splitOnce(
                  Functions.String.removeLast(arg), "(")[1].split(","))));
        if(fieldList.parallelStream().anyMatch(field -> !table.getFieldList().contains(field))){
          throw new SQLException("Cannot insert values into nonexistent fields");
        }
      }
    }

    private void values(LinkedList<String> args){
      args.removeFirst();
      String arg = Functions.List.concat(args).trim();
      if(!arg.contains("(")){
        throw new SQLException("Values have to be supplied in INSERT query");
      } else {
        if(arg.charAt(arg.length()-1) != ')'){
          throw new SQLException("Invalid paranthesis in INTO clause");
        }
        List<String> valuesToInsert = new LinkedList<>(Arrays.asList(Functions.String.clean(Functions.String.splitOnce(Functions.String.removeLast(arg), "(")[1].split(","))));
        if(valuesToInsert.size() != fieldList.size()){
          throw new SQLException("Number of values does not match the number of fields");
        }
        Table.Entry newEntry = table.emptyEntry();
        for(int i = 0 ; i < valuesToInsert.size() ; i ++){
          newEntry.set(fieldList.get(i), valuesToInsert.get(i));
        }
        table.addEntry(newEntry);
      }
    }
  }

  //TODO Maybe make WHERE always come before SET
  // for example in "UPDATE MARA SET A = B WHERE A EQ B
  // -> "UPDATE MARA WHERE A EQ B SET A = B
  private class Update{
    private Table table = Table.empty();
    private List<Table.Entry> entriesToUpdate;
    private String tableName;
    protected Table init(List<List<String>> args){
      table = Table.empty();
      entriesToUpdate = new LinkedList<>();
      for(List<String> list: args){
        try {
          Method method = Update.class.getDeclaredMethod(list.get(0).toLowerCase(), LinkedList.class);
          method.invoke(null,  new LinkedList<>(list));
          performancePrint("Meantime " + method.getName() + ": " + Performance.getSubTime());
        } catch(NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
          e.printStackTrace();
        }
      }
      commitHooks.add(() -> table.save());
      return table;
    }

    private void update(LinkedList<String> args){
      args.removeFirst();
      if(args.size() != 1){
        throw new SQLException("Cannot update values of multiple tables");
      }
      tableName = args.getFirst();
//      try {
//        table = Table.Static.get(args.getFirst());
//      } catch (FileNotFoundException e) {
//        throw new SQLException("Table \"" + args.getFirst() + "\" cannot be found");
//      }
    }

    private void where(LinkedList<String> args){
      args.removeFirst();
      //table = SQL.query("SELECT * FROM " + tableName, parentDB).table;
      table = parentDB.query("SELECT * FROM " + tableName);
      entriesToUpdate = table.getContent().stream().filter(entry -> entry.meetsCondition(args)).collect(Collectors.toList());
    }

    private void set(LinkedList<String> args){
      args.removeFirst();
      List<String> setters = new LinkedList<>(Arrays.asList(Functions.List.concat(args).split(",")));
      //TODO MAKE SURE LENGTH 3 AND MIDDLE EQ = THEN SET THE VALUES
      if(setters.parallelStream().anyMatch(statement -> {
        String[] split = statement.split(" ");
        return split.length != 3 || !split[1].trim().equals("=") || split[0].contains("=");
      })){
        throw new SQLException("Values must be set using =");
      }
      //TODO CHECK!?!??
      setters.forEach(statement -> {
        String[] split = statement.split("=", 2);
        Type type = table.getTypeList().get(table.getFieldList().indexOf(split[0].trim()));
        entriesToUpdate.forEach(entry -> {
          entry.set(split[0].trim().toUpperCase(), Functions.Object.castToType(split[1].trim(), type.getType(), type.getLength()));
        });
      });
    }
  }

  private class Drop{
    private Table table;
    protected Table init(List<List<String>> args){
      table = Table.empty();
      for(List<String> list: args){
        try {
          Method method = Drop.class.getDeclaredMethod(list.get(0).toLowerCase(), LinkedList.class);
          method.invoke(null,  new LinkedList<>(list));
          performancePrint("Meantime " + method.getName() + ": " + Performance.getSubTime());
        } catch(NoSuchMethodException e) {
          throw new SQLException("Function " + list.get(0).toUpperCase() + " nonexistent");
        } catch(IllegalAccessException e){
          e.printStackTrace();
        } catch(InvocationTargetException e) {
          Throwable cause = e.getCause();
          if(cause instanceof DatabaseException){
            throw (DatabaseException)cause;
          }
          e.getCause().printStackTrace();
        }
      }
      return table;
    }
    private void drop(LinkedList<String> args){
      args.removeFirst();
      if(!args.isEmpty()){
        throw new SQLException("Drop statement must not have parameters");
      }
    }
    private void table(LinkedList<String> args){
      args.removeFirst();
      if(args.size() > 1 && !args.get(1).equalsIgnoreCase("IMMEDIATELY")){
        throw new SQLException("Only one argument allowed in DROP TABLE");
      }
      Runnable commit = () -> {
        try {
          if(!new File(parentDB.getTable(args.getFirst()).getPath()).delete()){
            throw new SQLException("Cannot drop table \"" + args.getFirst() + "\"");
          }
        } catch (DatabaseException e) {
          throw new SQLException("Table \"" + args.getFirst() + "\" cannot be found");
        }
      };
      if(args.size() == 2){
        commit.run();
      } else {
        commitHooks.add(commit);
      }
    }
  }

  private class Create{
    private Table table;
    protected Table init(List<List<String>> args){
      table = Table.empty();
      for(List<String> list: args){
        try {
          Method method = Create.class.getDeclaredMethod(list.get(0).toLowerCase(), LinkedList.class);
          method.invoke(null,  new LinkedList<>(list));
          performancePrint("Meantime " + method.getName() + ": " + Performance.getSubTime());
        } catch(NoSuchMethodException e) {
          throw new SQLException("Function " + list.get(0).toUpperCase() + " nonexistent");
        } catch(IllegalAccessException e){
          e.printStackTrace();
        } catch(InvocationTargetException e) {
          Throwable cause = e.getCause();
          if(cause instanceof DatabaseException){
            throw (DatabaseException)cause;
          }
          e.getCause().printStackTrace();
        }
      }
      return table;
    }
    private void create(LinkedList<String> args){
      args.removeFirst();
      if(!args.isEmpty()){
        throw new SQLException("Create statement must not have parameters");
      }
    }
    private void table(LinkedList<String> args){
      args.removeFirst();
      String arg = Functions.List.concatNoSpace(args).trim();
      if(!arg.contains("(")){
        throw new SQLException("Cannot create table without a fieldcat");
      }
      table = Table.empty();
      if(arg.charAt(arg.length()-1) != ')'){
        throw new SQLException("Invalid paranthesis in TABLE clause");
      }
      table.setName(arg.split("\\(", 2)[0]);
      LinkedHashMap<String, Type> fieldCat = new LinkedHashMap<>();
      Arrays.asList(
          Functions.String.clean(
              Functions.String.splitOnce(
                  Functions.String.removeLast(arg), "(")[1].split(","))).stream().
                    forEachOrdered(field -> {
                      String[] split = field.split(":", 2);
                      fieldCat.put(split[0], new Type(split[1]));
                    });
      table.setFieldCat(fieldCat);
      table.setPath(parentDB.getRoot() + "/" + table.getName());
      table.setSettings(new HashMap<>(){{put("compressed", true);}});
      commitHooks.add(() -> table.save());
    }
  }

  private List<Runnable> commitHooks = new LinkedList<>();
  private void addCommitHook(Runnable runnable){
    commitHooks.add(runnable);
  }
  public void commitWork(){
    commitHooks.forEach(Runnable::run);
    commitHooks.clear();
  }

  public void print(){
    Functions.Console.print(this.table);
  }
  public void println(){
    Functions.Console.println(this.table);
  }
  public void printSize(){
    Functions.Console.println(this.table.size());
  }
  public void printContent(){
    Functions.Console.println(table.getContent());
  }
  
  public String toString(){
    if(this.table == null){
      return "Empty SQL";
    }else{
      return this.table.toString();
    }
  }
  public List<Table.Entry> content(){
    return this.table.getContent();
  }
  public int size(){
    if(this.table == null){
      return -1;
    }else{
      return this.table.size();
    }
  }
  
  
  
  
  
  private static void join(Table left, Table right, String type, List<String> conditions){
    left.getFieldCat().putAll(right.getFieldCat());
    left.setFieldCat(left.getFieldCat());
    left.getContent().forEach(entry -> {
      for(int i = 0 ; i < right.getFieldList().size() - 1 ; i ++){
        entry.values.add(null);
      }
    });
    //noinspection SwitchStatementWithTooFewBranches
    switch(type.toUpperCase()){
      case "INNER" -> {
        left.getContent().removeIf(leftEntry -> {
          List<Table.Entry> matchingRightEntries = right.getContent().parallelStream().filter(rightEntry -> {
            List<String> currentConditions = conditions.parallelStream().map(condition -> {
              if(condition.toUpperCase().startsWith(right.getName().toUpperCase() + "~")){
                return rightEntry.get(Functions.String.splitOnce(condition, "~")[1]).toString();
              }
              return condition;
            }).map(condition -> {
              //TODO
              // Replace fields of left Table with values, and use Functions.Boolean.evaluate() instead of Table.Entry.meetsCondition()
              if(condition.toUpperCase().startsWith(left.getName().toUpperCase() + "~")){
                return Functions.String.splitOnce(condition, "~")[1];
              }
              return condition;
            }).collect(Collectors.toList());
            return leftEntry.meetsCondition(currentConditions);
          }).collect(Collectors.toList());
          if(!matchingRightEntries.isEmpty()){
            Table.Entry entry = matchingRightEntries.get(0);
            entry.getFieldList().forEach(field -> {
              leftEntry.set(field, entry.get(field));
            });
          }
          return matchingRightEntries.isEmpty();
        });
      }
      default -> {
        throw new SQLException(type.toUpperCase() + " JOIN is not supported");
      }
    }
  }
}
