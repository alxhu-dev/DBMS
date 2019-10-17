package table;

import Functions.Path;
import database.DatabaseException;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

//TODO Uncompressed binary write
public class Table {
  /**
   * Constants
   */
  final private static LinkedHashMap<String, Type> ASTERISKS_FIELDCAT = new LinkedHashMap<>(){{
    put("*", null);
  }};
  final private Object contentSync = new Object();
  
  /**
   * Statics
   */
  public static class Static{
//    private static String getFullPath(String name){
//      name = name.replace("\\","/");
//      if(name.toUpperCase().charAt(0) > 64 && name.toUpperCase().charAt(0) < 91 && name.charAt(1) == ':'){
//        return name;
//      }else if(name.toUpperCase().startsWith(getRootPath().toUpperCase())){
//        return name;
//      }else{
//        return getRootPath() + name;
//      }
//    }
//    public static void loadAllTables(){
//      List<File> files = Functions.File.Static.getAllFiles(rootPath);
//      files.forEach(file -> {
//        try {
//          get(file.getPath());
//        } catch (FileNotFoundException e) {
//          throw new DatabaseException("Cannot load Table \"" + file.getPath() + "\"");
//        }
//      });
//    }
    public static String getName(String name){
      return new LinkedList<>(Arrays.asList(name.replace("\\", "/").split("/"))).getLast().split("\\.", 2)[0];
    }
    
    public static void save(Table table){
      List<String> outFile = new LinkedList<>();
      
      //SETTINGS
      StringBuilder temp = new StringBuilder();
      String[] keys = table.getSettings().keySet().toArray(new String[0]);
      Object[] values = table.getSettings().values().toArray(new Object[0]);
      for(int i = 0 ; i < keys.length ; i ++){
        temp.append(Functions.Array.lastElement(values[i].getClass().getName().split("\\.")).substring(0, 1).toUpperCase()).append("_").append(keys[i]).append("=").append(values[i].toString()).append(";");
      }
      outFile.add(Functions.String.removeLast(temp.toString()));
      
      //FIELDCAT
      temp = new StringBuilder();
      keys = table.getFieldCat().keySet().toArray(new String[0]);
      values = table.getFieldCat().values().toArray(new Object[0]);
      for(int i = 0 ; i < keys.length ; i ++){
        temp.append(keys[i]).append(":").append(values[i]).append(";");
      }
      outFile.add(Functions.String.removeLast(temp.toString()));
      
      //CONTENT
      for (Entry entry : table.getContent()) {
        outFile.add(entry.csvString());
      }
      Functions.File.Static.saveStrings(table.getPath(), outFile);
    }
    public static Table load(String name) throws FileNotFoundException {
      if(name == null){
        throw new DatabaseException("Attempt to load null table");
      }
      Iterator<String> iterator;
      Table table;
      try {
        table = new Table(getName(name));
        name = Path.getAbsolutePath(name);
        table.setPath(name);
        List<String> file = Functions.File.Static.loadAsList(name);
        iterator = file.iterator();
      }catch(IllegalArgumentException e){
        throw new FileNotFoundException(name);
      }
      table.setSettings(createSettings(iterator.next()));
      table.setFieldCat(createFieldcat(iterator.next()));
      while (iterator.hasNext()) {
        table.addEntry(iterator.next());
      }
      return table;
    }
  
    static LinkedHashMap<String, Type> fieldCatMerge(LinkedHashMap<String, Type> fieldCat, LinkedHashMap<String, Type> newFieldCat) {
      if(fieldCat == null){
        return newFieldCat;
      }
      if(fieldCat.size() == 1 && fieldCat.containsKey("*")){
        return newFieldCat;
      }
      fieldCat.replaceAll((k, v) -> newFieldCat.get(k));
      return fieldCat;
    }
    private static LinkedHashMap<String, Type> createFieldcat(String s) {
      LinkedHashMap<String, Type> map = new LinkedHashMap<>();
      String[] fields = Functions.String.splitMultiple(s, ':',';');
      for(int i = 0 ; i + 1 < fields.length ; i += 2){
        map.put(fields[i].toUpperCase(), new Type(fields[i+1]));
      }
      return map;
    }
    private static HashMap<String, Object> createSettings(String s) {
      HashMap<String, Object> map = new HashMap<>();
      String[] settings = Functions.String.splitMultiple(s, '_', '=', ';');
      for(int i = 0 ; i + 2 < settings.length ; i+=3){
        map.put(settings[i + 1], switch(settings[i].toUpperCase()){
          case "B" -> Boolean.valueOf(settings[i+2]);
          case "S" -> settings[i+2];
          case "C" -> Functions.Character.toChar(settings[i+2]);
          default -> throw new IllegalArgumentException("Cannot find settings type \"" + settings[i] + "\"");
        });
      }
      return map;
    }
    
  }
  
  /**
   * Getters & Setters
   */
  public String getPath() {
    return path;
  }
  public void setPath(String path) {
    this.path = path;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name.toUpperCase();
  }
  public List<Entry> getContent() {
    if(this.content == null){
      this.setContent(new LinkedList<>());
    }
    return content;
  }
  public void setContent(List<Entry> content) {
    this.content = content;
  }
  public LinkedHashMap<String, Type> getFieldCat() {
    return fieldCat;
  }
  public void setFieldCat(LinkedHashMap<String, Type> fieldCat) {
    this.fieldCat = fieldCat;
    if(fieldCat != null) {
      fieldCat.entrySet().parallelStream().forEach(entry -> {
        if(!entry.getKey().equals(entry.getKey().toUpperCase())){
          fieldCat.put(entry.getKey().toUpperCase(), entry.getValue());
          fieldCat.remove(entry.getKey());
        }
      });
      Type[] values = Functions.Collection.values(this.getFieldCat().entrySet());
      if(values != null) {
        this.setTypeList(values);
      }
      String[] fields = Functions.Collection.keys(this.getFieldCat().entrySet());
      if(fields != null) {
        this.setFieldList(fields);
      }
    }
    if (!this.isEmpty()) {
      this.getContent().parallelStream().forEach(entry -> entry.setFieldCat(this.getFieldCat()));
    }
  }
  public List<Type> getTypeList() {
    return typeList;
  }
  public void setTypeList(List<Type> typeList) {
    this.typeList = typeList;
  }
  public void setTypeList(Type[] types){
    if(types != null){
      setTypeList(Arrays.asList(types));
    }
  }
  public List<String> getFieldList() {
    return fieldList;
  }
  public void setFieldList(List<String> fieldList) {
    this.fieldList = fieldList.parallelStream().map(String::toUpperCase).collect(Collectors.toList());
  }
  public void setFieldList(String[] fields){
    if(fields != null){
      setFieldList(Arrays.asList(fields));
    }
  }
  public HashMap<String, Object> getSettings() {
    return settings;
  }
  public void setSettings(HashMap<String, Object> settings) {
    this.settings = settings;
    if(!this.isEmpty()) {
      for (Entry entry : this.getContent()) {
        entry.setSettings(this.getSettings());
      }
    }
  }
  @Deprecated
  public void setFields(Collection<String> fields){
    LinkedHashMap<String, Type> map = new LinkedHashMap<>();
    for(String string: fields){
      map.put(string.toUpperCase(), null);
    }
    setFieldCat(map);
  }
  
  /**
   * Random Methods
   */
  public void save(){
    Static.save(this);
  }
  @Deprecated
  public void loadIn(String otherTableName){
    Table otherTable = null;
    try {
      otherTable = Static.load(otherTableName);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return;
    }
    this.setSettings(otherTable.getSettings());
    if(!this.getFieldCat().equals(otherTable.getFieldCat())) {
      this.setFieldCat(Static.fieldCatMerge(this.getFieldCat(), otherTable.getFieldCat()));
      this.setContent(otherTable.getContent());
    }
    String[] fields = this.getFieldList().toArray(new String[0]);
    for(Table.Entry entry: this.getContent()){
      entry.setFields(fields);
    }
  }
  
  public int size() {
    return this.getContent().size();
  }
  public boolean isEmpty(){
    return this.getContent() == null || this.getContent().isEmpty();
  }
  public void setUnique(boolean bool){
    this.unique = bool;
  }
  
  
  /**
   * Members
   */
  private String name;
  private String path;
  private List<Entry> content;
  private LinkedHashMap<String, Type> fieldCat;
  private List<Type> typeList;
  private List<String> fieldList;
  private HashMap<String, Object> settings;
  
  boolean checkedCompressed = false;
  boolean compressed = false;
  boolean unique = false;
  
  /**
   * Constructors
   */
  public static Table empty(){
    return new Table("", null, null, null);
  }
  public Table(String name, List<Entry> content, LinkedHashMap<String, Type> fieldCat, HashMap<String, Object> settings){
    this.setName(name);
    this.setContent(content);
    this.setFieldCat(fieldCat);
    this.setSettings(settings);
  }
  public Table(List<Entry> content, LinkedHashMap<String, Type> fieldCat, HashMap<String, Object> settings){
    this("", content, fieldCat, settings);
  }
  public Table(String name){
    this(name, new LinkedList<>(), new LinkedHashMap<>(), new HashMap<>());
  }
  public Table(){
    this("");
  }

  
  public Table addTable(Table table){
    if(ASTERISKS_FIELDCAT.equals(this.getFieldCat()) || this.getFieldCat() == null || this.getFieldCat().isEmpty()){
      this.setFieldCat(table.getFieldCat());
      this.setSettings(table.getSettings());
    }
    if(this.getFieldCat().equals(table.getFieldCat())){
      for (Entry entry : table.getContent()) {
        this.addEntry(entry);
      }
    }else{
      throw new IllegalArgumentException("Cannot add different Tables");
    }
    return this;
  }
  public Table addTable(String table){
    try {
      return addTable(Static.load(table));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return this;
    }
  }
  public Table addTableThreaded(String table){
    try {
      return addTableThreaded(Static.load(table));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return this;
    }
  }
  public Table addTableThreaded(Table newTable){
    if(!this.getFieldCat().isEmpty() && !newTable.getFieldCat().isEmpty() && !newTable.fieldCat.equals(this.getFieldCat())){
      throw new DatabaseException("Cannot add Tables with different Fieldcats: " + this.getName() + ", " + newTable.getName());
    }
    if(ASTERISKS_FIELDCAT.equals(this.getFieldCat()) || this.getFieldCat().isEmpty()){
      this.setFieldCat(newTable.getFieldCat());
      this.setSettings(newTable.getSettings());
    }
    synchronized (this.contentSync){
      newTable.getContent().forEach(this::addEntry);
    }
    return this;
  }
  
  @Deprecated
  public Table query(String query) {
    throw new UnsupportedOperationException();
//    //TODO Replace with real SQL or delete
//    SQL sql = new SQL();
//    sql.table = this;
//    if(!query.toUpperCase().startsWith("SELECT")){
//      sql.operation = SQL.Operation.SELECT;
//    }
//    try {
//      return sql.query(query);
//    } catch (FileNotFoundException e) {
//      System.err.println("File not Found in table.query");
//      e.printStackTrace();
//      return this;
//    }
  }

  public String toString(){
    StringBuilder retString = new StringBuilder(this.getName() + "\n" +
        (this.getSettings() == null ? "No Settings" : this.getSettings().toString()) + "\n" +
        (this.getFieldCat() == null ? "No Fieldcat" : this.getFieldCat().toString()));
    for (Entry entry : getContent()) {
      retString.append("\n").append(entry);
    }
    return retString.toString();
  }
  public String[] toStringArray(){
    Entry[] entries = this.getContent().toArray(new Entry[0]);
    String[] ret = new String[entries.length];
    for(int i = 0 ; i < ret.length ; i ++){
      ret[i] = entries[i].toString();
    }
    return ret;
  }

  public Entry emptyEntry(){
    return new Entry();
  }
  public void addEntry(String entry){
    Entry newEntry = new Entry(entry);
    if(!unique || !this.getContent().contains(newEntry)) {
      addEntry(newEntry);
    }
  }
  public void addEntry(Entry entry){
    if(!unique || !this.getContent().contains(entry)) {
      entry.setFieldCat(this.getFieldCat());
      this.getContent().add(entry);
    }
  }
  private static int instanceCounter = 0;
  public class Entry{
    private int uniqueIndex;
    private Map<String, Type> fieldCat;
    private List<Type> typeList;
    private List<String> fieldList;
    private HashMap<String, Object> settings;
    public List<Object> values;
    private Entry(){
      instanceCounter++;
      uniqueIndex = instanceCounter;
      this.setFieldCat(Table.this.getFieldCat());
      this.setSettings(Table.this.getSettings());
      this.values = new LinkedList<>();
      for(int i = 0 ; i < this.getFieldList().size() ; i ++){
        this.values.add(i, null);
      }
    }
    public Entry(String values){
      instanceCounter++;
      uniqueIndex = instanceCounter;
      this.setFieldCat(Table.this.getFieldCat());
      this.setSettings(Table.this.getSettings());
      this.values = new LinkedList<>();
      if(!checkedCompressed) {
        compressed = !this.getSettings().containsKey("compressed") || !this.getSettings().get("compressed").equals("true");
        checkedCompressed = true;
      }
      if(compressed){
        List<String> elements = Functions.String.splitToList(values, ';');
        List<Type> typeList = this.getTypeList();
        if(elements.size() != this.getFieldCat().size()){
          throw new IllegalArgumentException("Cannot convert line: \"" + values + "\"");
        }
        Iterator<String> itString = elements.iterator();
        Iterator<Type> itType = typeList.iterator();
        while(itString.hasNext() && itType.hasNext()){
          Type type = itType.next();
          this.values.add(Functions.Object.castToType(itString.next(), type.getType(), type.getLength()));
        }
      }else{
        //TODO
        throw new UnsupportedOperationException("Uncompressed files not yet supported");
      }
    }
    public int getUniqueIndex(){
      return this.uniqueIndex;
    }
    public String csvString(){
      StringBuilder retString = new StringBuilder();
      if(this.getSettings().get("compressed").equals(true)) {
        for (Object value : values) {
          retString.append(value).append(";");
        }
      }
      return Functions.String.removeLast(retString.toString());
    }
    public String toGroupString(){
      assert(this.getFieldList().get(this.getFieldList().size() - 1).equals("sum"));
      StringBuilder retString = new StringBuilder();
      for (Object value : Functions.List.subList(this.values, 0, this.values.size() - 1)) {
        retString.append(value).append(" | ");
      }
      retString = new StringBuilder(retString.substring(0, retString.length() - 3));
      return retString.toString();
    }
    public String toString(){
      StringBuilder retString = new StringBuilder();
      for (Object value : values) {
        retString.append(value).append(" | ");
      }
      retString = new StringBuilder(retString.substring(0, retString.length() - 3));
      return retString.toString();
    }
    public boolean meetsCondition(List<String> parameter){
      if(parameter.isEmpty()){
        return true;
      }
      LinkedList<String> conditions = new LinkedList<>();
      for(int conditionIndex = 0 ; conditionIndex + 2 < parameter.size() ; conditionIndex += 4){
        //TODO String offsets
        String value;
        try {
          value = this.values.get(this.getFieldList().indexOf(parameter.get(conditionIndex).toUpperCase())).toString();
        } catch(IndexOutOfBoundsException e){
          throw new DatabaseException("Unknown field \"" + parameter.get(conditionIndex) + "\" in table \"" + Table.this.getName() + "\"");
        }
        conditions.add(String.valueOf(Functions.Boolean.evaluate(value, parameter.get(conditionIndex + 1), Functions.String.unquote(parameter.get(conditionIndex + 2)))));
        if(conditionIndex + 3 < parameter.size()){
          conditions.add(parameter.get(conditionIndex + 3));
        }
      }
      return Functions.Boolean.evaluateLogical(conditions);
    }
    public Object get(String key){
      return this.values.get(this.getFieldList().indexOf(key.toUpperCase()));
    }
    public void set(String key, Object value){
      //this.values.ensurecapacity(this.getFieldList().indexOf(key));
      this.values.set(this.getFieldList().indexOf(key), value);
    }
    public void setFields(String[] fields){
      if(fields.length == 1 && fields[0].equals("*")){
        return;
      }
      Map<String, Type> fieldCat = new LinkedHashMap<>();
      List<Object> values = new LinkedList<>();
      for (String field : fields) {
        if (this.getFieldCat().containsKey(field)) {
          fieldCat.put(field, this.getFieldCat().get(field));
          values.add(this.values.get(this.getFieldList().indexOf(field)));
        } else {
          throw new IllegalArgumentException("Field \"" + field + "\" cannot be found in Table \"" + Table.this.getName() + "\"");
        }
      }
      this.tempfieldCat = fieldCat;
      this.values = values;
    }
    private Map<String, Type> tempfieldCat;
    public void updateParent(){
      this.setFieldCat(this.tempfieldCat);
    }

    public void newFieldCat(Map<String, Type> fieldCat) {
      LinkedList<Object> values = new LinkedList<>();
      LinkedList<String> keys = new LinkedList<>(fieldCat.keySet());
      keys.remove("sum");
      for (String key : keys) {
        try {
          values.add(this.values.get(this.getFieldList().indexOf(key)));
        } catch(IndexOutOfBoundsException e) {
          System.err.println("Field " + key + " does not exist in Fieldlist " + this.getFieldList().toString());
        } catch(Exception e){
          //TODO Obsolete?
          Functions.Console.println(this.getFieldList().indexOf(key));
          throw e;
        }
      }
      Functions.List.ensureCapacity(values, fieldCat.size());
      this.values = values;
    }
    public boolean equals(Object other){
      if(other instanceof Entry){
        return this.equalsExceptSum((Entry)other);
      }
      return false;
    }
    public boolean equalsPrecise(Entry other){
      return this.values.equals(other.values);
    }
    public boolean equalsExceptSum(Entry other){
      assert(this.getFieldList().get(this.getFieldList().size() - 1).equals("sum"));      //Last Element in fieldCat is the sum
      if(this.getFieldList().get(this.getFieldList().size() - 1).toUpperCase().equals("SUM")){
        return Functions.List.subList(this.values, 0, this.values.size() - 1).equals(Functions.List.subList(other.values, 0 , other.values.size() - 1));
      }
      return this.equalsPrecise(other);
    }
    public int hashCode(){
      return this.toGroupString().hashCode();
    }
  
    public Map<String, Type> getFieldCat() {
      return fieldCat;
    }
  
    public void setFieldCat(Map<String, Type> fieldCat) {
      this.fieldCat = fieldCat;
      if(fieldCat != null) {
        Type[] values = Functions.Collection.values(this.getFieldCat().entrySet());
        if(values != null) {
          this.setTypeList(Arrays.asList(values));
        }
        String[] fields = Functions.Collection.keys(this.getFieldCat().entrySet());
        if(fields != null) {
          this.setFieldList(Arrays.asList(fields));
        }
      }
    }
  
    public List<Type> getTypeList() {
      return typeList;
    }
  
    public void setTypeList(List<Type> typeList) {
      this.typeList = typeList;
    }
  
    public List<String> getFieldList() {
      return fieldList;
    }
  
    public void setFieldList(List<String> fieldList) {
      this.fieldList = fieldList;
    }
  
    public HashMap<String, Object> getSettings() {
      return settings;
    }
  
    public void setSettings(HashMap<String, Object> settings) {
      this.settings = settings;
    }
  }
  
}


