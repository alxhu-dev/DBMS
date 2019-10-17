package table;

public class Type {
  private String type;
  private int length;
  boolean primary_key;
  //TODO primary keys implementieren
  public Type(String type, int length){
    this.setType(type);
    this.setLength(length);
  }
  public Type(String fullType){
    this.setType(fullType.split("\\(", 2)[0]);
    this.setLength(Integer.parseInt(Functions.String.removeLast(fullType.split("\\(", 2)[1])));
  }
  public String toString(){
    return this.getType() + "(" + this.getLength() + ")";
  }
  public boolean equals(Object object){
    if(object instanceof Type){
      Type type = (Type)object;
      return this.getType().equals(type.getType()) && this.getLength() == type.getLength();
    }
    return false;
  }
  
  public String getType() {
    return type;
  }
  
  public void setType(String type) {
    this.type = type;
  }
  
  public int getLength() {
    return length;
  }
  
  public void setLength(int length) {
    this.length = length;
  }
}
