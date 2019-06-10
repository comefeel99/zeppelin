package org.apache.zeppelin.metatron.message;

public class Field {
  long id;
  String name;
  String alias;
  String type;
  String logicalType;
  String role;
  long seq;
  String biType;

  public void Field() {
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getAlias() {
    return alias;
  }

  public String getType() {
    return type;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public String getRole() {
    return role;
  }

  public long getSeq() {
    return seq;
  }

  public String getBiType() {
    return biType;
  }
}
