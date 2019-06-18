package org.apache.zeppelin.metatron.message;

public class Field {
  long id;
  String name;
  String logicalName;
  String type;
  String logicalType;
  String role;
  String aggrType;
  long seq;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;

  }

  public String getName() {
    return name;
  }


  public void setName(String name) {
    this.name = name;
  }

  public String getLogicalName() {
    return logicalName;
  }

  public void setLogicalName(String logicalName) {
    this.logicalName = logicalName;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public void setLogicalType(String logicalType) {
    this.logicalType = logicalType;
  }

  public String getRole() {
    return role;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public String getAggrType() {
    return aggrType;
  }

  public void setAggrType(String aggrType) {
    this.aggrType = aggrType;
  }

  public long getSeq() {
    return seq;
  }

  public void setSeq(long seq) {
    this.seq = seq;

  }
}
