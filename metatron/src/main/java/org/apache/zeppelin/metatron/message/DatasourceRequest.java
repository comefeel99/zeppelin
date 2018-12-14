package org.apache.zeppelin.metatron.message;

import java.util.LinkedList;
import java.util.List;

public class DatasourceRequest {
  String name;
  String type;
  boolean temporary;
  List<Object> joins;

  public DatasourceRequest(String name, String type, boolean temporary) {
    this.name = name;
    this.type = type;
    this.temporary = temporary;
    this.joins = new LinkedList<>();
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public boolean isTemporary() {
    return temporary;
  }

  public List<Object> getJoins() {
    return joins;
  }
}
