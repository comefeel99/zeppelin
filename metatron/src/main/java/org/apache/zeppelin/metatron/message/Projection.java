package org.apache.zeppelin.metatron.message;

public class Projection {
  String type;
  String name;

  public Projection(String type, String name) {
    this.type = type;
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }
}
