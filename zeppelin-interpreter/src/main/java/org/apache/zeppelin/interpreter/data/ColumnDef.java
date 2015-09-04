package org.apache.zeppelin.interpreter.data;

/**
 * ColumnDefinition of TableData
 */
public class ColumnDef {
  String name;

  public ColumnDef(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
