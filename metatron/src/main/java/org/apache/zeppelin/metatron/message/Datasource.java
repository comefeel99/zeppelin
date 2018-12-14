package org.apache.zeppelin.metatron.message;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Datasource {
  String name;
  String engineName;
  String id;
  String description;
  String connType;
  Date modifiedTime;
  boolean published;
  DatasourceSummary summary;

  List<Field> fields;

  Map<String, Link> _links;

  public String getName() {
    return name;
  }

  public String getEngineName() {
    return engineName;
  }

  public String getId() {
    return id;
  }

  public String getDescription() {
    return description;
  }

  public String getConnType() {
    return connType;
  }

  public Date getModifiedTime() {
    return modifiedTime;
  }

  public boolean isPublished() {
    return published;
  }

  public List<Field> getFields() {
    return fields;
  }

  public Map<String, Link> get_links() {
    return _links;
  }
}
