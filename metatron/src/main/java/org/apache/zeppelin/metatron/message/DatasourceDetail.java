package org.apache.zeppelin.metatron.message;

import java.util.Map;

public class DatasourceDetail extends Datasource {
  String granularity;
  Map<String, Object> contexts;
  User createdBy;
  User modifiedBy;
  String status;
  String dsType;
  String srcType;

  public String getGranularity() {
    return granularity;
  }

  public Map<String, Object> getContexts() {
    return contexts;
  }

  public User getCreatedBy() {
    return createdBy;
  }

  public User getModifiedBy() {
    return modifiedBy;
  }

  public String getStatus() {
    return status;
  }

  public String getDsType() {
    return dsType;
  }

  public String getSrcType() {
    return srcType;
  }
}
