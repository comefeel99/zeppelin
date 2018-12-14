package org.apache.zeppelin.metatron.message;

import java.util.List;

public class DatasourcesResponse {
  static class Embedded {
    List<Datasource> datasources;
  }

  Embedded _embedded;

  public List<Datasource> getDatasources() {
    return _embedded.datasources;
  }
}
