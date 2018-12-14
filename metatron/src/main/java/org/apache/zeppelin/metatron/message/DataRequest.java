package org.apache.zeppelin.metatron.message;

import java.util.List;

public class DataRequest {

  private DatasourceRequest dataSource;
  private List<Filter> filters;
  private Limits limits;
  private List<Projection> projections;
  private boolean preview;

  public DataRequest(
          DatasourceRequest dataSource,
          List<Filter> filters,
          List<Projection> projections,
          Limits limits,
          boolean preview)
  {
    this.dataSource = dataSource;
    this.filters = filters;
    this.projections = projections;
    this.limits = limits;
    this.preview = preview;
  }

  public DatasourceRequest getDataSource() {
    return dataSource;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public Limits getLimits() {
    return limits;
  }

  public List<Projection> getProjections() {
    return projections;
  }

  public boolean isPreview() {
    return preview;
  }
}
