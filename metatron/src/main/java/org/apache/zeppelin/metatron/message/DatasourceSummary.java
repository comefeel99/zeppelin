package org.apache.zeppelin.metatron.message;

import java.util.Date;

public class DatasourceSummary {
  Date ingestionMinTime;
  Date ingestionMaxTime;
  Date lastAccessTime;
  long size;
  long count;

  public void DatasourceSummary() {
  }

  public Date getIngestionMinTime() {
    return ingestionMinTime;
  }

  public Date getIngestionMaxTime() {
    return ingestionMaxTime;
  }

  public Date getLastAccessTime() {
    return lastAccessTime;
  }

  public long getSize() {
    return size;
  }

  public long getCount() {
    return count;
  }
}
