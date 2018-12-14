package org.apache.zeppelin.metatron.message;

public class Limits {
  long limit;

  public Limits(long limit) {
    this.limit = limit;
  }

  public long getLimit() {
    return limit;
  }
}
