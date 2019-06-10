package org.apache.zeppelin.metatron.message;

public class Link {
  String href;
  boolean templated;

  public String getHref() {
    return href;
  }

  public boolean isTemplated() {
    return templated;
  }
}
