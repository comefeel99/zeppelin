package org.apache.zeppelin.metatron.message;

import java.util.LinkedList;
import java.util.List;

public class Filter {
  String type;
  String field;
  List<String> valueList;

  public Filter(String type, String field, List<String> valueList) {
    this.type = type;
    this.field = field;
    this.valueList = valueList;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    String type;
    String field;
    List<String> valueList = new LinkedList<>();

    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    public Builder setField(String field) {
      this.field = field;
      return this;
    }

    public Builder addValue(String value) {
      valueList.add(value);
      return this;
    }

    public Filter build() {
      return new Filter(type, field, valueList);
    }
  }

  public String getType() {
    return type;
  }

  public String getField() {
    return field;
  }

  public List<String> getValueList() {
    return valueList;
  }
}
