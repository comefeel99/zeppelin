package org.apache.zeppelin.metatron.message;

import java.util.List;

public class SQLQueryResponse {

    List<Record> fields;
    List<Record> data;

    public List<Record> getFields() {
        return fields;
    }

    public void setFields(List<Record> fields) {
        this.fields = fields;
    }

    public List<Record> getData() {
        return data;
    }

    public void setData(List<Record> data) {
        this.data = data;
    }
}
