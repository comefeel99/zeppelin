package org.apache.zeppelin.metatron.message;

import java.util.List;

public class SQLQuery {

    static class Connection {
        List<Datasource> datasources;
        String implementor;
        String authenticationType;
        String username;
        String password;
        String hostname;
        String port;
    }

    String type;
    Connection connection;
    String database;
    String query;

    public SQLQuery(String query) {

//      TODO: need to change metatron api for sql query with default connection info

        type = "QUERY";
        connection = new Connection();
        connection.implementor = "DRUID";
        connection.authenticationType = "MANUAL";
        connection.username = "";
        connection.password = "";
        connection.hostname = "localhost";
        connection.port = "8082";
        database = "druid";
        this.query = query;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
