/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.metatron;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.metatron.antlr.MetatronLexer;
import org.apache.zeppelin.metatron.antlr.MetatronParser;
import org.apache.zeppelin.metatron.client.MetatronClient;
import org.apache.zeppelin.metatron.message.*;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;

/**
 * Metatron Interpreter for Zeppelin.
 */
public class MetatronInterpreter extends Interpreter {
  private static Logger LOGGER = LoggerFactory.getLogger(MetatronInterpreter.class);

  public static final String METATRON_URL = "metatron.url";

  private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private final Pattern showDatabasesPattern;
  private final Pattern showDetailPattern;
  private final Pattern getDataPattern;
  private final Pattern sqlQueryPattern;
  private MetatronClient client;

  public MetatronInterpreter(Properties property) {
    super(property);
    showDatabasesPattern = Pattern.compile("show datasources");
    showDetailPattern = Pattern.compile("show (?<datasource>.*)");
    getDataPattern = Pattern.compile("datasource=(?<datasource>[^ ]+)[ ](?<filter>[^ ]+)[ ]limit=(?<limit>[0-9]+)[ ](?<dimension>[^ ]+)[ ](?<measure>[^ ]+)");
    sqlQueryPattern = Pattern.compile("select (.*)");
  }

  @Override
  public void open() {
    try {
      client = new MetatronClient(getProperty(METATRON_URL));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    if (interpreterContext != null) {
      interpreterContext.getResourcePool().put("metatron", this);
      client.setAccessToken(interpreterContext
              .getAuthenticationInfo()
              .getUserCredentials()
              .getUsernamePassword("token")
              .getPassword());
    }

    try {
      InterpreterResult result = runMetatronQuery(cmd, interpreterContext);
      if (result != null) {
        return result;
      } else {
        return new InterpreterResult(InterpreterResult.Code.ERROR, String.format("Unknown expression '%s'", cmd));
      }
    } catch (IOException e) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
  }

  public InterpreterResult runMetatronQuery(String query) throws IOException {
    return runMetatronQuery(query, null);
  }

  String ignoreComment(String input){

    String[] change_target = input.split("\\n");

    StringBuilder result = new StringBuilder();

    String prefix = "";
    for( String curLine : change_target ){

      String tLine = curLine.trim();

      if( tLine.length() > 0 && tLine.charAt(0) == '#'){
        continue;
      }

      result.append(prefix);
      prefix = "\n";
      result.append(curLine);
    }

    return result.toString();

  }

  InterpreterResult runMetatronQuery(String query, InterpreterContext interpreterContext) throws IOException {


    query = ignoreComment(query);

    Matcher m = showDatabasesPattern.matcher(query);
    if (m.matches()) {
      List<Datasource> resp = client.showDatasources();
      return new InterpreterResult(
              InterpreterResult.Code.SUCCESS,
              ImmutableList.of(
                      datasourcesToTable(resp)
              )
      );
    }

    m = showDetailPattern.matcher(query);
    if (m.matches()) {
      DatasourceDetail detail = client.showDatasource(m.group("datasource"));

      StringBuilder summary = new StringBuilder();
      String summaryFormat = "%-20s: %s\n";
      summary.append(String.format(summaryFormat, "Created by", detail.getCreatedBy().getFullName()));
      summary.append(String.format(summaryFormat, "Published", detail.isPublished()));
      summary.append(String.format(summaryFormat, "Status", detail.getStatus()));
      summary.append(String.format(summaryFormat, "Description", detail.getDescription()));

      StringBuilder fields = new StringBuilder();
      fields.append("id\tname\tlogicalName\ttype\tlogicalType\trole\taggrType\tseq\n");
      for (Field f : detail.getFields()) {
        fields.append(String.valueOf(f.getId()) + '\t' +
                f.getName() + '\t' +
                f.getLogicalName() + '\t' +
                f.getType() + '\t' +
                f.getLogicalType() + '\t' +
                f.getRole() + '\t' +
                f.getAggrType() + '\t' +
                String.valueOf(f.getSeq()) + '\n');
      }

      return new InterpreterResult(
              InterpreterResult.Code.SUCCESS,
              ImmutableList.of(
                      new InterpreterResultMessage(
                              InterpreterResult.Type.TEXT, summary.toString()),
                      new InterpreterResultMessage(
                              InterpreterResult.Type.TABLE, fields.toString())
              )
      );
    }

    m = getDataPattern.matcher(query);
    if (m.matches()) {
      String datasourceName = m.group("datasource");
      String filterExpr = m.group("filter");
      String limit = m.group("limit");
      String dimension = m.group("dimension");
      String measure = m.group("measure");

      List<Filter> filters = new LinkedList<>();
      for (String expr : filterExpr.split(",")) {
        String[] fieldValue = expr.split("=");
        filters.add(Filter.newBuilder()
                .setType("include")
                .setField(fieldValue[0])
                .addValue(fieldValue[1])
                .build());
      }

      DataResponse data = client.getData(
              datasourceName,
              filters,
              ImmutableList.of(
                      new Projection("dimension", dimension),
                      new Projection("measure", measure)),
              new Limits(Long.parseLong(limit))
      );

      StringBuilder table = new StringBuilder();

      if (interpreterContext != null) {
        interpreterContext.getResourcePool().put("data", data);
      }

      // create header
      if (data.size() <= 0) {
        return new InterpreterResult(InterpreterResult.Code.SUCCESS);
      }

      for (String key : data.get(0).keySet()) {
        if (table.toString().length() > 0) {
          table.append("\t");
        }
        table.append(key);
      }
      table.append("\n");

      // add rows
      for (Record r : data) {
        Collection<Object> values = r.values();
        int i = 0;
        for (Object v : values) {
          if (i++ > 0) {
            table.append("\t");
          }
          table.append(v);
        }
        table.append("\n");
      }

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, table.toString());
    }


    m = sqlQueryPattern.matcher(query);
    if (m.matches()) {

      SQLQueryResponse sqlQueryResponse = client.runSQLQuery( query );



      StringBuilder table = new StringBuilder();

      if (interpreterContext != null) {
        interpreterContext.getResourcePool().put("data", sqlQueryResponse);
      }

      // create header
      if (sqlQueryResponse.getData().size() <= 0) {
        return new InterpreterResult(InterpreterResult.Code.SUCCESS);
      }


      for (Record r : sqlQueryResponse.getFields()) {
        if (table.toString().length() > 0) {
          table.append("\t");
        }
        table.append(r.get("name"));
      }
      table.append("\n");


      // add rows
      for (Record r : sqlQueryResponse.getData()) {

        Iterator i = r.values().iterator();

        if( i.hasNext() ) {
          table.append(i.next());
        }

        while( i.hasNext()) {
          table.append("\t");
          table.append(i.next());
        }

        table.append("\n");
      }

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, table.toString());
    }



    return null;
//    // parse statements using antlr and execute
//    MetatronParser parser = parseMetatronExpr(query);
//    List<InterpreterResultMessage> results = execStatement(parser.exprs().stmt());
//    if (results != null && results.size() > 0) {
//      return new InterpreterResult(InterpreterResult.Code.SUCCESS, results);
//    } else {
//      return null;
//    }
  }

  MetatronParser parseMetatronExpr(String cmd) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(cmd.getBytes(StandardCharsets.UTF_8));
    MetatronLexer lexer = new org.apache.zeppelin.metatron.antlr.MetatronLexer(CharStreams.fromStream(inputStream, StandardCharsets.UTF_8));
    MetatronParser parser = new org.apache.zeppelin.metatron.antlr.MetatronParser(new CommonTokenStream(lexer));
    return parser;
  }

//  List<InterpreterResultMessage> execStatement(List<MetatronParser.StmtContext> stmts) {
//    return stmts.stream()
//        .flatMap(s -> execStatement(s).stream())
//        .collect(Collectors.toList());
//  }
//
//  List<InterpreterResultMessage> execStatement(MetatronParser.StmtContext stmt) {
////    String resource = stmt.RESOURCE().getText();
//    String resource = "sales";
//
//    DatasourceDetail detail = null;
//    try {
//      detail = client.showDatasource(resource);
//    } catch (IOException e) {
//      logger.error("Can't get datasource detail " + resource);
//      return ImmutableList.of(
//          new InterpreterResultMessage(InterpreterResult.Type.TEXT, e.getMessage())
//      );
//    }
//
//    StringBuilder summary = new StringBuilder();
//    String summaryFormat = "%-20s: %s\n";
//    summary.append(String.format(summaryFormat, "Created by", detail.getCreatedBy().getFullName()));
//    summary.append(String.format(summaryFormat, "Published", detail.isPublished()));
//    summary.append(String.format(summaryFormat, "Status", detail.getStatus()));
//    summary.append(String.format(summaryFormat, "Description", detail.getDescription()));
//
//    StringBuilder fields = new StringBuilder();
//    fields.append("id\tname\talias\ttype\tlogicalType\trole\tbiType\n");
//    for (Field f : detail.getFields()) {
//      fields.append(f.getId() + '\t' +
//          f.getName() + '\t' +
//          f.getAlias() + '\t' +
//          f.getType() + '\t' +
//          f.getLogicalType() + '\t' +
//          f.getRole() + '\t' +
//          f.getBiType() + '\n');
//    }
//
//    return ImmutableList.of(
//        new InterpreterResultMessage(
//            InterpreterResult.Type.TEXT, summary.toString()),
//        new InterpreterResultMessage(
//            InterpreterResult.Type.TABLE, fields.toString())
//    );
//  }

  private InterpreterResultMessage datasourcesToTable(List<Datasource> ds) {
    StringBuilder table = new StringBuilder();
    table.append("id\tname\ttype\tengine\tdescription\n");

    for(Datasource d : ds) {
      table.append(d.getId() + '\t' + d.getName() + '\t'  + d.getConnType() + '\t' + d.getEngineName() + '\t' + d.getDescription() + '\n') ;
    }
    return new InterpreterResultMessage(InterpreterResult.Type.TABLE, table.toString());
  }


  @Override
  public void cancel(InterpreterContext interpreterContext) {
    // Nothing to do
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext interpreterContext) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String s, int i,
      InterpreterContext interpreterContext) {
    final List suggestions = new ArrayList<>();
    return suggestions;
  }

  public MetatronClient getClient() {
    return client;
  }

  private String getTokenForApiRequest(InterpreterContext interpreterContext) {
    AuthenticationInfo authenticationInfo = interpreterContext.getAuthenticationInfo();
    // TODO 'authentication' should hold token from Metatron SSO
    // For now, request token manually here
    return null;
  }
}
