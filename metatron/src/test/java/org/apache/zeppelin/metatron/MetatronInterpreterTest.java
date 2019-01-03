package org.apache.zeppelin.metatron;

import org.apache.zeppelin.metatron.antlr.MetatronParser;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class MetatronInterpreterTest {

  @Test
  public void testParseSingleStatement() throws IOException {
    // given
    MetatronInterpreter interpreter = new MetatronInterpreter(new Properties());

    // when
    MetatronParser parser = interpreter.parseMetatronExpr("describe datasources");

    // then
    List<MetatronParser.StmtContext> stmts = parser.exprs().stmt();
    assertEquals(1, stmts.size());
    assertEquals("datasources", stmts.get(0).RESOURCE().getText());
  }

  @Test
  public void testParseMultipleStatements() throws IOException {
    // given
    MetatronInterpreter interpreter = new MetatronInterpreter(new Properties());

    // when
    MetatronParser parser = interpreter.parseMetatronExpr("describe datasources;describe sales;");

    // then
    List<MetatronParser.StmtContext> stmts = parser.exprs().stmt();
    assertEquals(2, stmts.size());
    assertEquals("datasources", stmts.get(0).RESOURCE().getText());
    assertEquals("sales", stmts.get(1).RESOURCE().getText());
  }

  @Test
  public void testInvalidGrammar() throws IOException {
    // given
    MetatronInterpreter interpreter = new MetatronInterpreter(new Properties());

    // when
    MetatronParser parser = interpreter.parseMetatronExpr("something not valid");

    // then
    List<MetatronParser.StmtContext> stmts = parser.exprs().stmt();
    assertEquals(0, stmts.size());
  }
}
