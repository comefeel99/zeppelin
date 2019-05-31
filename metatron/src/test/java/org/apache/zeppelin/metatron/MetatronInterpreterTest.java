package org.apache.zeppelin.metatron;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.apache.zeppelin.metatron.antlr.MetatronParser;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.After;
import org.junit.Before;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class MetatronInterpreterTest {

//
//  @Test
//  public void testParsenewStatement() throws IOException {
//    // given
//    MetatronInterpreter interpreter = new MetatronInterpreter(new Properties());
//
//    // when
////    MetatronParser parser = interpreter.parseMetatronExpr("source=sales Category=Furniture");
//    MetatronParser parser = interpreter.parseMetatronExpr("source=sales Category=Furniture | group sum(sales) by Category | rename sum(sales) as sum_sales | field Category, sum(sales)");
////    MetatronParser parser = interpreter.parseMetatronExpr("abck adf;adf");
//
//    List<ANTLRErrorListener> antlrErrorListeners = (List<ANTLRErrorListener>) parser.getErrorListeners();
//    // then
//    MetatronParser.ExprsContext exprs = parser.exprs();
//
//    MetatronParser.StmtContext stmtContext = exprs.stmt();
//
//    MetatronParser.BaseExpressionContext baseExpressionContext = stmtContext.baseExpression();
//
//    MetatronParser.SelectExpressionContext selectExpressionContext = baseExpressionContext.selectExpression();
//
//    String errorstring = selectExpressionContext.exception.toString();
//
//    String sample = exprs.getText();
////    String sample2 = exprs.IDENTIFIER().getText();
////    List<MetatronParser.StmtContext> stmts = parser.exprs().stmt();
////    assertEquals(1, stmts.size());
////    assertEquals("datasources", stmts.get(0).RESOURCE().getText());
//  }

//  @Test
//  public void testParseSingleStatement() throws IOException {
//    // given
//    MetatronInterpreter interpreter = new MetatronInterpreter(new Properties());
//
//    // when
//    MetatronParser parser = interpreter.parseMetatronExpr("describe datasources");
//
//    // then
//    List<MetatronParser.StmtContext> stmts = parser.exprs().stmt();
//    assertEquals(1, stmts.size());
////    assertEquals("datasources", stmts.get(0).RESOURCE().getText());
//  }
//
//  @Test
//  public void testParseMultipleStatements() throws IOException {
//    // given
//    MetatronInterpreter interpreter = new MetatronInterpreter(new Properties());
//
//    // when
//    MetatronParser parser = interpreter.parseMetatronExpr("describe datasources;describe sales;");
//
//    // then
//    List<MetatronParser.StmtContext> stmts = parser.exprs().stmt();
//    assertEquals(2, stmts.size());
////    assertEquals("datasources", stmts.get(0).RESOURCE().getText());
////    assertEquals("sales", stmts.get(1).RESOURCE().getText());
//  }
//
//  @Test
//  public void testInvalidGrammar() throws IOException {
//    // given
//    MetatronInterpreter interpreter = new MetatronInterpreter(new Properties());
//
//    // when
//    MetatronParser parser = interpreter.parseMetatronExpr("something not valid");
//
//    // then
//    List<MetatronParser.StmtContext> stmts = parser.exprs().stmt();
//    assertEquals(0, stmts.size());
//  }

    @DataPoint
    private MetatronInterpreter interpreter;

    private static final String METATRON_TEST_URL = "http://localhost:8180";
    String accessToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1NTgzNTc5NjAsInVzZXJfbmFtZSI6ImFkbWluIiwiYXV0aG9yaXRpZXMiOlsiUEVSTV9TWVNURU1fTUFOQUdFX0RBVEFTT1VSQ0UiLCJQRVJNX1NZU1RFTV9NQU5BR0VfUFJJVkFURV9XT1JLU1BBQ0UiLCJQRVJNX1NZU1RFTV9NQU5BR0VfVVNFUiIsIlBFUk1fU1lTVEVNX01BTkFHRV9TWVNURU0iLCJfX1BFUk1JU1NJT05fTUFOQUdFUiIsIl9fQURNSU4iLCJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9WSUVXX1dPUktTUEFDRSIsIl9fREFUQV9NQU5BR0VSIiwiUEVSTV9TWVNURU1fTUFOQUdFX01FVEFEQVRBIiwiUEVSTV9TWVNURU1fTUFOQUdFX1dPUktTUEFDRSIsIl9fUFJJVkFURV9VU0VSIl0sImp0aSI6ImQ1OTliYmI0LTE3NGEtNDFiYy04YjA5LTdkMWE1M2JkYTM0YSIsImNsaWVudF9pZCI6InBvbGFyaXNfY2xpZW50Iiwic2NvcGUiOlsid3JpdGUiXX0.ncX7HjWoN7V8xExMqlmIlAR9ceQ9GSiROkSdZZyN1to";

    @Before
    public void setUp() throws Exception {

        final Properties props = new Properties();
        props.put(MetatronInterpreter.METATRON_URL, METATRON_TEST_URL);
        interpreter = new MetatronInterpreter(props);
        interpreter.open();

        interpreter.getClient().setAccessToken(accessToken);
    }

    @After
    public void tearDown() throws InterpreterException {
        interpreter.close();
    }


    @Test
    public void testShowDatasources() {

        final InterpreterContext ctx = buildContext("testShowDatasources");

//        InterpreterResult res = interpreter.interpret("show datasources", ctx);
        InterpreterResult res = interpreter.interpret("show datasources", null);

        assertEquals(InterpreterResult.Code.SUCCESS, res.code());
        String abc = res.message().get(0).getData();
        assertEquals("id\tname\ttype\tengine\tdescription\nds-gis-37\tsales\tsales_geo\tSales data (2011~2014)", res.message().get(0).getData());

    }

    @Test
    public void testShowDatasourceDetail() {

        final InterpreterContext ctx = buildContext("testShowDatasourceDetail");

//        InterpreterResult res = interpreter.interpret("show datasources", ctx);
        InterpreterResult res = interpreter.interpret("show sales", null);

        assertEquals(InterpreterResult.Code.SUCCESS, res.code());
        String abc = res.message().get(0).getData();
        assertEquals("id\tname\ttype\tengine\tdescription\nds-gis-37\tsales\tsales_geo\tSales data (2011~2014)", res.message().get(0).getData());

    }

    @Test
    public void testGetData() {

        final InterpreterContext ctx = buildContext("testGetData");

        InterpreterResult res = interpreter.interpret("datasource=sales Category=Furniture limit=10 Category Sales", ctx);

        assertEquals(InterpreterResult.Code.SUCCESS, res.code());

    }

    @Test
    public void testSQLQuery() {

        final InterpreterContext ctx = buildContext("testSQLQuery");

        InterpreterResult res = interpreter.interpret("select * from druid.sales", null);

        assertEquals(InterpreterResult.Code.SUCCESS, res.code());

    }

    @Test
    public void testComment() {

        final InterpreterContext ctx = buildContext("testComment");

        InterpreterResult res = interpreter.interpret(" #this is test\nshow datasources", null);

        assertEquals(InterpreterResult.Code.SUCCESS, res.code());

    }

    private InterpreterContext buildContext(String noteAndParagraphId) {
        return InterpreterContext.builder()
                .setNoteId(noteAndParagraphId)
                .setParagraphId(noteAndParagraphId)
                .setAngularObjectRegistry(new AngularObjectRegistry("metatron", null))
                .build();
    }
}
