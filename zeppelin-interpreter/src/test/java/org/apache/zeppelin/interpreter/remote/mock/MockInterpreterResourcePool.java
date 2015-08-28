package org.apache.zeppelin.interpreter.remote.mock;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.AngularObjectWatcher;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.Interpreter.FormType;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.resource.ResourcePool;

import com.google.gson.Gson;

public class MockInterpreterResourcePool extends Interpreter {
  static {
    Interpreter.register(
        "pool",
        "pool",
        MockInterpreterResourcePool.class.getName(),
        new InterpreterPropertyBuilder()
            .add("p1", "v1", "property1").build());

  }


  public MockInterpreterResourcePool(Properties property) {
    super(property);
  }

  @Override
  public void open() {
  }

  @Override
  public void close() {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    String[] stmt = st.split(" ");
    String cmd = stmt[0];
    String name = null;
    if (stmt.length >= 2) {
      name = stmt[1];
    }
    String value = null;
    if (stmt.length >= 3) {
      value = stmt[2];
    }

    String value2 = null;
    if (stmt.length >= 4) {
      value2 = stmt[3];
    }

    ResourcePool pool = context.getResourcePool();
    Object o = null;

    if (cmd.equals("put")) {
      pool.put(name, value);
    } else if (cmd.equalsIgnoreCase("get")) {
      o = pool.get(name, value);
    } else if (cmd.equalsIgnoreCase("search")) {
      o = pool.search(name, value);
    } else if (cmd.equals("remove")) {
      pool.remove(name);
    }

    try {
      Thread.sleep(500); // wait for watcher executed
    } catch (InterruptedException e) {
    }

    Gson gson = new Gson();
    return new InterpreterResult(Code.SUCCESS, gson.toJson(o));
  }

  @Override
  public void cancel(InterpreterContext context) {
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return null;
  }
}