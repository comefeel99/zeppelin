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
package org.apache.zeppelin.notebook.ipynb;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.markdown.Markdown;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.eclipse.jgit.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * iPython notebook format
 */
public class Ipynb {
  Logger logger = LoggerFactory.getLogger(Ipynb.class);
  public List<Cell> cells;

  public int nbformat;
  public int nbformat_minor;

  public Map<String, Object> metadata;

  public static Note read(File ipynbFile, String mdInterpreterName, String codeInterpreterName)
      throws IOException {
    Gson gson = new Gson();
    Markdown md = new Markdown(new Properties());

    Ipynb nb = gson.fromJson(FileUtils.readFileToString(ipynbFile), Ipynb.class);
    Note note = new Note(null, null, null, null);

    for (Cell cell : nb.cells) {
      Paragraph p = note.addParagraph();

      String interpreterPrefix = "";
      String text = "";

      if (cell.source != null) {
        text = StringUtils.join(cell.source, "");
      }

      if (Cell.CellType.markdown == cell.cell_type) {
        interpreterPrefix = "%" + mdInterpreterName + "\n";
        InterpreterResult result = md.interpret(text, null);
        p.setReturn(result, null);
        p.getConfig().put("editorHide", true);
      } else if (Cell.CellType.code == cell.cell_type) {
        interpreterPrefix = "%" + codeInterpreterName + "\n";

        if (cell.outputs != null) {
          // handle only last output at the moment
          Output output = cell.outputs.get(cell.outputs.size() - 1);
          if (output.text != null) {
            p.setReturn(new InterpreterResult(
                InterpreterResult.Code.SUCCESS,
                InterpreterResult.Type.TEXT,
                StringUtils.join(output.text, "")), null);
          } else if (output.data != null) {
            if (output.data.get("text/html") != null) {
              p.setReturn(new InterpreterResult(
                  InterpreterResult.Code.SUCCESS,
                  InterpreterResult.Type.HTML,
                  StringUtils.join((ArrayList) output.data.get("text/plain"), "")), null);
            } else if (output.data.get("text/plain") != null) {
              p.setReturn(new InterpreterResult(
                  InterpreterResult.Code.SUCCESS,
                  InterpreterResult.Type.TEXT,
                  StringUtils.join((ArrayList) output.data.get("text/plain"), "")), null);
            } else if (output.data.get("image/png") != null) {
              String imgString = ((String) output.data.get("image/png")).replaceAll("\n", "");
              p.setReturn(new InterpreterResult(
                  InterpreterResult.Code.SUCCESS,
                  InterpreterResult.Type.IMG,
                  imgString), null);
            } else {
              // supported output data type not found
            }
          }
        }
      }

      p.setText(interpreterPrefix + text);
    }

    return note;
  }



  public static void main(String [] args) throws IOException {
    File nbFile = new File("/Users/moon/Projects/jupyter/SVC_demo_sql.ipynb");
    Note note = Ipynb.read(nbFile, "md", "python");
    Gson gson = new Gson();
    String jsonNote = gson.toJson(note);

    new File(nbFile.getParent(), note.id()).mkdir();

    FileUtils.writeStringToFile(
        new File(nbFile.getParent() + "/" + note.id(), "note.json"),
        jsonNote);

  }
}
