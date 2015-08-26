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
package org.apache.zeppelin.interpreter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * ZeppelinApplicationOutput.
 * Output is text data.
 */
public class InterpreterOutput extends OutputStream {

  private final List<Object> outList = new LinkedList<Object>();

  public InterpreterOutput() {
    clear();
  }

  public void clear() {
    synchronized (outList) {
      outList.clear();
    }
  }


  @Override
  public void write(int b) throws IOException {
    synchronized (outList) {
      outList.add(b);
    }
  }

  @Override
  public void write(byte [] b) throws IOException {
    synchronized (outList) {
      outList.add(b);
    }
  }

  @Override
  public void write(byte [] b, int off, int len) throws IOException {
    synchronized (outList) {
      byte[] buf = new byte[len];
      System.arraycopy(b, off, buf, 0, len);
      outList.add(buf);
    }
  }

  /**
   * In dev mode, it monitors file and update ZeppelinServer
   * @param file
   */
  public void write(File file) {
    outList.add(file);
  }

  public byte[] toByteArray() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    synchronized (outList) {
      for (Object o : outList) {
        if (o instanceof File) {
          File f = (File) o;
          FileInputStream fin = new FileInputStream(f);
          copyStream(fin, out);
          fin.close();
        } else if (o instanceof byte[]) {
          out.write((byte[]) o);
        } else if (o instanceof Integer) {
          out.write((int) o);
        } else {
          // can not handle the object
        }
      }
    }
    out.close();
    return out.toByteArray();
  }

  private void copyStream(InputStream in, OutputStream out) throws IOException {
    int bufferSize = 8192;
    byte[] buffer = new byte[bufferSize];

    while (true) {
      int bytesRead = in.read(buffer);
      if (bytesRead == -1) {
        break;
      } else {
        out.write(buffer, 0, bytesRead);
      }
    }
  }

}
