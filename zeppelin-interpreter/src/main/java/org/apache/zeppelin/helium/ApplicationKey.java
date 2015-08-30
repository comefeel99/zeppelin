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

package org.apache.zeppelin.helium;

/**
 * Key that identifies Application
 */
public class ApplicationKey {
  String className;
  String mavenArtifact;

  /**
   * @param mavenArtifact "groupId:artifactId:version" format
   * @param className
   */
  public ApplicationKey(String mavenArtifact, String className) {
    this.mavenArtifact = mavenArtifact;
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  public String getMavenArtifact() {
    return mavenArtifact;
  }

  public boolean equals(Object o) {
    if (o instanceof ApplicationKey) {
      ApplicationKey spec = (ApplicationKey) o;
      if (mavenArtifact.equals(spec.mavenArtifact) &&
          className.equals(spec.className)) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }

  public int hashCode() {
    return (mavenArtifact + "-" + className).hashCode();
  }
}
