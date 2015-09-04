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

package org.apache.zeppelin.rest;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.server.JsonResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;

/**
 * Helium Rest API
 *
 */
@Path("/helium")
@Produces("application/json")
@Api(value = "/helium", description = "Helium REST API")
public class HeliumRestApi {
  Logger logger = LoggerFactory.getLogger(InterpreterRestApi.class);

  public HeliumRestApi() {

  }

  /**
   * Get possible applications for result of this paragraph
   * @return
   */
  @GET
  @Path("")
  @ApiOperation(httpMethod = "GET", value = "List all interpreter setting")
  @ApiResponses(value = {@ApiResponse(code = 500, message = "When something goes wrong")})
  public Response listSettings() {
    List<InterpreterSetting> interpreterSettings = null;
    return new JsonResponse(Status.OK, "", interpreterSettings).build();
  }
}
