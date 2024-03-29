/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.web.grafana.interceptor;

import org.osgi.service.component.annotations.Component;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Component
public class LoginInterceptor implements HandlerInterceptor {

  @Value("${spring.datasource.username}")
  private String username;

  @Value("${spring.datasource.password}")
  private String password;

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
      throws Exception {
    String authorization = request.getHeader("authorization");
    if (authorization != null) {
      byte[] decodedBytes =
          Base64.getDecoder()
              .decode(authorization.replace("Basic ", "").getBytes(StandardCharsets.UTF_8));
      authorization = new String(decodedBytes);
      String[] authorizationHeader = authorization.split(":");
      if (authorizationHeader.length != 2
          || !authorizationHeader[0].equals(username)
          || !authorizationHeader[1].equals(password)) {
        response.sendError(HttpStatus.UNAUTHORIZED.value(), "Username or password incorrect");
        return false;
      }
    } else {
      response.sendError(HttpStatus.UNAUTHORIZED.value(), "Username or password incorrect");
      return false;
    }
    return true;
  }
}
