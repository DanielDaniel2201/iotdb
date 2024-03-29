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
package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.HashMap;
import java.util.Map;

public class IoTDBDescriptorTest {
  private final String confPath = System.getProperty(IoTDBConstant.IOTDB_CONF, null);

  @BeforeClass
  public static void init() {
    URL.setURLStreamHandlerFactory(
        new ConfigurableStreamHandlerFactory(
            "classpath",
            new URLStreamHandler() {
              @Override
              protected URLConnection openConnection(URL u) {
                // Well ... we actually don't even need to do anything ... just need to provide a
                // handler...
                throw new RuntimeException("This shouldn't have been called");
              }
            }));
  }

  @After
  public void clear() {
    if (confPath != null) {
      System.setProperty(IoTDBConstant.IOTDB_CONF, confPath);
    } else {
      System.clearProperty(IoTDBConstant.IOTDB_CONF);
    }
  }

  @Test
  public void testConfigURLWithFileProtocol() {
    IoTDBDescriptor desc = IoTDBDescriptor.getInstance();
    String pathString = "file:/usr/local/bin";

    System.setProperty(IoTDBConstant.IOTDB_CONF, pathString);
    URL confURL = desc.getPropsUrl(IoTDBConfig.CONFIG_NAME);
    Assert.assertTrue(confURL.toString().startsWith(pathString));
  }

  @Test
  public void testConfigURLWithClasspathProtocol() {
    IoTDBDescriptor desc = IoTDBDescriptor.getInstance();

    String pathString = "classpath:/root/path";
    System.setProperty(IoTDBConstant.IOTDB_CONF, pathString);
    URL confURL = desc.getPropsUrl(IoTDBConfig.CONFIG_NAME);
    Assert.assertTrue(confURL.toString().startsWith(pathString));
  }

  @Test
  public void testConfigURLWithPlainFilePath() {
    IoTDBDescriptor desc = IoTDBDescriptor.getInstance();
    URL path = IoTDBConfig.class.getResource("/" + IoTDBConfig.CONFIG_NAME);
    // filePath is a plain path string
    String filePath = path.getFile();
    System.setProperty(IoTDBConstant.IOTDB_CONF, filePath);
    URL confURL = desc.getPropsUrl(IoTDBConfig.CONFIG_NAME);
    Assert.assertEquals(confURL.toString(), path.toString());
  }

  static class ConfigurableStreamHandlerFactory implements URLStreamHandlerFactory {
    private final Map<String, URLStreamHandler> protocolHandlers;

    public ConfigurableStreamHandlerFactory(String protocol, URLStreamHandler urlHandler) {
      protocolHandlers = new HashMap<>();
      addHandler(protocol, urlHandler);
    }

    public void addHandler(String protocol, URLStreamHandler urlHandler) {
      protocolHandlers.put(protocol, urlHandler);
    }

    public URLStreamHandler createURLStreamHandler(String protocol) {
      return protocolHandlers.get(protocol);
    }
  }
}
