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

package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAlignedSeriesQueryWithMisMatchIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setMaxTsBlockLineNumber(3);
    EnvFactory.getEnv().initClusterEnvironment();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("insert into root.sg1.d1(time, s1, s2) aligned values (1, 1, true)");
      statement.execute("flush");
      statement.execute("delete timeseries root.sg1.d1.s2");
      statement.execute("insert into root.sg1.d1(time, s1, s2) aligned values (10, 10, 100)");
      statement.execute("flush");
    } catch (Exception e) {
      fail(e.getMessage());
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void selectAllDeletedColumns() {
    // data at timestamp [1,2] has been deleted and should not be kept in result
    String[] retArray = {
      "1,1.0,null", "10,10.0,100.0",
    };

    String[] columnNames = {"root.sg1.d1.s1", "root.sg1.d1.s2"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select s1, s2 from root.sg1.d1")) {

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      Map<String, Integer> map = new HashMap<>();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        map.put(resultSetMetaData.getColumnName(i), i);
      }
      assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
      int cnt = 0;
      while (resultSet.next()) {
        StringBuilder builder = new StringBuilder();
        builder.append(resultSet.getString(1));
        for (String columnName : columnNames) {
          int index = map.get(columnName);
          builder.append(",").append(resultSet.getString(index));
        }
        assertEquals(retArray[cnt], builder.toString());
        cnt++;
      }
      assertEquals(retArray.length, cnt);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
