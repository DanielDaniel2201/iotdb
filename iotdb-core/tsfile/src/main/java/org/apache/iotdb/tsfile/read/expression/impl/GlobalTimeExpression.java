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

package org.apache.iotdb.tsfile.read.expression.impl;

import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GlobalTimeExpression implements IUnaryExpression, Serializable {

  private static final long serialVersionUID = 1146132942359113670L;
  private Filter filter;

  public GlobalTimeExpression(Filter filter) {
    this.filter = filter;
  }

  @Override
  public Filter getFilter() {
    return filter;
  }

  @Override
  public void setFilter(Filter filter) {
    this.filter = filter;
  }

  @Override
  public ExpressionType getType() {
    return ExpressionType.GLOBAL_TIME;
  }

  @Override
  public IExpression clone() {
    return new GlobalTimeExpression(filter.copy());
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    try {
      ReadWriteIOUtils.write((byte) getType().ordinal(), byteBuffer);
      filter.serialize(byteBuffer);
    } catch (IOException e) {
      // ignored
    }
  }

  public static GlobalTimeExpression deserialize(ByteBuffer byteBuffer) {
    return new GlobalTimeExpression(Filter.deserialize(byteBuffer));
  }

  @Override
  public String toString() {
    return "[" + this.filter + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GlobalTimeExpression that = (GlobalTimeExpression) o;
    return Objects.equals(toString(), that.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(toString());
  }
}
