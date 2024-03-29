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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.exception.PathParseException;
import org.apache.iotdb.tsfile.read.common.parser.PathVisitor;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class PathTest {
  @Test
  public void testLegalPath() {
    // empty path
    Path a = new Path("", true);
    Assert.assertEquals("", a.getDevice());
    Assert.assertEquals("", a.getMeasurement());

    // empty device
    Path b = new Path("s1", true);
    Assert.assertEquals("s1", b.getMeasurement());
    Assert.assertEquals("", b.getDevice());

    // normal node
    Path c = new Path("root.sg.a", true);
    Assert.assertEquals("root.sg", c.getDevice());
    Assert.assertEquals("a", c.getMeasurement());

    // quoted node
    Path d = new Path("root.sg.`a.b`", true);
    Assert.assertEquals("root.sg", d.getDevice());
    Assert.assertEquals("`a.b`", d.getMeasurement());

    Path e = new Path("root.sg.`a.``b`", true);
    Assert.assertEquals("root.sg", e.getDevice());
    Assert.assertEquals("`a.``b`", e.getMeasurement());

    Path f = new Path("root.`sg\"`.`a.``b`", true);
    Assert.assertEquals("root.`sg\"`", f.getDevice());
    Assert.assertEquals("`a.``b`", f.getMeasurement());

    Path g = new Path("root.sg.`a.b\\\\`", true);
    Assert.assertEquals("root.sg", g.getDevice());
    Assert.assertEquals("`a.b\\\\`", g.getMeasurement());

    // quoted node of digits
    Path h = new Path("root.sg.`111`", true);
    Assert.assertEquals("root.sg", h.getDevice());
    Assert.assertEquals("`111`", h.getMeasurement());

    // quoted node of key word
    Path i = new Path("root.sg.`select`", true);
    Assert.assertEquals("root.sg", i.getDevice());
    Assert.assertEquals("select", i.getMeasurement());

    // wildcard
    Path j = new Path("root.sg.`a*b`", true);
    Assert.assertEquals("root.sg", j.getDevice());
    Assert.assertEquals("`a*b`", j.getMeasurement());

    Path k = new Path("root.sg.*", true);
    Assert.assertEquals("root.sg", k.getDevice());
    Assert.assertEquals("*", k.getMeasurement());

    Path l = new Path("root.sg.**", true);
    Assert.assertEquals("root.sg", l.getDevice());
    Assert.assertEquals("**", l.getMeasurement());

    // raw key word
    Path m = new Path("root.sg.select", true);
    Assert.assertEquals("root.sg", m.getDevice());
    Assert.assertEquals("select", m.getMeasurement());

    Path n = new Path("root.sg.device", true);
    Assert.assertEquals("root.sg", n.getDevice());
    Assert.assertEquals("device", n.getMeasurement());

    Path o = new Path("root.sg.drop_trigger", true);
    Assert.assertEquals("root.sg", o.getDevice());
    Assert.assertEquals("drop_trigger", o.getMeasurement());

    Path p = new Path("root.sg.and", true);
    Assert.assertEquals("root.sg", p.getDevice());
    Assert.assertEquals("and", p.getMeasurement());

    p = new Path("root.sg.or", true);
    Assert.assertEquals("root.sg", p.getDevice());
    Assert.assertEquals("or", p.getMeasurement());

    p = new Path("root.sg.not", true);
    Assert.assertEquals("root.sg", p.getDevice());
    Assert.assertEquals("not", p.getMeasurement());

    p = new Path("root.sg.null", true);
    Assert.assertEquals("root.sg", p.getDevice());
    Assert.assertEquals("null", p.getMeasurement());

    p = new Path("root.sg.contains", true);
    Assert.assertEquals("root.sg", p.getDevice());
    Assert.assertEquals("contains", p.getMeasurement());

    p = new Path("root.sg.`0000`", true);
    Assert.assertEquals("root.sg", p.getDevice());
    Assert.assertEquals("`0000`", p.getMeasurement());

    p = new Path("root.sg.`0e38`", true);
    Assert.assertEquals("root.sg", p.getDevice());
    Assert.assertEquals("`0e38`", p.getMeasurement());

    p = new Path("root.sg.`00.12`", true);
    Assert.assertEquals("root.sg", p.getDevice());
    Assert.assertEquals("`00.12`", p.getMeasurement());
  }

  @Test
  public void tesIllegalPath() {
    try {
      new Path("root.sg`", true);
      fail();
    } catch (PathParseException ignored) {

    }

    try {
      new Path("root.sg\na", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.select`", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      // pure digits
      new Path("root.111", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      // single ` in quoted node
      new Path("root.`a``", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      // single ` in quoted node
      new Path("root.``a`", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.a*%", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.a*b", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.0e38", true);
      fail();
    } catch (PathParseException ignored) {
    }

    try {
      new Path("root.0000", true);
      fail();
    } catch (PathParseException ignored) {
    }
  }

  @Test
  public void testRealNumber() {
    Assert.assertTrue(PathVisitor.isRealNumber("0e38"));
    Assert.assertTrue(PathVisitor.isRealNumber("0000"));
    Assert.assertTrue(PathVisitor.isRealNumber("00.12"));
  }
}
