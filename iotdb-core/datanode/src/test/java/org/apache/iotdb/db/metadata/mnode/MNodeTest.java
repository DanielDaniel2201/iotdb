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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.commons.schema.node.utils.IMNodeFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MNodeTest {

  private final IMNodeFactory<IMemMNode> nodeFactory =
      MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory();

  @Test
  public void testAddChild() {
    IMemMNode rootNode = nodeFactory.createInternalMNode(null, "root");

    IMemMNode speedNode =
        rootNode
            .addChild(nodeFactory.createInternalMNode(null, "sg1"))
            .addChild(nodeFactory.createInternalMNode(null, "a"))
            .addChild(nodeFactory.createInternalMNode(null, "b"))
            .addChild(nodeFactory.createInternalMNode(null, "c"))
            .addChild(nodeFactory.createInternalMNode(null, "d"))
            .addChild(nodeFactory.createInternalMNode(null, "device"))
            .addChild(nodeFactory.createInternalMNode(null, "speed"));
    assertEquals("root.sg1.a.b.c.d.device.speed", speedNode.getFullPath());

    IMemMNode temperatureNode =
        rootNode
            .getChild("sg1")
            .addChild(nodeFactory.createInternalMNode(null, "aa"))
            .addChild(nodeFactory.createInternalMNode(null, "bb"))
            .addChild(nodeFactory.createInternalMNode(null, "cc"))
            .addChild(nodeFactory.createInternalMNode(null, "dd"))
            .addChild(nodeFactory.createInternalMNode(null, "device11"))
            .addChild(nodeFactory.createInternalMNode(null, "temperature"));
    assertEquals("root.sg1.aa.bb.cc.dd.device11.temperature", temperatureNode.getFullPath());
  }
}
