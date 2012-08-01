/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nokia.dempsy.router.microshard;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.invm.LocalClusterSessionFactory;
import com.nokia.dempsy.config.ClusterId;

public class MicroShardManagerTest
{
   private ClusterInfoSession session;
   
   @Before
   public void init()
   {
      session = new LocalClusterSessionFactory().createSession();
   }
   
   @After
   public void stop()
   {
      if(session != null)
      {
         session.stop();
      }
   }
   
   @Test
   public void testLeaderElectionSingleNode() throws ClusterInfoException
   {
      MicroShardManager m = new MicroShardManager(session, new ClusterId("Test", "Test"));
      Assert.assertTrue(m.isLeader());
   }

   @Test
   public void testLeaderElectionMultipleNode() throws ClusterInfoException
   {
      MicroShardManager m1 = new MicroShardManager(session, new ClusterId("Test", "Test"));
      Assert.assertTrue(m1.isLeader());
      MicroShardManager m2 = new MicroShardManager(session, new ClusterId("Test", "Test"));
      Assert.assertFalse(m2.isLeader());
   }

   @Test
   public void testLeaderElectionMultipleNodeFailures() throws ClusterInfoException
   {
      MicroShardManager m1 = new MicroShardManager(session, new ClusterId("Test", "Test"));
      Assert.assertTrue(m1.isLeader());
      MicroShardManager m2 = new MicroShardManager(session, new ClusterId("Test", "Test"));
      Assert.assertFalse(m2.isLeader());
      MicroShardManager m3 = new MicroShardManager(session, new ClusterId("Test", "Test"));
      Assert.assertFalse(m3.isLeader());
      m2.stop();
      Assert.assertTrue(m1.isLeader());
      Assert.assertFalse(m3.isLeader());
      m2 = new MicroShardManager(session, new ClusterId("Test", "Test"));
      Assert.assertFalse(m2.isLeader());
      m1.stop();
      Assert.assertFalse(m2.isLeader());
      Assert.assertTrue(m3.isLeader());
   }

}
