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

package com.nokia.dempsy.cluster;

import static com.nokia.dempsy.TestUtils.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.nokia.dempsy.TestUtils.Condition;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.mpcluster.zookeeper.ZookeeperTestServer.InitZookeeperServerBean;

public class TestAllClusterImpls
{
//   String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-LocalActx.xml", "testDempsy/ClusterInfo-ZookeeperActx.xml" };
//   String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-LocalActx.xml" };
   String[] clusterManagers = new String[]{ "testDempsy/ClusterInfo-ZookeeperActx.xml" };
   
   private List<ClusterInfoSession> sessionsToClose = new ArrayList<ClusterInfoSession>();

   public void cleanup()
   {
      for(ClusterInfoSession session : sessionsToClose)
         session.stop();
      sessionsToClose.clear();
   }
   
   private static InitZookeeperServerBean zkServer = null;

   @BeforeClass
   public static void setupZookeeperSystemVars() throws IOException
   {
      zkServer = new InitZookeeperServerBean();
   }
   
   @AfterClass
   public static void shutdownZookeeper()
   {
      zkServer.stop();
   }
   
   private static interface Checker
   {
      public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable;
   }
   
   private <T,N> void runAllCombinations(Checker checker) throws Throwable
   {
      for (String clusterManager : clusterManagers)
      {
         ClassPathXmlApplicationContext actx = new ClassPathXmlApplicationContext(clusterManager);
         actx.registerShutdownHook();

         ClusterInfoSessionFactory factory = (ClusterInfoSessionFactory)actx.getBean("clusterSessionFactory");

         if (checker != null)
            checker.check("pass for:" + clusterManager,factory);

         actx.stop();
         actx.destroy();
      }
   }
   
   private static ClusterInfoLeaf<String> createApplicationLevel(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      ClusterInfoLeaf<?> root = session.getRoot();
      @SuppressWarnings("unchecked")
      ClusterInfoLeaf<String> appHandle = (ClusterInfoLeaf<String>)root.createNewChild(cid.getApplicationName(), false);
      return appHandle;
   }
   
   private static ClusterInfoLeaf<String> createClusterLevel(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      ClusterInfoLeaf<String> appHandle = createApplicationLevel(cid,session);
      
      @SuppressWarnings("unchecked")
      ClusterInfoLeaf<String> clusterHandle = (ClusterInfoLeaf<String>)appHandle.createNewChild(cid.getMpClusterName(), false);
      return clusterHandle;
   }
   
   @SuppressWarnings("unchecked")
   private static ClusterInfoLeaf<String> getClusterLeaf(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      return (ClusterInfoLeaf<String>) session.getRoot().getSubLeaf(cid.getApplicationName()).getSubLeaf(cid.getMpClusterName());
   }
   
   @Test
   public void testMpClusterFromFactory() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app1","test-cluster");
            
            ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            ClusterInfoLeaf<String> clusterHandle = createClusterLevel(cid, session);
            
            assertNotNull(pass,clusterHandle);
            
            // there should be nothing currently registered
            Collection<ClusterInfoLeaf<?>> slots = clusterHandle.getSubLeaves();
            assertNotNull(pass,slots);
            assertEquals(pass,0,slots.size());
            
            assertNull(pass,clusterHandle.getData());
            
            session.stop();
         }
         
      });
   }
   
   @Test
   public void testSimpleClusterLevelData() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app2","test-cluster");
            
            ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            ClusterInfoLeaf<String> clusterHandle = createClusterLevel(cid,session);
            assertNotNull(pass,clusterHandle);

            String data = "HelloThere";
            clusterHandle.setData(data);
            String cdata = clusterHandle.getData();
            assertEquals(pass,data,cdata);
            
            session.stop();
         }
         
      });
   }
   
   @Test
   public void testSimpleClusterLevelDataThroughApplication() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app3","testSimpleClusterLevelDataThroughApplication");
            
            ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            ClusterInfoLeaf<String> mpapp = createApplicationLevel(cid,session);
            @SuppressWarnings("unchecked")
            ClusterInfoLeaf<String> clusterHandle = (ClusterInfoLeaf<String>)mpapp.createNewChild(cid.getMpClusterName(),false);
            assertNotNull(pass,clusterHandle);
            Collection<ClusterInfoLeaf<?>> clusterHandles = mpapp.getSubLeaves();
            assertNotNull(pass,clusterHandles);
            assertEquals(1,clusterHandles.size());
            assertEquals(clusterHandle,clusterHandles.iterator().next());

            String data = "HelloThere";
            clusterHandle.setData(data);
            String cdata = clusterHandle.getData();
            assertEquals(pass,data,cdata);
            
            session.stop();

         }
         
      });
   }

   
   @Test
   public void testSimpleJoinTest() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app4","test-cluster");

            ClusterInfoSession session = factory.createSession();
            assertNotNull(pass,session);
            sessionsToClose.add(session);
            ClusterInfoLeaf<String> cluster = createClusterLevel(cid,session);
            assertNotNull(pass,cluster);

            @SuppressWarnings("unchecked")
            ClusterInfoLeaf<String> node = (ClusterInfoLeaf<String>)cluster.createNewChild("Test",true);
            Assert.assertEquals(1, cluster.getSubLeaves().size());

            String data = "testSimpleJoinTest-data";
            node.setData(data);
            Assert.assertEquals(pass,data, node.getData());
            
            node.leaveParent();
            
            // wait for no more than ten seconds
            for (long timeToEnd = System.currentTimeMillis() + 10000; timeToEnd > System.currentTimeMillis();)
            {
               Thread.sleep(10);
               if (cluster.getSubLeaves().size() == 0)
                  break;
            }
            Assert.assertEquals(pass,0, cluster.getSubLeaves().size());
            
            session.stop();
         }
      });
   }
   
   private class TestWatcher implements ClusterInfoLeafWatcher
   {
      public boolean recdUpdate = false;
      public CountDownLatch latch;
      
      public TestWatcher(int count) { latch = new CountDownLatch(count); }
      
      @Override
      public void process() 
      {
         recdUpdate = true;
         latch.countDown();
      }
       
   }
   
   @Test
   public void testSimpleWatcherData() throws Throwable
   {
      runAllCombinations(new Checker()
      {
         @SuppressWarnings("unchecked")
         @Override
         public void check(String pass, ClusterInfoSessionFactory factory) throws Throwable
         {
            ClusterId cid = new ClusterId("test-app5","test-cluster");
            
            ClusterInfoSession mainSession = factory.createSession();
            assertNotNull(pass,mainSession);            
            sessionsToClose.add(mainSession);
            
            TestWatcher mainAppWatcher = new TestWatcher(1);
            ClusterInfoLeaf<String> mpapp = createApplicationLevel(cid,mainSession);
            mpapp.addWatcher(mainAppWatcher);
            assertEquals(0,mpapp.getSubLeaves().size());
            
            ClusterInfoSession otherSession = factory.createSession();
            assertNotNull(pass,otherSession);
            sessionsToClose.add(otherSession);
            
            assertFalse(pass,mainSession.equals(otherSession));
            
            ClusterInfoLeaf<String> clusterHandle = (ClusterInfoLeaf<String>)mpapp.createNewChild(cid.getMpClusterName(),false);
            assertNotNull(pass,clusterHandle);
            
            assertTrue(poll(5000, mainAppWatcher, new Condition<TestWatcher>() {
               public boolean conditionMet(TestWatcher o) { return o.recdUpdate;  }
            }));
            
            mainAppWatcher.recdUpdate = false;

            ClusterInfoLeaf<String> otherCluster = getClusterLeaf(cid,otherSession);
            assertNotNull(pass,otherCluster);

            // in case the mainAppWatcher wrongly receives an update, let's give it a chance. 
            Thread.sleep(500);
            assertFalse(mainAppWatcher.recdUpdate);

            TestWatcher mainWatcher = new TestWatcher(1);
            clusterHandle.addWatcher(mainWatcher);
            
            TestWatcher otherWatcher = new TestWatcher(1);
            otherCluster.addWatcher(otherWatcher);
            
            String data = "HelloThere";
            clusterHandle.setData(data);
            
            // this should have affected otherWatcher 
            assertTrue(pass,otherWatcher.latch.await(5, TimeUnit.SECONDS));
            assertTrue(pass,otherWatcher.recdUpdate);
            
            // we do expect an update here also
            assertTrue(pass,mainWatcher.latch.await(5,TimeUnit.SECONDS));
            assertTrue(pass,mainWatcher.recdUpdate);
            
            // now check access through both sessions and we should see the update.
            String cdata = clusterHandle.getData();
            assertEquals(pass,data,cdata);
            
            cdata = otherCluster.getData();
            assertEquals(pass,data,cdata);
            
            mainSession.stop();
            otherSession.stop();
            
            // in case the mainAppWatcher wrongly receives an update, let's give it a chance. 
            Thread.sleep(500);
            assertFalse(mainAppWatcher.recdUpdate);
         }
         
      });
   }
   
   private ClusterInfoSession session1;
   private ClusterInfoSession session2;
   private volatile boolean thread1Passed = false;
   private volatile boolean thread2Passed = false;
   private CountDownLatch latch = new CountDownLatch(1);

   @Test
   public void testConsumerCluster() throws Throwable
   {
      System.out.println("Testing Consumer Cluster");
      
      runAllCombinations(new Checker()
      {
         @Override
         public void check(String pass,ClusterInfoSessionFactory factory) throws Throwable
         {
            final ClusterId cid = new ClusterId("test-app6","test-cluster");
            session1 = factory.createSession();
            createClusterLevel(cid, session1);
            sessionsToClose.add(session1);
            session2 = factory.createSession();
            sessionsToClose.add(session2);
            
            Thread t1 = new Thread(new Runnable()
            {
               @Override
               public void run()
               {
                  System.out.println("Consumer setting data");
                  try
                  {
                     ClusterInfoLeaf<String> consumer = getClusterLeaf(cid,session1);
                     consumer.setData("Test");
                     thread1Passed = true;
                     latch.countDown();
                  }
                  catch(Exception e)
                  {
                     e.printStackTrace();
                  }

               }
            });
            t1.start();
            Thread t2 = new Thread(new Runnable()
            {
               @Override
               public void run()
               {
                  try
                  {
                     latch.await(10, TimeUnit.SECONDS);
                     ClusterInfoLeaf<String> producer = getClusterLeaf(cid,session2);
                     
                     String data = producer.getData();
                     if ("Test".equals(data))
                        thread2Passed = true;
                  }
                  catch(Exception e)
                  {
                     e.printStackTrace();
                  }
               }
            });
            t2.start();

            t1.join(30000);
            t2.join(30000); // use a timeout just in case. A failure should be indicated below if the thread never finishes.

            assertTrue(pass,thread1Passed);
            assertTrue(pass,thread2Passed);
            
            session2.stop();
            session1.stop();
         }
      });
   }
}
