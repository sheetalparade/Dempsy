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

package com.nokia.dempsy.cluster.zookeeper2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoLeaf;
import com.nokia.dempsy.cluster.ClusterInfoLeafWatcher;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;

public class ZookeeperSession implements ClusterInfoSession
{
   private Logger logger = LoggerFactory.getLogger(ZookeeperSession.class);

   private ZookeeperLeaf<?> root;
   private volatile AtomicReference<ZooKeeper> zkref;
   private volatile boolean isRunning = true;
   protected long resetDelay = 500;
   protected String connectString;
   protected int sessionTimeout;
   
   protected ZookeeperSession(String connectString, int sessionTimeout) throws IOException
   {
      this.connectString = connectString;
      this.sessionTimeout = sessionTimeout;
      this.zkref = new AtomicReference<ZooKeeper>();
      ZooKeeper newZk = makeZooKeeperClient(connectString,sessionTimeout);
      if (newZk != null) setNewZookeeper(newZk);
      root = makeZookeeperLeaf(null,null,false);
   }

   @Override
   public ClusterInfoLeaf<?> getRoot()
   {
      return root;
   }

   @Override
   public void stop()
   {
      AtomicReference<ZooKeeper> curZk;
      synchronized(this)
      {
         isRunning = false;
         curZk = zkref;
         zkref = null; // this blows up any more usage
      }

      root.stop();
      try { curZk.get().close(); } catch (Throwable th) { /* let it go otherwise */ }
   }
   
   /**
    * This is defined here to be overridden in a test.
    */
   protected ZooKeeper makeZooKeeperClient(String connectString, int sessionTimeout) throws IOException
   {
      return new ZooKeeper(connectString, sessionTimeout, new Watcher()
      {
         @Override
         public void process(WatchedEvent event)
         {
            if (logger.isTraceEnabled())
               logger.trace("CALLBACK:Main Watcher:" + event);
         }
      });
   }
   
   /**
    * This is defined here to be overridden in a test.
    */
   @SuppressWarnings({"rawtypes","unchecked"})
   protected ZookeeperLeaf<?> makeZookeeperLeaf(String path, ZookeeperLeaf<?> parent, boolean isEphemeral)
   {
      return new ZookeeperLeaf(path,parent,isEphemeral);
   }
   
   private class ZookeeperLeaf<T> implements Watcher, ClusterInfoLeaf<T>
   {
      private String path;
      private ZookeeperPath zkPath;
      private ZookeeperLeaf<?> parent;
      private boolean isEphemeral;
      private Serializer<T> serializer;
      private Map<String,ZookeeperLeaf<?>> children = new HashMap<String,ZookeeperSession.ZookeeperLeaf<?>>();
      
      private CopyOnWriteArraySet<ClusterInfoLeafWatcher> watchers = new CopyOnWriteArraySet<ClusterInfoLeafWatcher>();
      private Object processLock = new Object();
      
      private volatile boolean isDirty = true; // default to true so that watcher registration can happen
      
      private T data = null;

      private ZookeeperLeaf(String path, ZookeeperLeaf<?> parent, boolean isEphemeral)
      {
         this.path = path;
         zkPath = new ZookeeperPath(parent,path);
         this.parent = parent;
         this.isEphemeral = isEphemeral;
         serializer = new JSONSerializer<T>();
      }

      @Override
      public Collection<ClusterInfoLeaf<?>> getSubLeaves() throws ClusterInfoException
      {
         Collection<ClusterInfoLeaf<?>> ret;
         synchronized(children)
         {
            if (isDirty)
               refresh();
         
            ret = new ArrayList<ClusterInfoLeaf<?>>(children.size());
            ret.addAll(children.values());
         }
         return ret;
      }

      @Override
      public ClusterInfoLeaf<?> getSubLeaf(String path) throws ClusterInfoException
      {
         synchronized(children)
         {
            if (isDirty)
               refresh();

            return children.get(path);
         }
      }

      @Override
      public ClusterInfoLeaf<?> createNewChild(String nodeName, boolean ephemeral) throws ClusterInfoException
      {
         if (isRunning)
         {
            ZooKeeper cur = zkref.get();
            try
            {
               @SuppressWarnings({"unchecked","rawtypes"})
               ZookeeperLeaf<?> ret  = new ZookeeperLeaf(nodeName,this,ephemeral);
               cur.create(ret.zkPath.path, new byte[0], Ids.OPEN_ACL_UNSAFE, ephemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT);
               synchronized(children)
               {
                  children.put(nodeName, ret);
               }
               return ret;
            }
            catch(KeeperException.NodeExistsException e)
            {
               if(logger.isDebugEnabled())
                  logger.debug("Failed to join the cluster " + path + 
                        ". Couldn't create the node within zookeeper using \"" + zkPath + "\"");
               return null;
            }
            catch(KeeperException e)
            {
               resetZookeeper(cur);
               throw new ClusterInfoException("Zookeeper failed while trying to join the cluster " + path + 
                     ". Couldn't create the node within zookeeper using \"" + zkPath + "\"",e);
            }
            catch(InterruptedException e)
            {
               resetZookeeper(cur);
               throw new ClusterInfoException("Interrupted while trying to join the cluster " + path + 
                     ". Couldn't create the node within zookeeper using \"" + zkPath + "\"",e);
            }
         }

         throw new ClusterInfoException("join called on stopped MpClusterSlot (" + zkPath + 
               ") on provided zookeeper instance.");
      }

      @Override
      public void leaveParent() throws ClusterInfoException
      {
         if (isRunning)
         {
            ZooKeeper cur = zkref.get();

            try
            {
               // this should trigger the parent to reset as dirty
               cur.delete(zkPath.path,-1);
            }
            catch(KeeperException e)
            {
               resetZookeeper(cur);
               throw new ClusterInfoException("Failed to leave. " + 
                     "Couldn't delete the node within zookeeper using \"" + zkPath + "\"",e);
            }
            catch(InterruptedException e)
            {
               resetZookeeper(cur);
               throw new ClusterInfoException("Interrupted while trying to leave the cluster." + 
                     "Couldn't delete the node within zookeeper using \"" + zkPath + "\"",e);
            }
         }
         else
            throw new ClusterInfoException("leave called on stopped MpClusterSlot (" + zkPath + 
                  ") on provided zookeeper instance.");
      }

      @Override
      public String getLeafName()
      {
         return path;
      }

      @Override
      public synchronized T getData() throws ClusterInfoException
      {
         if (isDirty)
            refresh();
         
         return data;
      }

      @Override
      public synchronized void setData(T data) throws ClusterInfoException
      {
         this.data = data;
         setInfoToPath(zkPath, data, serializer);
      }

      @Override
      public void addWatcher(ClusterInfoLeafWatcher watcher)
      {
         watchers.add(watcher);
      }

      @Override
      public void process(WatchedEvent event)
      {
         synchronized(children)
         {
            isDirty = true;
            children.clear();
         }
      }
      
      private void refresh() throws ClusterInfoException
      {
         synchronized(children)
         {
            if (isDirty)
            {
               @SuppressWarnings("unchecked")
               T readInfoFromPath = (T)readInfoFromPath(zkPath, serializer);
               this.data = readInfoFromPath;
               
               if (!isEphemeral)
               {
               }
               
               isDirty = false;
            }
         }
      }
      
      private void stop()
      {
         synchronized(children)
         {
            for (ClusterInfoLeaf<?> child : children.values())
               ((ZookeeperLeaf<?>)child).stop();
            children.clear();
         }

         watchers.clear();
         
         synchronized(processLock)
         {
            // this just holds up if process is currently running ...
            // if process isn't running then the above clear should 
            //   prevent and watcher.process calls from ever being made.
         }
      }
   }
   
   private synchronized void setNewZookeeper(ZooKeeper newZk)
   {
      if (logger.isTraceEnabled())
         logger.trace("reestablished connection to " + connectString);
      
      if (isRunning)
      {
         ZooKeeper last = zkref.getAndSet(newZk);
         if (last != null)
         {
            try { last.close(); } catch (Throwable th) {}
         }
      }
      else
      {
         // in this case with zk == null we're shutting down.
         try { newZk.close(); } catch (Throwable th) {}
      }
   }
   
   private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   private volatile boolean beingReset = false;
   
   private synchronized void resetZookeeper(ZooKeeper failedInstance)
   {
      AtomicReference<ZooKeeper> tmpZkRef = zkref;
      // if we're not shutting down (which would be indicated by tmpZkRef == null
      //   and if the failedInstance we're trying to reset is the current one, indicated by tmpZkRef.get() == failedInstance
      //   and if we're not already working on beingReset
      if (tmpZkRef != null && tmpZkRef.get() == failedInstance && !beingReset)
      {
         beingReset = true;
         scheduler.schedule(new Runnable()
         {
            @Override
            public void run()
            {
               ZooKeeper newZk = null;
               try
               {
                  newZk = makeZooKeeperClient(connectString, sessionTimeout);
               }
               catch (IOException e)
               {
                  logger.warn("Failed to reset the ZooKeeper connection to " + connectString);
                  newZk = null;
               }
               finally
               {
                  if (newZk == null && isRunning)
                     // reschedule me.
                     scheduler.schedule(this, resetDelay, TimeUnit.MILLISECONDS);
               }

               // this is true if the reset worked and we're not in the process
               // of shutting down.
               if (newZk != null && isRunning)
               {
                  // we want the setNewZookeeper and the clearing of the
                  // beingReset flag to be atomic so future failures that result
                  // in calls to resetZookeeper will either:
                  //   1) be skipped because they are for an older ZooKeeper instance.
                  //   2) be executed because they are for this new ZooKeeper instance.
                  // what we dont want is the possibility that the reset will be skipped
                  // even though the reset is called for this new ZooKeeper, but we haven't cleared
                  // the beingReset flag yet.
                  synchronized(ZookeeperSession.this)
                  {
                     setNewZookeeper(newZk);
                     beingReset = false;
                  }
                  
                  // now reset the watchers
                  root.process(null);
               }
               else if (newZk != null)
               {
                  // in this case with zk == null we're shutting down.
                  try { newZk.close(); } catch (Throwable th) {}
               }
            }
         }, resetDelay, TimeUnit.MILLISECONDS);
      }
   }

   /**
    * Helper class for calculating the path within zookeeper given the 
    */
   private static class ZookeeperPath
   {
      public static String root = "/";
      public String path;
      public boolean isRoot;
      
      public ZookeeperPath(ZookeeperLeaf<?> parent, String path)
      {
         isRoot = parent == null;
         this.path = isRoot ? null : 
            (parent.zkPath.path == null ? root : parent.zkPath.path) + path;
      }
      
      public String toString() { return path; }
   }

   private class JSONSerializer<TS> implements Serializer<TS>
   {
      ObjectMapper objectMapper;

      public JSONSerializer()
      {
         objectMapper = new ObjectMapper();
         objectMapper.enableDefaultTyping();
         objectMapper.configure(SerializationConfig.Feature.WRITE_EMPTY_JSON_ARRAYS, true);
         objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
         objectMapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, true);
      }

      @SuppressWarnings("unchecked")
      @Override
      public TS deserialize(byte[] data) throws SerializationException
      {
         ArrayList<TS> info = null;
         if(data != null)
         {
            String jsonData = new String(data);
            try
            {
               info = objectMapper.readValue(jsonData, ArrayList.class);
            }
            catch(Exception e)
            {
               throw new SerializationException("Error occured while deserializing data "+jsonData, e);
            }
         }
         return (info != null && info.size()>0)?info.get(0):null;
      }

      @Override
      public byte[] serialize(TS data) throws SerializationException 
      {
         String jsonData = null;
         if(data != null)
         {
            ArrayList<TS> arr = new ArrayList<TS>();
            arr.add(data);
            try
            {
               jsonData = objectMapper.writeValueAsString(arr);
            }
            catch(Exception e)
            {
               throw new SerializationException("Error occured during serializing class " +
                     SafeString.valueOfClass(data) + " with information "+SafeString.valueOf(data), e);
            }
         }
         return (jsonData != null)?jsonData.getBytes():null;
      }

   }
   
   private Object readInfoFromPath(ZookeeperPath path, Serializer<?> ser) throws ClusterInfoException
   {
      if (isRunning)
      {
         try
         {
            byte[] ret = zkref.get().getData(path.path, true, null);

            if (ret != null && ret.length > 0)
               return ser.deserialize(ret);
            return null;
         }
         // this is an indication that the node has disappeared since we retrieved 
         // this MpContainerClusterNode
         catch (KeeperException.NoNodeException e) { return null; }
         catch (RuntimeException e) { throw e; } 
         catch (Exception e) 
         {
            throw new ClusterInfoException("Failed to get node information for (" + path + ").",e);
         }
      }
      return null;
   }
   
   @SuppressWarnings("unchecked")
   private void setInfoToPath(ZookeeperPath path, Object info, @SuppressWarnings("rawtypes") Serializer ser) throws ClusterInfoException
   {
      if (isRunning)
      {
         try
         {
            byte[] buf = null;
            if (info != null)
               // Serialize to a byte array
               buf = ser.serialize(info);

            zkref.get().setData(path.path, buf, -1);
         }
         catch (RuntimeException e) { throw e;} 
         catch (Exception e) 
         {
            throw new ClusterInfoException("Failed to get node information for (" + path + ").",e);
         }
      }
   }


}
