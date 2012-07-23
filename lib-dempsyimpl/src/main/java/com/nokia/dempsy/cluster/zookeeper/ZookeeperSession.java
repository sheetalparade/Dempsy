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

package com.nokia.dempsy.cluster.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.serialization.SerializationException;
import com.nokia.dempsy.serialization.Serializer;

public class ZookeeperSession implements ClusterInfoSession
{
   private static Logger logger = LoggerFactory.getLogger(ZookeeperSession.class);

   private volatile AtomicReference<ZooKeeper> zkref;

   private volatile boolean isRunning = true;
   protected long resetDelay = 500;
   protected String connectString;
   protected int sessionTimeout;
   private Serializer<Object> serializer = new JSONSerializer<Object>();
   
   private Set<WatcherProxy> registeredWatchers = new HashSet<WatcherProxy>();
   
   protected ZookeeperSession(String connectString, int sessionTimeout) throws IOException
   {
      this.connectString = connectString;
      this.sessionTimeout = sessionTimeout;
      this.zkref = new AtomicReference<ZooKeeper>();
      ZooKeeper newZk = makeZooKeeperClient(connectString,sessionTimeout);
      if (newZk != null) setNewZookeeper(newZk);
   }

   @Override
   public boolean mkdir(String path, boolean ephemeral) throws ClusterInfoException
   {
      Object ret = callZookeeper("mkdir", path, null, ephemeral, new ZookeeperCall()
      {
         @Override
         public Object call(ZooKeeper cur, String path, WatcherProxy watcher, Object userdata) throws KeeperException, InterruptedException, SerializationException
         {
            cur.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, ((Boolean)userdata).booleanValue() ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT);
            return true;
         }
      });
      return ((Boolean)ret).booleanValue();
   }

   @Override
   public void rmdir(String path) throws ClusterInfoException
   {
      callZookeeper("rmdir", path, null, null, new ZookeeperCall()
      {
         @Override
         public Object call(ZooKeeper cur, String path, WatcherProxy watcher, Object userdata) throws KeeperException, InterruptedException, SerializationException
         {
            cur.delete(path,-1);
            return null;
         }
      });
   }

   @Override
   public boolean exists(String path, ClusterInfoWatcher watcher) throws ClusterInfoException
   {
      Object ret = callZookeeper("exists", path, watcher, null, new ZookeeperCall()
      {
         @Override
         public Object call(ZooKeeper cur, String path, WatcherProxy wp, Object userdata) throws KeeperException, InterruptedException, SerializationException
         {
            return wp == null ? 
                  (cur.exists(path,true) == null ? false : true) :
                     (cur.exists(path,wp) == null ? false : true);
         }
      });
      return ((Boolean)ret).booleanValue();
   }

   @Override
   public Object getData(String path, ClusterInfoWatcher watcher) throws ClusterInfoException
   {
      return callZookeeper("getData", path, watcher, null, new ZookeeperCall()
      {
         @Override
         public Object call(ZooKeeper cur, String path, WatcherProxy wp, Object userdata) throws KeeperException, InterruptedException, SerializationException
         {
            byte[] ret = wp == null ? 
                  cur.getData(path, true, null) :
                     cur.getData(path, wp, null);

            if (ret != null && ret.length > 0)
               return serializer.deserialize(ret);
            return null;
         }
      });
   }

   @Override
   public void setData(String path, Object info) throws ClusterInfoException
   {
      callZookeeper("mkdir", path, null, info, new ZookeeperCall()
      {
         @Override
         public Object call(ZooKeeper cur, String path, WatcherProxy watcher, Object info) throws KeeperException, InterruptedException, SerializationException
         {
            byte[] buf = null;
            if (info != null)
               // Serialize to a byte array
               buf = serializer.serialize(info);

            zkref.get().setData(path, buf, -1);
            return null;
         }
      });
   }

   @SuppressWarnings("unchecked")
   @Override
   public Collection<String> getSubdirs(String path, ClusterInfoWatcher watcher) throws ClusterInfoException
   {
      return (Collection<String>)callZookeeper("getSubdirs", path, watcher, null, new ZookeeperCall()
      {
         @Override
         public Object call(ZooKeeper cur, String path, WatcherProxy wp, Object userdata) throws KeeperException, InterruptedException, SerializationException
         {
            return wp == null ? cur.getChildren(path, true) : cur.getChildren(path, wp);
         }
      });
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

   
   private class WatcherProxy implements Watcher
   {
      private ClusterInfoWatcher watcher;
      
      private WatcherProxy(ClusterInfoWatcher watcher)
      {
         this.watcher = watcher;
      }
      
      @Override
      public void process(WatchedEvent event)
      {
         synchronized(registeredWatchers)
         {
            registeredWatchers.remove(this);
         }
         
         try
         {
            synchronized(this)
            {
               watcher.process();
            }
         }
         catch (RuntimeException rte)
         {
            logger.warn("Watcher " + SafeString.objectDescription(watcher) + 
                  " threw an exception in it's \"process\" call.",rte);
         }
      }
   }
   
   private interface ZookeeperCall
   {
      public Object call(ZooKeeper cur, String path, WatcherProxy watcher, Object userdata) throws KeeperException, InterruptedException, SerializationException;
   }
   
   private Object callZookeeper(String name, String path, ClusterInfoWatcher watcher, Object userdata, ZookeeperCall callee) throws ClusterInfoException
   {
      if (isRunning)
      {
         WatcherProxy wp = watcher != null ? new WatcherProxy(watcher) : null;
         if (wp != null)
         {
            synchronized (registeredWatchers)
            {
               registeredWatchers.add(wp);
            }
         }

         ZooKeeper cur = zkref.get();
         try
         {
            return callee.call(cur, path, wp, userdata);
         }
         catch(KeeperException.NodeExistsException e)
         {         

            if(logger.isDebugEnabled())
               logger.debug("Failed call to " + name + " at " + path);
            return false;
         }
         catch(KeeperException e)
         {
            resetZookeeper(cur);
            throw new ClusterInfoException("Zookeeper failed while trying to " + name + " at " + path,e);
         }
         catch(InterruptedException e)
         {
            resetZookeeper(cur);
            throw new ClusterInfoException("Interrupted while trying to " + name + " at " + path,e);
         }
         catch(SerializationException e)
         {
            throw new ClusterInfoException("Failed to deserialize the object durring a " + name + " call at " + path,e);
         }

      }

      throw new ClusterInfoException(name + " called on stopped ZookeeperSession.");
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
                  
                  // now notify the watchers
                  for (WatcherProxy watcher : registeredWatchers)
                     watcher.process(null);
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

}
