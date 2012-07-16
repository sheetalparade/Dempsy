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

package com.nokia.dempsy.cluster.invm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoLeaf;
import com.nokia.dempsy.cluster.ClusterInfoLeafWatcher;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.internal.util.SafeString;

/**
 * This class is for running all cluster management from within the same vm, and 
 * for the same vm. It's meant to mimic the Zookeeper implementation such that 
 * callbacks are not made to watchers registered to sessions through wich changes
 * are made.
 */
public class LocalClusterSessionFactory implements ClusterInfoSessionFactory
{
   private static Logger logger = LoggerFactory.getLogger(LocalClusterSessionFactory.class);

   private List<LocalSession> currentSessions = new CopyOnWriteArrayList<LocalSession>();
   private LocalSession.LocalLeaf<?> root = null;
   
   @Override
   public ClusterInfoSession createSession()
   {
      LocalSession ret = new LocalSession();
      currentSessions.add(ret);
      return ret;
   }
   
   public class LocalSession implements ClusterInfoSession
   {
      @SuppressWarnings({"unchecked","rawtypes"})
      private LocalSession()
      {
         synchronized (LocalClusterSessionFactory.this)
         {
            if (root == null)
               root = new LocalLeaf(null,null);
         }
      }
      
      @Override
      public ClusterInfoLeaf<?> getRoot() { return root; }
      
      @Override
      public void stop()
      {
         
      }
      
      public class LocalLeaf<T> implements ClusterInfoLeaf<T>
      {
         protected ConcurrentHashMap<String, ClusterInfoLeaf<?>> children = new ConcurrentHashMap<String, ClusterInfoLeaf<?>>();
         
         private List<ClusterInfoLeafWatcher> watchers = new ArrayList<ClusterInfoLeafWatcher>();
         private Object processLock = new Object();
         private String path;
         private AtomicReference<T> data = new AtomicReference<T>();
         private LocalLeaf<?> parent;
         
         private LocalLeaf(String path, LocalLeaf<?> rent)
         {
            this.path = path;
            this.parent = rent;
         }

         @Override
         public synchronized void addWatcher(ClusterInfoLeafWatcher watch)
         {
            if(!watchers.contains(watch))
               watchers.add(watch);
         }
         
         @Override
         public Collection<ClusterInfoLeaf<?>> getSubLeaves()
         {
            return children.values();
         }
         
         @Override
         public ClusterInfoLeaf<?> getSubLeaf(String path)
         {
            return children.get(path);
         }

         @Override
         public T getData() 
         {
            return data == null ? null : data.get();
         }
         
         @Override
         public void setData(T data)
         {
            this.data.set(data);
            callUpdateWatchersForCluster();
         }

         @Override
         public String getLeafName() { return path; }
         
         @Override
         public void leaveParent() throws ClusterInfoException
         {
            if (parent == null)
               throw new ClusterInfoException("Leave parent called on " + SafeString.objectDescription(this) + 
                     " which is at the root of the ClusterInfo tree.");
            parent.remove(getLeafName());
         }
         
         private void remove(String childPath) throws ClusterInfoException
         {
            Object child = children.remove(childPath);
            if (child == null)
               throw new ClusterInfoException("Parent ClusterInfo tree node has no child with the path " + childPath);
            callUpdateWatchersForCluster();
         }
            
         @Override
         public ClusterInfoLeaf<?> createNewChild(String path, boolean ephemeral)
         {
            // This can't return null due to the constructor
            @SuppressWarnings({"unchecked","rawtypes"})
            ClusterInfoLeaf<?> ret = new LocalLeaf(path,this);
            ClusterInfoLeaf<?> tmp = children.putIfAbsent(path,ret);
            
            if (tmp != null) // this indicates that there was one here already
            {
               if(logger.isDebugEnabled())
                  logger.debug("The leaf " + this.path + " already contains the path " + path);
               return null;
            }
            
            // if we got here then we added a slot. ... so update
            callUpdateWatchersForCluster();
            
            return ret;
         }

         private final void callUpdateWatchersForCluster()
         {
            synchronized(processLock)
            {
               if (inProcess)
               {
                  recursionAttempt = true;
                  return;
               }

               do
               {
                  recursionAttempt = false;
                  inProcess = true;

                  for(ClusterInfoLeafWatcher watcher: watchers)
                  {
                     try
                     {
                        watcher.process();
                     }
                     catch (RuntimeException e)
                     {
                        logger.error("Failed to handle process for watcher " + SafeString.objectDescription(watcher),e);
                     }
                  }
               } while (recursionAttempt);

               inProcess = false;
            }
         }
         
      } // end leaf node definition
      
      private volatile boolean inProcess = false;
      private volatile boolean recursionAttempt = false;
      
   } // end session definition

}
