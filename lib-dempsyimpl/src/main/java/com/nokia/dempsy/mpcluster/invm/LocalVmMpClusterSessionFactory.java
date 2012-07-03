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

package com.nokia.dempsy.mpcluster.invm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.mpcluster.MpApplication;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterNode;
import com.nokia.dempsy.mpcluster.MpClusterSession;
import com.nokia.dempsy.mpcluster.MpClusterSessionFactory;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;

/**
 * This class is for running all cluster management from within the same vm, and 
 * for the same vm. It's meant to mimic the Zookeeper implementation such that 
 * callbacks are not made to watchers registered to sessions through wich changes
 * are made.
 */
public class LocalVmMpClusterSessionFactory<T,N> implements MpClusterSessionFactory<T, N>
{
   protected ConcurrentHashMap<ClusterId,ConcurrentHashMap<String, MpClusterSlot<N>>> nodes = new ConcurrentHashMap<ClusterId, ConcurrentHashMap<String, MpClusterSlot<N>>>();
   protected ConcurrentHashMap<String,MpApplication<T, N>> apps = new ConcurrentHashMap<String,MpApplication<T, N>>();
   protected ConcurrentHashMap<ClusterId,AtomicReference<T>> clusterData = new ConcurrentHashMap<ClusterId, AtomicReference<T>>();
   private ConcurrentHashMap<ClusterId, MpCluster<T, N>> cache = new ConcurrentHashMap<ClusterId, MpCluster<T, N>>();

   private static Logger logger = LoggerFactory.getLogger(LocalVmMpClusterSessionFactory.class);

   List<LocalVmMpSession> currentSessions = new CopyOnWriteArrayList<LocalVmMpSession>();
   
   @Override
   public MpClusterSession<T, N> createSession() throws MpClusterException
   {
      LocalVmMpSession ret = new LocalVmMpSession();
      currentSessions.add(ret);
      return ret;
   }
   
   public class LocalVmMpSession implements MpClusterSession<T, N>
   {
      private volatile boolean isStopped = false;
      
      @Override
      public MpCluster<T, N> getCluster(ClusterId clusterId) throws MpClusterException
      {
         if (!isStopped)
         {
            LocalVmMpCluster cluster = new LocalVmMpCluster(clusterId); // potential new one
            LocalVmMpCluster ret = (LocalVmMpCluster)cache.putIfAbsent(clusterId, cluster); // ret == null if this was added, otherwise ret is what was already in the map
            if (ret == null)
            {
               // then we're adding a new cluster.
               callUpdateWatchersForApplication(clusterId.getApplicationName());
            }
            return ret == null ? cluster : ret; // if cluster was newly added (ret == null) then return it. Otherwise return what was already cached.
         }
         
         throw new MpClusterException("getCluster() with a cluster id of " + SafeString.valueOf(clusterId) +
               " was called on a stopped session.");
      }
      
      public void stop()
      {
         isStopped = true;
         
         for (MpCluster<T, N> cluster : cache.values())
            ((LocalVmMpCluster)cluster).stop();
      }
      
      public class LocalVmMpApplication implements MpApplication<T,N>
      {
         private List<MpClusterWatcher> watchers = new ArrayList<MpClusterWatcher>();

         @Override
         public Collection<MpCluster<T, N>> getActiveClusters()
         {
            Set<MpCluster<T, N>> ret = new HashSet<MpCluster<T, N>>();
            ret.addAll(cache.values());
            return ret;
         }

         @Override
         public synchronized void addWatcher(MpClusterWatcher watch)
         {
            if(!watchers.contains(watch))
               watchers.add(watch);
         }
      }
      
      public MpApplication<T, N> getApplication(String applicationId) throws MpClusterException
      {
         if (!isStopped)
         {
            LocalVmMpApplication app = new LocalVmMpApplication(); // potential new one
            LocalVmMpApplication ret = (LocalVmMpApplication)apps.putIfAbsent(applicationId, app); // ret == null if this was added, otherwise ret is what was already in the map
            return ret == null ? app : ret; // if cluster was newly added (ret == null) then return it. Otherwise return what was already cached.
         }
         
         throw new MpClusterException("getApplication() with a application id of " + applicationId +
               " was called on a stopped session.");
      }

      
      public class LocalVmMpCluster implements MpCluster<T, N>
      {
         private List<MpClusterWatcher> watchers = new ArrayList<MpClusterWatcher>();
         private ClusterId clusterId;
         private Object processLock = new Object();
         
         private LocalVmMpCluster(ClusterId clusterId) 
         {
            this.clusterId = clusterId;
            
            // initialize the slot info if there is none
            nodes.putIfAbsent(clusterId, new ConcurrentHashMap<String, MpClusterSlot<N>>());
         }

         @Override
         public synchronized void addWatcher(MpClusterWatcher watch)
         {
            if(!watchers.contains(watch))
               watchers.add(watch);
         }

         @Override
         public Collection<MpClusterSlot<N>> getActiveSlots() 
         {
            // this really can't be null given the constructor above
            ConcurrentHashMap<String, MpClusterSlot<N>> cur = nodes.get(clusterId);
            return cur != null ? cur.values() : null;
         }

         @Override
         public T getClusterData() 
         {
            AtomicReference<T> ret = clusterData.get(clusterId);
            return ret == null ? null : ret.get();
         }

         @Override
         public ClusterId getClusterId() { return clusterId; }
         
         private void stop()
         {
            synchronized(processLock)
            {
               watchers.clear();
            }
         }

         private class LocalVmMpClusterSlot implements MpClusterSlot<N>
         {
            private String slotName;
            private AtomicReference<N> data = new AtomicReference<N>();
            
            private LocalVmMpClusterSlot(String slotName) { this.slotName = slotName; }
            
            @Override
            public N getSlotInformation() { return data.get();  }

            @Override
            public void setSlotInformation(N info) { data.set(info); callUpdateWatchersForCluster(clusterId); }

            @Override
            public void leave()
            {
               ConcurrentHashMap<String, MpClusterSlot<N>> cursmap = nodes.get(clusterId);
               if (cursmap !=  null)
               {
                  MpClusterSlot<N> ref = cursmap.remove(slotName);
                  if (ref != null)
                     callUpdateWatchersForCluster(clusterId);
               }
            }
            
            @Override
            public String getSlotName() { return slotName; }
         } // end slot definition


         @Override
         public MpClusterSlot<N> allocateSlot(String slotName) throws MpClusterException
         {
            // This can't return null due to the constructor
            ConcurrentHashMap<String, MpClusterSlot<N>> sdmap = nodes.get(clusterId);
            
            MpClusterSlot<N> slot = new LocalVmMpClusterSlot(slotName);
            MpClusterSlot<N> tmps = sdmap.putIfAbsent(slotName, slot);
            if (tmps != null) // this indicates that there was one here already
            {
               if(logger.isDebugEnabled())
                  logger.debug("The cluster " + clusterId + " already contains the slot " + slotName);
               return null;
            }
            
            // if we got here then we added a slot. ... so update
            callUpdateWatchersForCluster(clusterId);
            
            return slot;
         }

         @Override
         public void setClusterData(T data)
         {
            AtomicReference<T> newref = new AtomicReference<T>();
            AtomicReference<T> ref = clusterData.putIfAbsent(clusterId, newref);
            if (ref == null)
               ref = newref;
            ref.set(data);
            callUpdateWatchersForCluster(clusterId);
         }

         @Override
         public Collection<MpClusterNode<N>> getActiveNodes() throws MpClusterException
         {
            // TODO Auto-generated method stub
            return null;
         }
         
      } // end cluster definition
      
      private final void callUpdateWatchersForCluster(ClusterId clusterId) { updateClusterWatchers(clusterId); }
      
      private final void callUpdateWatchersForApplication(String applicationId)
      {
         LocalVmMpSession.LocalVmMpApplication application = (LocalVmMpApplication)apps.get(applicationId);
         if (application != null)
         {
            synchronized(application)
            {
               for(MpClusterWatcher watcher: application.watchers)
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
            }
         }
      }
   } // end session definition

   protected void updateClusterWatchers(ClusterId clusterId)
   {
      LocalVmMpSession.LocalVmMpCluster cluster = (LocalVmMpSession.LocalVmMpCluster)cache.get(clusterId);
      if (cluster != null)
      {
         synchronized(cluster.processLock)
         {
            for(MpClusterWatcher watcher: cluster.watchers)
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
         }
      }
   }

}
