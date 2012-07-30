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

package com.nokia.dempsy.cluster.microshard;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.router.RoutingStrategy;
import com.nokia.dempsy.router.RoutingStrategy.Outbound.Coordinator;
import com.nokia.dempsy.router.SlotInformation;
import com.nokia.dempsy.serialization.Serializer;

public class MicroShardStrategy implements RoutingStrategy
{
   
   private MSInbound inBound = null;
   private MicroShardClusterInformation clusterInformation;
   private ConcurrentHashMap<ClusterId, Outbound> outbounds = new ConcurrentHashMap<ClusterId, RoutingStrategy.Outbound>();

   public MicroShardStrategy(Serializer<?> serializer, Integer totalShards)
   {
      this.clusterInformation = new MicroShardClusterInformation();
      this.clusterInformation.setSerializer(serializer);
      this.clusterInformation.setTotalShards(totalShards);
   }

   public class MSInbound implements Inbound
   {
      private ClusterInfoSession cluster;
      private Collection<Class<?>> messageTypes;
      private Destination thisDestination;
      private ClusterId clusterId;
      private AtomicBoolean running = new AtomicBoolean(true);
      private SlotInformation thisInfo = null;
      private CopyOnWriteArraySet<Integer> ownShards = new CopyOnWriteArraySet<Integer>();
      
      public MSInbound(ClusterInfoSession cluster, ClusterId clusterId, Collection<Class<?>> messageTypes, Destination thisDestination) throws ClusterInfoException, UnknownHostException
      {
         this.cluster = cluster;
         this.messageTypes = messageTypes;
         this.thisDestination = thisDestination;
         this.clusterId = clusterId;
         clusterInformation.setRoutingStrategy(MicroShardStrategy.this);
         clusterInformation.setMessageTypes(messageTypes);
         register();
      }

      @Override
      public boolean doesMessageKeyBelongToNode(Object messageKey)
      {
         return ownShards.contains(messageKey.hashCode()%clusterInformation.getTotalShards());
      }

      @Override
      public void stop()
      {
         running.set(false);
         try
         {
            cluster.rmdir("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/nodes/"+this.getNodeName());
         }
         catch(Exception e)
         {
         }
      }
      
      private synchronized void register() throws ClusterInfoException, UnknownHostException
      {
         if(!running.get()) return;
         
         cluster.mkdir("/"+this.clusterId.getApplicationName(), DirMode.PERSISTENT);
         if(cluster.mkdir("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName(), DirMode.PERSISTENT) != null)
         {
            cluster.setData("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName(), clusterInformation);
         }
         cluster.mkdir("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/nodes", DirMode.PERSISTENT);
         if(cluster.mkdir("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/nodes/"+this.getNodeName(), DirMode.EPHEMERAL) != null)
         {
            thisInfo = new DefaultSlotInfo();
            thisInfo.setDestination(this.thisDestination);
            thisInfo.setMessageClasses(this.messageTypes);
            
            cluster.setData("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/nodes/"+this.getNodeName(), thisInfo);
         }
         
         cluster.exists("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/nodes/"+this.getNodeName(), new ClusterInfoWatcher()
         {
            @Override
            public void process()
            {
               try{ register(); }
               catch(Exception e)
               {
               }
            }
         });
         getOwnShards();
      }
      
      private void getOwnShards() throws ClusterInfoException
      {
         if(cluster.exists("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/shards", null))
         {
            Collection<String> remoteShards = cluster.getSubdirs("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/shards", new ClusterInfoWatcher()
            {
               @Override
               public void process()
               {
                  try
                  {
                     getOwnShards();
                  }
                  catch(ClusterInfoException e)
                  {
                  }
               }
            });
            if(remoteShards != null)
            {
               for(String shard: remoteShards)
               {
                  SlotInformation info = (SlotInformation)cluster.getData("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/shards/"+shard, null);
                  if(info != null && info.equals(this.thisInfo))
                  {
                     this.ownShards.add(Integer.valueOf(shard));
                  }
                  else
                  {
                     this.ownShards.remove(Integer.valueOf(shard));
                  }
               }
            }
         }
      }
      
      private String getNodeName() throws UnknownHostException
      {
         return InetAddress.getLocalHost().getHostName();
      }
      
      public class DefaultSlotInfo extends SlotInformation 
      {
         private static final long serialVersionUID = 1L;
      }
   }
   
   public class MSOutbound implements Outbound
   {
      private ClusterId clusterId;
      private ClusterInfoSession clusterSession;
      private Coordinator coordinator;
      private MicroShardClusterInformation clusterInformation;
      private ConcurrentHashMap<Integer, SlotInformation> shards = new ConcurrentHashMap<Integer, SlotInformation>();
      private AtomicBoolean running = new AtomicBoolean(false);
      private AtomicBoolean refresh = new AtomicBoolean(false);
      
      public MSOutbound(Coordinator coordinator, ClusterInfoSession clusterSession, ClusterId clusterId) throws ClusterInfoException
      {
         this.clusterId = clusterId;
         this.clusterSession = clusterSession;
         this.coordinator = coordinator;
         poplateDestinations();
      }
      
      @Override
      public Destination selectDestinationForMessage(Object messageKey, Object message) throws DempsyException
      {
         Integer calculatedModValue = Math.abs(messageKey.hashCode()%this.clusterInformation.getTotalShards());
         SlotInformation slotInformation = this.shards.get(calculatedModValue);
         return (slotInformation!=null)?slotInformation.getDestination():null;
      }

      @Override
      public ClusterId getClusterId()
      {
         return this.getClusterId();
      }

      @Override
      public void stop()
      {
         this.coordinator.unregisterOutbound(this);
         this.shards.clear();
         this.clusterInformation = null;
      }
      
      private void poplateDestinations() throws ClusterInfoException
      {
         if(running.get())
         {
            refresh.set(true);
            return;
         }
         running.set(true);
         if(this.clusterSession.exists("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName(), new ClusterInfoWatcher()
         {
            @Override
            public void process()
            {
               try
               {
                  poplateDestinations();
               }
               catch(ClusterInfoException e)
               {
               }
            }
         }))
         {
            this.clusterInformation = (MicroShardClusterInformation) this.clusterSession.getData("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName(), null);
            if(this.clusterSession.exists("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/shards", null))
            {
               Collection<String> remoteShards = this.clusterSession.getSubdirs("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/shards", new ClusterInfoWatcher()
               {
                  @Override
                  public void process()
                  {
                     try
                     {
                        poplateDestinations();
                     }
                     catch(ClusterInfoException e)
                     {
                     }
                  }
               });
               if(remoteShards != null)
               {
                  for(String shard : remoteShards)
                  {
                     SlotInformation slotInformation = (SlotInformation)this.clusterSession.getData("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/shards/"+shard, new ClusterInfoWatcher()
                     {
                        @Override
                        public void process()
                        {
                           try
                           {
                              poplateDestinations();
                           }
                           catch(ClusterInfoException e)
                           {
                           }
                        }
                     });
                     if(this.shards.putIfAbsent(Integer.parseInt(shard), slotInformation)!=null)
                     {
                        this.shards.replace(Integer.parseInt(shard), slotInformation);
                     }
                  }
               }
            }
            this.coordinator.registerOutbound(this, this.clusterInformation.getMessageTypes());
         }
         if(refresh.get())
         {
            refresh.set(false);
            poplateDestinations();
         }
         running.set(false);
      }
   }

   @Override
   public Inbound createInbound(ClusterInfoSession cluster, ClusterId clusterId, Collection<Class<?>> messageTypes, Destination thisDestination)
   {
      if (inBound == null)
      {
         synchronized(this)
         {
            if(inBound == null)
            {
               try
               {
                  inBound = new MSInbound(cluster, clusterId, messageTypes, thisDestination);
               }
               catch(Exception e)
               {
                  return null;
               }
            }
         }
      }
      return inBound;
   }

   @Override
   public Outbound createOutbound(Coordinator coordinator, ClusterInfoSession cluster, ClusterId clusterId)
   {
      Outbound outbound = outbounds.get(clusterId);
      if(outbound != null)
      {
         return outbound;
      }
      synchronized(outbounds)
      {
         outbound = outbounds.get(clusterId);
         if(outbound != null)
         {
            return outbound;
         }
         try
         {
            outbound = new MSOutbound(coordinator, cluster, clusterId); 
            outbounds.putIfAbsent(clusterId, outbound);
         }
         catch(ClusterInfoException e)
         {
            return null;
         }
         return outbound;
      }
   }
}
