/*
 * Copyright 2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package com.nokia.dempsy.router.microshard;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.router.SlotInformation;

public class MicroShardManager
{
   private ClusterInfoSession clusterSession;
   private ClusterId clusterId;
   private AtomicBoolean leader = new AtomicBoolean(false);
   private Integer seq;
   MicroShardClusterInformation clusterInformation;
   private MicroShardUtils utils;
   private AtomicBoolean running = new AtomicBoolean(false);
   private AtomicBoolean refresh = new AtomicBoolean(false);
   private ConcurrentHashMap<String, List<Integer>> nodeMap = new ConcurrentHashMap<String, List<Integer>>();

   public MicroShardManager(ClusterInfoSession clusterSession, ClusterId clusterId) throws ClusterInfoException
   {
      this.clusterSession = clusterSession;
      this.clusterId = clusterId;
      this.utils = new MicroShardUtils(this.clusterId);
      init();
   }

   private void init() throws ClusterInfoException
   {
      this.clusterSession.mkdir(this.utils.getAppDIr(), DirMode.PERSISTENT);
      this.clusterSession.mkdir(this.utils.getManagerDir(), DirMode.PERSISTENT);
      if(seq != null && this.clusterSession.exists(this.utils.getManagerDir()+"/M_" + seq.intValue(), new ClusterInfoWatcher()
      {
         @Override
         public void process()
         {
            try
            {
               init();
            }
            catch(ClusterInfoException e)
            {
            }
         }
      }))
      {
         //Already exists.
      }
      else
      {
         String path = this.clusterSession.mkdir(this.utils.getManagerDir()+"/M_", DirMode.EPHEMERAL_SEQUENTIAL);
         if(path == null)
            throw new ClusterInfoException("Unable to create ephemeral-sequential dir at " + this.utils.getManagerDir()+"/M_");
         seq = Integer.parseInt(path.substring(path.lastIndexOf("_") + 1));
      }
      registerForLeaderElection();
      this.clusterSession.mkdir(this.utils.getClusterDir(), DirMode.PERSISTENT);
      this.clusterSession.mkdir(this.utils.getShardsDir(), DirMode.PERSISTENT);
      manageNodes();
   }

   private void registerForLeaderElection() throws ClusterInfoException
   {
      Collection<String> subDirs = this.clusterSession.getSubdirs(this.utils.getManagerDir(), new ClusterInfoWatcher()
      {
         @Override
         public void process()
         {
            try
            {
               registerForLeaderElection();
            }
            catch(ClusterInfoException e)
            {
            }
         }
      });
      SortedSet<Integer> leader = new TreeSet<Integer>();
      for(String dir: subDirs)
      {
         leader.add(Integer.parseInt(dir.substring(dir.lastIndexOf("_") + 1)));
      }
      if(leader.size() > 0)
      {
         Integer i = leader.first();
         if(i.equals(seq))
         {
            this.leader.set(true);
         }
         else
         {
            this.clusterSession.exists(this.utils.getManagerDir()+"/M_" + i, new ClusterInfoWatcher()
            {
               @Override
               public void process()
               {
                  try
                  {
                     registerForLeaderElection();
                  }
                  catch(ClusterInfoException e)
                  {
                  }
               }
            });
         }
      }
   }

   private void manageNodes() throws ClusterInfoException
   {
      if(this.running.get())
      {
         this.refresh.set(true);
         return;
      }
      this.refresh.set(false);
      this.running.set(true);
      
      clusterInformation = (MicroShardClusterInformation)this.clusterSession.getData(this.utils.getClusterDir(), new ClusterInfoWatcher()
      {
         @Override
         public void process()
         {
            try
            {
               manageNodes();
            }
            catch(Exception e)
            {
            }
         }
      });
      Collection<String> nodes = this.clusterSession.getSubdirs(this.utils.getNodesDir(), new ClusterInfoWatcher()
      {
         @Override
         public void process()
         {
            try
            {
               manageNodes();
            }
            catch(Exception e)
            {
            }
         }
      });
      
      Collection<String> shards = this.clusterSession.getSubdirs(this.utils.getShardsDir(), new ClusterInfoWatcher()
      {
         @Override
         public void process()
         {
            try
            {
               manageNodes();
            }
            catch(Exception e)
            {
            }
         }
      });

      updateShardsInCache(nodes, shards);

      if(this.leader.get())
      {
         assignSlots();
      }
      
      this.running.set(false);
      if(this.refresh.get())
      {
         manageNodes();
      }
   }
   
   private void updateShardsInCache(Collection<String> nodes, Collection<String> shards) throws ClusterInfoException
   {
      if(nodes == null || nodes.size() == 0) 
      {
         this.nodeMap.clear();
         return;
      }
      HashMap<SlotInformation, List<Integer>> slots = new HashMap<SlotInformation, List<Integer>>();
      if(shards != null)
      {
         for(String shard : shards)
         {
            SlotInformation slotInformation = (SlotInformation)this.clusterSession.getData(shard, new ClusterInfoWatcher()
            {
               @Override
               public void process()
               {
                  try
                  {
                     manageNodes();
                  }
                  catch(Exception e)
                  {
                  }
               }
            });
            if(slotInformation != null)
            {
               List<Integer> list = slots.get(slotInformation);
               if(list == null) list = new ArrayList<Integer>();
               list.add(Integer.parseInt(shard.substring(shard.lastIndexOf('/'))));
               slots.put(slotInformation, list);
            }
         }
      }
      for(String node : nodes)
      {
         SlotInformation info = (SlotInformation)this.clusterSession.getData(node, new ClusterInfoWatcher()
         {
            @Override
            public void process()
            {
               try
               {
                  manageNodes();
               }
               catch(Exception e)
               {
               }
            }
         });
         if(info != null)
         {
            List<Integer> associatedShards = slots.get(info);
            if(associatedShards == null) this.nodeMap.remove(node);
            else
            {
               List<Integer> prev = this.nodeMap.putIfAbsent(node, associatedShards);
               if(prev != null) this.nodeMap.replace(node, prev, associatedShards);
            }
         }
      }
      Set<String> set = this.nodeMap.keySet();
      for(Iterator<String> iterator = set.iterator(); iterator.hasNext();)
      {
         String string = iterator.next();
         if(slots.containsKey(string)) continue;
         else iterator.remove();
      }
   }
   
   private void assignSlots() throws ClusterInfoException
   {
      int totalAssignedShards = 0;
      List<Integer> assignedShards = new ArrayList<Integer>(this.clusterInformation.getTotalShards());
      for(List<Integer> list : this.nodeMap.values())
      {
         totalAssignedShards+=list.size();
         assignedShards.addAll(list);
      }
      
      if(totalAssignedShards == this.clusterInformation.getTotalShards())
         return; //Nothing to do. everything is up to date
      
      int maxShardCount = (int)Math.ceil((double)this.clusterInformation.getTotalShards()/(double)this.nodeMap.size());
      int minShardCount = this.clusterInformation.getTotalShards()/this.nodeMap.size();

      List<Integer> reassignShards = new ArrayList<Integer>();
      for(String node : this.nodeMap.keySet())
      {
         List<Integer> list = this.nodeMap.get(node);
         int overAssign = maxShardCount - list.size();
         while(overAssign > 0)
         {
            reassignShards.add(list.remove(0));
            overAssign = maxShardCount - list.size();
         }
      }
      Random random = new Random();
      for(String node : this.nodeMap.keySet())
      {
         List<Integer> list = this.nodeMap.get(node);
         int underAssign = list.size() - minShardCount;
         while(underAssign > 0)
         {
            int shard = -1;
            if(reassignShards.size()>0)
            {
               shard = reassignShards.get(0);
            }
            else
            {
               shard = random.nextInt(this.clusterInformation.getTotalShards());
               while(assignedShards.contains(shard))
               {
                  shard = random.nextInt(this.clusterInformation.getTotalShards());
               }
               list.add(shard);
               assignedShards.add(shard);
            }
            this.clusterSession.rmdir(this.utils.getShardsDir()+"/"+shard);
            this.clusterSession.mkdir(this.utils.getShardsDir()+"/"+shard, DirMode.PERSISTENT);
            this.clusterSession.setData(this.utils.getShardsDir()+"/"+shard, this.clusterSession.getData(node, null));
            underAssign = list.size() - minShardCount;;
         }
      }
      
      while(assignedShards.size() < this.clusterInformation.getTotalShards())
      {
         for(String node : this.nodeMap.keySet())
         {
            List<Integer> list = this.nodeMap.get(node);
            if(list.size()<maxShardCount)
            {
               int shard = random.nextInt(this.clusterInformation.getTotalShards());
               while(assignedShards.contains(shard))
               {
                  shard = random.nextInt(this.clusterInformation.getTotalShards());
               }
               list.add(shard);
               assignedShards.add(shard);
               this.clusterSession.rmdir(this.utils.getShardsDir()+"/"+shard);
               this.clusterSession.mkdir(this.utils.getShardsDir()+"/"+shard, DirMode.PERSISTENT);
               this.clusterSession.setData(this.utils.getShardsDir()+"/"+shard, this.clusterSession.getData(node, null));
            }
         }
      }
   }
}
