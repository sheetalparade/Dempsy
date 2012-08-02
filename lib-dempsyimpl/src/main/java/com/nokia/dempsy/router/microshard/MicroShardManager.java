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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.router.SlotInformation;

public class MicroShardManager
{
   private Logger logger = LoggerFactory.getLogger(MicroShardManager.class);
   private ClusterInfoSession clusterSession;
   private ClusterId clusterId;
   private AtomicBoolean leader = new AtomicBoolean(false);
   private Integer seq;
   private MicroShardClusterInformation clusterInformation;
   private MicroShardUtils utils;
   private AtomicBoolean running = new AtomicBoolean(false);
   private AtomicBoolean refresh = new AtomicBoolean(false);
   private ConcurrentHashMap<String, List<Integer>> nodeMap = new ConcurrentHashMap<String, List<Integer>>();
   private AtomicBoolean iAmAssigningNodes = new AtomicBoolean(false); // only for unit testing

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
               logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.init()", e);
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
      this.clusterSession.mkdir(this.utils.getNodesDir(), DirMode.PERSISTENT);
      manageNodes();
   }
   
   public void stop() throws ClusterInfoException
   {
      if(seq != null)
      {
         this.clusterSession.rmdir(this.utils.getManagerDir()+"/M_"+seq);
         seq = null;
      }
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
               logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.registerForLeaderElection()", e);
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
                     logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.registerForLeaderElection()", e);
                  }
               }
            });
         }
      }
   }
   
   public boolean isLeader(){ return this.leader.get();}

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
               logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.manageNodes()", e);
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
               logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.manageNodes()", e);
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
               logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.manageNodes()", e);
            }
         }
      });

      updateShardsInCache(nodes, shards);

      if(this.leader.get())
      {
         assignSlots(nodes, shards);
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
                     logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.manageNodes()", e);
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
         SlotInformation info = (SlotInformation)this.clusterSession.getData(this.utils.getNodesDir()+"/"+node, new ClusterInfoWatcher()
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
                  logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.manageNodes()", e);
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
   
   private void assignSlots(Collection<String> nodes, Collection<String> shards) throws ClusterInfoException
   {
      this.iAmAssigningNodes.set(true);
      if(this.clusterInformation == null) return;
      int totalAssignedShards = 0;
      List<Integer> assignedShards = new ArrayList<Integer>(this.clusterInformation.getTotalShards());
      for(List<Integer> list : this.nodeMap.values())
      {
         totalAssignedShards+=list.size();
         assignedShards.addAll(list);
      }
      
      if(totalAssignedShards == this.clusterInformation.getTotalShards())
         return; //Nothing to do. everything is up to date
      
      if(nodes.size()==0) return ; //no nodes to assign
      
      int maxShardCount = (int)Math.ceil((double)this.clusterInformation.getTotalShards()/(double)nodes.size());
      int minShardCount = this.clusterInformation.getTotalShards()/nodes.size();

      List<Integer> reassignShards = new ArrayList<Integer>();
      for(String node : nodes)
      {
         List<Integer> list = this.nodeMap.get(node);
         int overAssign = (list!=null?list.size():0) - maxShardCount;
         while(overAssign > 0)
         {
            reassignShards.add(list.remove(0));
            overAssign = maxShardCount - list.size();
         }
      }
      Random random = new Random();
      for(String node : nodes)
      {
         List<Integer> list = this.nodeMap.get(node);
         int underAssign = (list!=null?list.size():0) - minShardCount;
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
            this.clusterSession.setData(this.utils.getShardsDir()+"/"+shard, this.clusterSession.getData(node, new ClusterInfoWatcher()
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
                     logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.manageNodes()", e);
                  }
               }
            }));
            underAssign = list.size() - minShardCount;;
         }
      }
      
      while(assignedShards.size() < this.clusterInformation.getTotalShards())
      {
         for(String node : nodes)
         {
            List<Integer> list = this.nodeMap.get(node);
            if((list!= null?list.size():0)<maxShardCount)
            {
               int shard = random.nextInt(this.clusterInformation.getTotalShards());
               while(assignedShards.contains(shard))
               {
                  shard = random.nextInt(this.clusterInformation.getTotalShards());
               }
               if(list == null) list = new ArrayList<Integer>();
               list.add(shard);
               assignedShards.add(shard);
               try
               {
                  this.clusterSession.rmdir(this.utils.getShardsDir()+"/"+shard);
               }
               catch(Exception e)
               {
                  logger.error("Directory might not be there "+this.utils.getShardsDir()+"/"+shard, e);
               }
               this.clusterSession.mkdir(this.utils.getShardsDir()+"/"+shard, DirMode.PERSISTENT);
               this.clusterSession.setData(this.utils.getShardsDir()+"/"+shard, this.clusterSession.getData(node, new ClusterInfoWatcher()
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
                        logger.error("Error during callback for com.nokia.dempsy.router.microshard.MicroShardManager.manageNodes()", e);
                     }
                  }
               }));
            }
         }
      }
   }

   protected boolean getiAmAssigningNodes(){ return iAmAssigningNodes.get(); }
}
