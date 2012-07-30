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

package com.nokia.dempsy.cluster.microshard;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.config.ClusterId;

public class MicroShardManager
{
   private ClusterInfoSession clusterSession;
   private ClusterId clusterId;
   private AtomicBoolean leader = new AtomicBoolean(false);

   public MicroShardManager(ClusterInfoSession clusterSession, ClusterId clusterId) throws ClusterInfoException
   {
      this.clusterSession = clusterSession;
      this.clusterId = clusterId;
      init();
   }

   private void init() throws ClusterInfoException
   {
      this.clusterSession.mkdir("/" + this.clusterId.getApplicationName(), DirMode.PERSISTENT);
      this.clusterSession.mkdir("/" + this.clusterId.getApplicationName()+"/manager", DirMode.PERSISTENT);
      this.clusterSession.mkdir("/" + this.clusterId.getApplicationName()+"/manager/M_", DirMode.EPHEMERAL_SEQUENTIAL);
      register();
      this.clusterSession.mkdir("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName(), DirMode.PERSISTENT);
      this.clusterSession.mkdir("/"+this.clusterId.getApplicationName()+"/"+this.clusterId.getMpClusterName()+"/shards", DirMode.PERSISTENT);
      getNodes();
   }
   
   public void register() throws ClusterInfoException
   {
      Collection<String> subDirs = this.clusterSession.getSubdirs("/" + this.clusterId.getApplicationName()+"/manager", new ClusterInfoWatcher()
      {
         @Override
         public void process()
         {
            try
            {
               register();
            }
            catch(ClusterInfoException e)
            {
            }
         }
      });
      SortedSet<Integer> leader = new TreeSet<Integer>();
      for(String dir: subDirs)
      {
         leader.add(Integer.parseInt(dir.substring(2)));
      }
      
   }
   public void getNodes()
   {
      
   }
   
   
}
