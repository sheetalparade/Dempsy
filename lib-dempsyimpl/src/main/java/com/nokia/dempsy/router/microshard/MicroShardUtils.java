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

import com.nokia.dempsy.config.ClusterId;

public class MicroShardUtils
{
   private ClusterId clusterId;
   private String appDIr;
   private String clusterDir;
   private String nodesDir;
   private String shardsDir;
   private String managerDir;
   
   public MicroShardUtils(ClusterId clusterId)
   {
      this.clusterId = clusterId;
   }

   public String getAppDIr()
   {
      return appDIr!=null?appDIr:"/"+this.clusterId.getApplicationName();
   }

   public String getClusterDir()
   {
      return clusterDir!=null?clusterDir:this.clusterId.asPath();
   }

   public String getNodesDir()
   {
      return nodesDir!=null?nodesDir:getClusterDir()+"/nodes";
   }

   public String getShardsDir()
   {
      return shardsDir!=null?shardsDir:getClusterDir()+"/shards";
   }

   public String getManagerDir()
   {
      return managerDir!=null?managerDir:getAppDIr()+"/manager";
   }

}
