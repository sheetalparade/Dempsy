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

import com.nokia.dempsy.router.ClusterInformation;
import com.nokia.dempsy.router.RoutingStrategy;
import com.nokia.dempsy.serialization.Serializer;

public class MicroShardClusterInformation extends ClusterInformation
{
   private static final long serialVersionUID = 1L;
   
   private RoutingStrategy routingStrategy;
   private Serializer<?> serializer;
   private Integer totalShards;
   
   public MicroShardClusterInformation()
   {
   }
   
   public RoutingStrategy getRoutingStrategy()
   {
      return routingStrategy;
   }
   public void setRoutingStrategy(RoutingStrategy routingStrategy)
   {
      this.routingStrategy = routingStrategy;
   }
   public Serializer<?> getSerializer()
   {
      return serializer;
   }
   public void setSerializer(Serializer<?> serializer)
   {
      this.serializer = serializer;
   }
   public Integer getTotalShards()
   {
      return totalShards;
   }
   public void setTotalShards(Integer totalShards)
   {
      this.totalShards = totalShards;
   }

}
