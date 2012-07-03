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

package com.nokia.dempsy.mpcluster;

/**
 * This interface represents a handle to a "node" within a cluster.  
 * There is a one to one relationship between a "node" and a Node in the 
 * cluster. 
 * See the ZookeeperCluster for an example where this isn't the case.
 * 
 * @param <T>
 */
public interface MpClusterNode<T>
{
   public T getNodeInformation() throws MpClusterException;
   
   public void setNodeInformation(T info) throws MpClusterException;
   
   /**
    * @return the name that the node was created with in the 
    * {@link MpCluster}.join() call
    */
   public String getNodeName();
   
   /**
    * Explicitly leave the cluster.
    *  
    * @throws MpClusterException when something goes wrong (Duh!)
    */
   public void leave() throws MpClusterException;
   
}
