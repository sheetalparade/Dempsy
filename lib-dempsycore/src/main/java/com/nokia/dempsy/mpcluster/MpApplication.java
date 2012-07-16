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

import java.util.Collection;

/**
 * This is a reference to a point in the cluster information store hierarchy where 
 * application specific information can be obtained. See {@link MpCluster} for a description
 * of the structure of the information hierarchy. 
 */
public interface MpApplication<T,N>
{
   /**
    * @return all of the active clusters known about for this {@link MpApplication}
    */
   public Collection<MpCluster<T,N>> getActiveClusters() throws MpClusterException;

   /**
    * Add {@link MpClusterWatcher} to be invoked when the application sees a new cluster.
    * Or a Cluster leaves the Application. The implementer should use using Set semantics
    * to keep track of the {@link MpClusterWatcher}s.
    *  
    * @param watch is the callback to be notified of a change.
    */
   public void addWatcher(MpClusterWatcher watch);
   
}
