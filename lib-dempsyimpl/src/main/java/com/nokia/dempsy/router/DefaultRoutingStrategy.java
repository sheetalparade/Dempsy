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

package com.nokia.dempsy.router;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.DempsyException;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.internal.util.SafeString;
import com.nokia.dempsy.messagetransport.Destination;
import com.nokia.dempsy.mpcluster.MpCluster;
import com.nokia.dempsy.mpcluster.MpClusterException;
import com.nokia.dempsy.mpcluster.MpClusterSlot;
import com.nokia.dempsy.mpcluster.MpClusterWatcher;

/**
 * This Routing Strategy uses the {@link MpCluster} to negotiate with other instances in the 
 * cluster.
 */
public class DefaultRoutingStrategy implements RoutingStrategy, MpClusterWatcher<ClusterInformation, SlotInformation>
{
   private static Logger logger = LoggerFactory.getLogger(DefaultRoutingStrategy.class);
   
   private int defaultTotalSlots;
   private int defaultNumNodes;
   
   private Map<ClusterId, List<Inbound>> inboundsMap = null;
   private Map<ClusterId, Outbound> outbounds = null;
   
   private Map<Class<?>, List<ClusterId>> messageTypeClustermap = new HashMap<Class<?>, List<ClusterId>>();
   
   public DefaultRoutingStrategy(int defaultTotalSlots, int defaultNumNodes)
   {
      this.defaultTotalSlots = defaultTotalSlots;
      this.defaultNumNodes = defaultNumNodes;
   }
   
   private class Outbound implements RoutingStrategy.Outbound
   {
      private ConcurrentHashMap<Integer, DefaultRouterSlotInfo> destinations = new ConcurrentHashMap<Integer, DefaultRouterSlotInfo>();
      private int totalAddressCounts = -1;
      private MpCluster<ClusterInformation, SlotInformation> cluster;
      
      public Outbound(MpCluster<ClusterInformation, SlotInformation> cluster)
      {
         this.cluster = cluster;
      }

      public synchronized SlotInformation selectSlotForMessageKey(Object messageKey) throws DempsyException
      {
         if (totalAddressCounts < 0)
            throw new DempsyException("It appears the Outbound strategy for the message key " + 
                  SafeString.objectDescription(messageKey) + 
                  " is being used prior to initialization.");
         int calculatedModValue = Math.abs(messageKey.hashCode()%totalAddressCounts);
         return destinations.get(calculatedModValue);
      }
      
      public ConcurrentHashMap<Integer, DefaultRouterSlotInfo> getDestinations(){ return this.destinations; }
      
      public synchronized void resetCluster() throws MpClusterException
      {
         if (logger.isTraceEnabled())
            logger.trace("Resetting Outbound Strategy for cluster " + cluster.getClusterId());
         
         destinations.clear();
         int newtotalAddressCounts = fillMapFromActiveSlots(destinations, cluster);
         if (newtotalAddressCounts == 0)
            throw new MpClusterException("The cluster " + cluster.getClusterId() + 
                  " seems to have invalid slot information. Someone has set the total number of slots to zero.");
         totalAddressCounts = newtotalAddressCounts > 0 ? newtotalAddressCounts : totalAddressCounts;
      }
      
   } // end Outbound class definition
   
   private class Inbound implements RoutingStrategy.Inbound
   {
      private List<Integer> destinationsAcquired = new ArrayList<Integer>();
      private Destination destination;
      
      private List<Class<?>> acceptedMessageTypes;
      private MpCluster<ClusterInformation, SlotInformation> cluster;
      
      private AtomicBoolean stopping = new AtomicBoolean(false);
      
      public Inbound(MpCluster<ClusterInformation, SlotInformation> cluster, List<Class<?>> acceptedMessageTypes, Destination destination)
      {
         this.destination = destination;
         this.cluster= cluster;
         this.acceptedMessageTypes = acceptedMessageTypes;
      }

      public synchronized void resetCluster() throws MpClusterException
      {
         if (logger.isTraceEnabled())
            logger.trace("Resetting Inbound Strategy for cluster " + cluster.getClusterId());
         
         // Ignore reset if stopping
         if(stopping.get()) { return; }

         int minNodeCount = defaultNumNodes;
         int totalAddressNeeded = defaultTotalSlots;
         Random random = new Random();
         
         //==============================================================================
         // need to verify that the existing slots in destinationsAcquired are still ours
         Map<Integer,DefaultRouterSlotInfo> slotNumbersToSlots = new HashMap<Integer,DefaultRouterSlotInfo>();
         fillMapFromActiveSlots(slotNumbersToSlots,cluster);
         Collection<Integer> slotsToReaquire = new ArrayList<Integer>();
         for (Integer destinationSlot : destinationsAcquired)
         {
            // select the coresponding slot information
            DefaultRouterSlotInfo slotInfo = slotNumbersToSlots.get(destinationSlot);
            if (slotInfo == null || !destination.equals(slotInfo.getDestination()))
               slotsToReaquire.add(destinationSlot);
         }
         //==============================================================================
         
         //==============================================================================
         // Now reaquire the potentially lost slots
         for (Integer slotToReaquire : slotsToReaquire)
         {
            if (!acquireSlot(slotToReaquire, totalAddressNeeded,
                  cluster, acceptedMessageTypes, destination))
            {
               // in this case, see if I already own it...
               logger.error("Cannot reaquire the slot " + slotToReaquire + " for the cluster " + cluster.getClusterId());
            }
         }
         //==============================================================================

         while(needToGrabMoreSlots(cluster,minNodeCount,totalAddressNeeded))
         {
            int randomValue = random.nextInt(totalAddressNeeded);
            if(destinationsAcquired.contains(randomValue))
               continue;
            if (acquireSlot(randomValue, totalAddressNeeded,
                  cluster, acceptedMessageTypes, destination))
               destinationsAcquired.add(randomValue);                  
         }
      }
      
      public void stop() throws MpClusterException
      {
         this.stopping.set(true);
         Collection<MpClusterSlot<SlotInformation>> slots = cluster.getActiveSlots();
         for(MpClusterSlot<SlotInformation> mpClusterSlot: slots)
         {
            Destination slotDestination = null;
            if(mpClusterSlot != null && mpClusterSlot.getSlotInformation() != null)
               slotDestination = mpClusterSlot.getSlotInformation().getDestination();
            if(slotDestination == null || slotDestination.equals(destination))
               mpClusterSlot.leave();
         }
      }
      
      private boolean needToGrabMoreSlots(MpCluster<ClusterInformation, SlotInformation> clusterHandle,
            int minNodeCount, int totalAddressNeeded) throws MpClusterException
      {
         int addressInUse = clusterHandle.getActiveSlots().size();
         int maxSlotsForOneNode = (int)Math.ceil((double)totalAddressNeeded / (double)minNodeCount);
         return addressInUse < totalAddressNeeded && destinationsAcquired.size() < maxSlotsForOneNode;
      }
      
      @Override
      public boolean doesMessageKeyBelongToCluster(Object messageKey)
      {
         return destinationsAcquired.contains(messageKey.hashCode()%defaultTotalSlots);
      }
      
      public Destination getDestination(){ return this.destination; }
      
   } // end Inbound class definition
   
   @Override
   public RoutingStrategy.Inbound getInbound(
         MpCluster<ClusterInformation, SlotInformation> cluster, List<Class<?>> acceptedMessageTypes, Destination destination) throws MpClusterException
   { 
      if(inboundsMap == null)
      {
         synchronized(this)
         {
            if(inboundsMap == null) 
            { 
               inboundsMap = new HashMap<ClusterId, List<Inbound>>();
            }
         }
      }
      Inbound inbound = null;
      List<Inbound> inbounds = inboundsMap.get(cluster.getClusterId());
      if(inbounds == null)
      {
         synchronized(inboundsMap)
         {
            inbounds = inboundsMap.get(cluster.getClusterId());
            if(inbounds == null)
            {
               inbounds = new ArrayList<Inbound>();
            }
         }
      }
      boolean notFound = true;
      for(Inbound in : inbounds)
      {
         if(destination.equals(in.getDestination())) { notFound = false; }
      }
      if(notFound)
      {
         inbound = new  Inbound(cluster, acceptedMessageTypes, destination);
         inbounds.add(inbound);
         inboundsMap.put(cluster.getClusterId(), inbounds);
         cluster.addWatcher(this);
      }
      return inbound; 
   }
   
   @Override
   public RoutingStrategy.Outbound getOutbound(MpCluster<ClusterInformation, SlotInformation> cluster) throws MpClusterException
   {
      if(outbounds == null)
      {
         synchronized(this)
         {
            if(outbounds == null) 
            { 
               outbounds = new HashMap<ClusterId, Outbound>();
            }
         }
      }
      Outbound outbound = outbounds.get(cluster.getClusterId());
      if(outbound == null)
      {
         synchronized(outbounds)
         {
            outbound = outbounds.get(cluster.getClusterId());
            if(outbound == null)
            {
               outbound = new  Outbound(cluster);
               outbounds.put(cluster.getClusterId(), outbound);
               cluster.addWatcher(this);
            }
         }
      }
      return outbound; 
   }
 
   @Override
   public synchronized List<Destination> getDestinations(Object key, Object message) throws DempsyException
   {
      List<Destination> destinations = new ArrayList<Destination>();
      List<ClusterId> clusters = messageTypeClustermap.get(message.getClass());
      if(clusters != null)
      {
         for(ClusterId clusterId : clusters)
         {
            Outbound outbound = outbounds.get(clusterId);
            if(outbound != null)
            {
               SlotInformation slotInformation = outbound.selectSlotForMessageKey(key);
               if(slotInformation != null) { destinations.add(slotInformation.getDestination()); }
            }
         }
      }
      return destinations;
   }

   static class DefaultRouterSlotInfo extends SlotInformation
   {
      private static final long serialVersionUID = 1L;

      private int totalAddress = -1;
      private int slotIndex = -1;

      public int getSlotIndex() { return slotIndex; }
      public void setSlotIndex(int modValue) { this.slotIndex = modValue; }

      public int getTotalAddress() { return totalAddress; }
      public void setTotalAddress(int totalAddress) { this.totalAddress = totalAddress; }

      @Override
      public int hashCode()
      {
         final int prime = 31;
         int result = super.hashCode();
         result = prime * result + (int)(slotIndex ^ (slotIndex >>> 32));
         result = prime * result + (int)(totalAddress ^ (totalAddress >>> 32));
         return result;
      }

      @Override
      public boolean equals(Object obj)
      {
         if (!super.equals(obj))
            return false;
         DefaultRouterSlotInfo other = (DefaultRouterSlotInfo)obj;
         if(slotIndex != other.slotIndex)
            return false;
         if(totalAddress != other.totalAddress)
            return false;
         return true;
      }
   }
   
   static class DefaultRouterClusterInfo extends ClusterInformation
   {
      private static final long serialVersionUID = 1L;

      private AtomicInteger minNodeCount = new AtomicInteger(5);
      private AtomicInteger totalSlotCount = new AtomicInteger(300);
      
      public DefaultRouterClusterInfo(int totalSlotCount, int nodeCount)
      {
         this.totalSlotCount.set(totalSlotCount);
         this.minNodeCount.set(nodeCount);
      }

      public int getMinNodeCount() { return minNodeCount.get(); }
      public void setMinNodeCount(int nodeCount) { this.minNodeCount.set(nodeCount); }

      public int getTotalSlotCount(){ return totalSlotCount.get();  }
      public void setTotalSlotCount(int addressMultiplier) { this.totalSlotCount.set(addressMultiplier); }
   }
   
   /**
    * Fill the map of slots to slotinfos for internal use. 
    * @return the totalAddressCount from each slot. These are supposed to be repeated.
    */
   private static int fillMapFromActiveSlots(Map<Integer,DefaultRouterSlotInfo> mapToFill, 
         MpCluster<ClusterInformation, SlotInformation> clusterHandle)
   throws MpClusterException
   {
      int totalAddressCounts = -1;
      Collection<MpClusterSlot<SlotInformation>> slotsFromClusterManager = clusterHandle.getActiveSlots();

      if(slotsFromClusterManager != null)
      {
         for(MpClusterSlot<SlotInformation> node: slotsFromClusterManager)
         {
            DefaultRouterSlotInfo slotInfo = (DefaultRouterSlotInfo)node.getSlotInformation();
            if(slotInfo != null)
            {
               mapToFill.put(slotInfo.getSlotIndex(), slotInfo);
               if (totalAddressCounts == -1)
                  totalAddressCounts = slotInfo.getTotalAddress();
               else if (totalAddressCounts != slotInfo.getTotalAddress())
                  logger.error("There is a problem with the slots taken by the cluster manager for the cluster " + 
                        clusterHandle.getClusterId() + ". Slot " + slotInfo.getSlotIndex() +
                        " from " + SafeString.objectDescription(slotInfo.getDestination()) + 
                        " thinks the total number of slots for this cluster it " + slotInfo.getTotalAddress() +
                        " but a former slot said the total was " + totalAddressCounts);
            }
         }
      }
      return totalAddressCounts;
   }
   
   private static boolean acquireSlot(int slotNum, int totalAddressNeeded,
         MpCluster<ClusterInformation, SlotInformation> clusterHandle,
         List<Class<?>> messagesTypes, Destination destination) throws MpClusterException
   {
      MpClusterSlot<SlotInformation> slot = clusterHandle.join(String.valueOf(slotNum));
      if(slot == null)
         return false;
      DefaultRouterSlotInfo dest = (DefaultRouterSlotInfo)slot.getSlotInformation();
      if(dest == null)
      {
         dest = new DefaultRouterSlotInfo();
         dest.setDestination(destination);
         dest.setSlotIndex(slotNum);
         dest.setTotalAddress(totalAddressNeeded);
         dest.setMessageClasses(messagesTypes);
         slot.setSlotInformation(dest);
      }
      return true;
   }
   
   @Override
   public synchronized void reset(MpCluster<ClusterInformation, SlotInformation> cluster)
   {
      if(inboundsMap != null)
      {
         List<Inbound> inbounds=null;
         try
         {
            inbounds = inboundsMap.get(cluster.getClusterId());
         }
         catch(MpClusterException e)
         {
            logger.error("Error getting clusterId for "+SafeString.valueOf(cluster), e);
         }
         if(inbounds != null)
            for(Inbound inbound: inbounds)
            {
               try
               {
                  inbound.resetCluster();
               }
               catch(MpClusterException e)
               {
                  logger.error("Error resetting inbound cluster "+SafeString.valueOf(cluster), e);
               }
            }
      }
      if(outbounds != null)
      {
         ClusterId clusterId = null;
         try
         {
            clusterId = cluster.getClusterId();
         }
         catch(Exception e)
         {
            logger.error("Error getting clusterId for cluster "+SafeString.valueOf(cluster), e);
            return;
         }
         Outbound outbound = outbounds.get(clusterId);
         if(outbound != null) 
         { 
            try
            {
               outbound.resetCluster();
            }
            catch(MpClusterException e)
            {
               logger.error("Error resetting outbound cluster "+SafeString.valueOf(clusterId)+" with outbound "+SafeString.valueOf(outbound), e);
            }
            ConcurrentHashMap<Integer, DefaultRouterSlotInfo> destinations = outbound.getDestinations();
            if(destinations != null && !destinations.isEmpty())
            {
               Set<Class<?>> acceptedMessageTypes = destinations.get(destinations.keys().nextElement()).getMessageClasses();
               for(Class<?> messageType: acceptedMessageTypes)
               {
                  List<ClusterId> clusterIds = messageTypeClustermap.get(messageType);
                  if(clusterIds == null) { clusterIds = new ArrayList<ClusterId>(); }
                  if(!clusterIds.contains(clusterId))
                     clusterIds.add(clusterId);
                  messageTypeClustermap.put(messageType,clusterIds);
               }
            }

         }
      }
   }

   @Override
   public void process(MpCluster<ClusterInformation, SlotInformation> cluster)
   {
      try
      {
         reset(cluster);
      }
      catch(Throwable e)
      {
         logger.error("Error resetting the cluster "+ SafeString.valueOf(cluster)+" while processing watcher "+SafeString.valueOfClass(this), e);
      }
   }
   
   @Override
   public void stop(com.nokia.dempsy.router.RoutingStrategy.Inbound inbound) throws MpClusterException
   {
      inbound.stop();
   }

}
