package com.nokia.dempsy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Ignore;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.config.ClusterId;
import com.nokia.dempsy.router.DecentralizedRoutingStrategy;

@Ignore
public class TestUtils
{
   public static interface Condition<T>
   {
      public boolean conditionMet(T o) throws Throwable;
   }

   public static <T> boolean poll(long timeoutMillis, T userObject, Condition<T> condition) throws Throwable
   {
      for (long endTime = System.currentTimeMillis() + timeoutMillis;
            endTime > System.currentTimeMillis() && !condition.conditionMet(userObject);)
         Thread.sleep(1);
      return condition.conditionMet(userObject);
   }
   
   public static String createApplicationLevel(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      String ret = "/" + cid.getApplicationName();
      session.mkdir(ret, false);
      return ret;
   }
   
   public static String createClusterLevel(ClusterId cid, ClusterInfoSession session) throws ClusterInfoException
   {
      String ret = createApplicationLevel(cid,session);
      ret += ("/" + cid.getMpClusterName());
      session.mkdir(ret, false);
      return ret;
   }

   public static boolean waitForClustersToBeInitialized(long timeoutMillis, 
         final int numSlotsPerCluster, Dempsy dempsy) throws Throwable
   {
      try
      {
         final List<ClusterId> clusters = new ArrayList<ClusterId>();
         
         // find out all of the ClusterIds
         for (Dempsy.Application app : dempsy.applications)
         {
            for (Dempsy.Application.Cluster cluster : app.appClusters)
               if (!cluster.clusterDefinition.isRouteAdaptorType())
                  clusters.add(new ClusterId(cluster.clusterDefinition.getClusterId()));
         }
         
         ClusterInfoSession session = dempsy.clusterSessionFactory.createSession();
         boolean ret = poll(timeoutMillis, session, new Condition<ClusterInfoSession>()
         {
            @Override
            public boolean conditionMet(ClusterInfoSession session)
            {
               try
               {
                  for (ClusterId c : clusters)
                  {
                     Collection<String> slots = session.getSubdirs(c.asPath(), null);
                     if (slots == null || slots.size() != numSlotsPerCluster)
                        return false;
                  }
               }
               catch(ClusterInfoException e)
               {
                  return false;
               }

               return DecentralizedRoutingStrategy.allOutboundsInitialized();
            }
         });
         
         session.stop();
         return ret;
      }
      catch (ClusterInfoException e)
      {
         return false;
      }
   }
}
