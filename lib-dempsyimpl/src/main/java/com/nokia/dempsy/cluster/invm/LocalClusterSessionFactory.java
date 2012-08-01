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

package com.nokia.dempsy.cluster.invm;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nokia.dempsy.cluster.ClusterInfoException;
import com.nokia.dempsy.cluster.ClusterInfoSession;
import com.nokia.dempsy.cluster.ClusterInfoSessionFactory;
import com.nokia.dempsy.cluster.ClusterInfoWatcher;
import com.nokia.dempsy.cluster.DirMode;
import com.nokia.dempsy.internal.util.SafeString;

/**
 * This class is for running all cluster management from within the same vm, and 
 * for the same vm. It's meant to mimic the Zookeeper implementation such that 
 * callbacks are not made to watchers registered to sessions through wich changes
 * are made.
 */
public class LocalClusterSessionFactory implements ClusterInfoSessionFactory
{
   private static Logger logger = LoggerFactory.getLogger(LocalClusterSessionFactory.class);
   private List<LocalSession> currentSessions = new CopyOnWriteArrayList<LocalSession>();

   // ====================================================================
   // This section pertains to the management of the tree information
   private Map<String,Entry> entries = new HashMap<String,Entry>();

   private static class Entry
   {
      private AtomicReference<Object> data = new AtomicReference<Object>();
      private Set<ClusterInfoWatcher> nodeWatchers = new HashSet<ClusterInfoWatcher>();
      private Set<ClusterInfoWatcher> childWatchers = new HashSet<ClusterInfoWatcher>();
      private Collection<String> children = new ArrayList<String>();

      private volatile boolean inProcess = false;
      private volatile boolean recursionAttempt = false;
      private Object processLock = new Object();
      
      private void callWatchers(boolean node, boolean child)
      {
         Set<ClusterInfoWatcher> twatchers = new HashSet<ClusterInfoWatcher>();
         if (node)
         {
            twatchers.addAll(nodeWatchers);
            nodeWatchers = new HashSet<ClusterInfoWatcher>();
         }
         if (child)
         {
            twatchers.addAll(childWatchers);
            childWatchers = new HashSet<ClusterInfoWatcher>();
         }
         
         synchronized(processLock)
         {
            if (inProcess)
            {
               recursionAttempt = true;
               return;
            }

            do
            {
               recursionAttempt = false;
               inProcess = true;

               for(ClusterInfoWatcher watcher: twatchers)
               {
                  try
                  {
                     watcher.process();
                  }
                  catch (RuntimeException e)
                  {
                     logger.error("Failed to handle process for watcher " + SafeString.objectDescription(watcher),e);
                  }
               }
            } while (recursionAttempt);

            inProcess = false;
         }
      }
   }

   private static String parent(String path)
   {
      File f = new File(path);
      return f.getParent();
   }
   
   private static boolean isRoot(String path) { return "/".equals(path); }
   
   private Entry get(String absolutePath, ClusterInfoWatcher watcher, boolean nodeWatch) throws ClusterInfoException
   {
      Entry ret;
      ret = entries.get(absolutePath);
      if (ret == null)
         throw new ClusterInfoException.NoNodeException("Path \"" + absolutePath + "\" doesn't exists.");
      if (watcher != null)
      {
         if (nodeWatch)
            ret.nodeWatchers.add(watcher);
         else
            ret.childWatchers.add(watcher);
      }
      return ret;
   }
   
   private synchronized Object ogetData(String path, ClusterInfoWatcher watcher) throws ClusterInfoException
   {
      Entry e = get(path,watcher,true);
      return e.data.get();
   }
   
   private synchronized void osetData(String path, Object data) throws ClusterInfoException
   {
      Entry e = get(path,null,true);
      e.data.set(data);
      e.callWatchers(true,false);
   }
   
   private synchronized boolean oexists(String path,ClusterInfoWatcher watcher)
   {
      Entry e = entries.get(path);
      if (e != null && watcher != null)
         e.nodeWatchers.add(watcher);
      return e != null;
   }
   
   private synchronized String omkdir(String path) throws ClusterInfoException
   {
      if (oexists(path,null))
         return path;
      
      String parentPath = parent(path);
      Entry parent = null;
      
      if (!isRoot(parentPath))
      {
         parent = entries.get(parentPath);
         if (parent == null)
         {
            throw new ClusterInfoException("No Parent for \"" + path + "\" which is expected to be \"" +
                  parent(path) + "\"");
         }
      }

      entries.put(path, new Entry());
      if (parent != null)
      {
         // find the relative path
         int lastSlash = path.lastIndexOf('/');
         parent.children.add(path.substring(lastSlash + 1));
         parent.callWatchers(false,true);
      }
      return path;
   }
   
   private synchronized void ormdir(String path) throws ClusterInfoException
   {
      Entry ths = entries.get(path);
      if (ths == null)
         throw new ClusterInfoException("rmdir of non existant node \"" + path + "\"");
      
      Entry parent = entries.get(parent(path));
      entries.remove(path);
      if (parent != null)
      {
         int lastSlash = path.lastIndexOf('/');
         parent.children.remove(path.substring(lastSlash + 1));
         parent.callWatchers(false,true);
      }
      ths.callWatchers(true, true);
   }
   
   private synchronized Collection<String> ogetSubdirs(String path, ClusterInfoWatcher watcher) throws ClusterInfoException
   {
      Entry e = get(path,watcher,false);
      Collection<String>ret = new ArrayList<String>(e.children.size());
      ret.addAll(e.children);
      return ret;
   }
   // ====================================================================
   
   @Override
   public ClusterInfoSession createSession()
   {
      LocalSession ret = new LocalSession();
      currentSessions.add(ret);
      return ret;
   }
   
   public class LocalSession implements ClusterInfoSession
   {
      AtomicInteger seq = new AtomicInteger();
      @Override
      public String mkdir(String path, DirMode mode) throws ClusterInfoException
      {
         return omkdir(mode.toString().indexOf("SEQUENTIAL")!=-1?path+seq.getAndIncrement():path);
      }

      @Override
      public void rmdir(String path) throws ClusterInfoException
      {
         ormdir(path);
      }

      @Override
      public boolean exists(String path, ClusterInfoWatcher watcher) throws ClusterInfoException
      {
         return oexists(path,watcher);
      }

      @Override
      public Object getData(String path, ClusterInfoWatcher watcher) throws ClusterInfoException
      {
         return ogetData(path,watcher);
      }

      @Override
      public void setData(String path, Object data) throws ClusterInfoException
      {
         osetData(path,data);
      }

      @Override
      public Collection<String> getSubdirs(String path, ClusterInfoWatcher watcher) throws ClusterInfoException
      {
         return ogetSubdirs(path,watcher);
      }

      @Override
      public void stop()
      {
         currentSessions.remove(this);
      }

   } // end session definition

}
