package com.datastax.oss.driver.internal.core.loadbalancing.nodeset;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.metadata.DefaultNodeInfo;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import jnr.ffi.annotations.Synchronized;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class LazyCopyQueryPlan extends AbstractQueue<Node> implements QueryPlan {
  private final Queue<Node> originalPlan;
  private final List<DefaultNodeInfo> itemsPulled = Collections.synchronizedList(new ArrayList<>());

  public LazyCopyQueryPlan(@NonNull Queue<Node> originalPlan) {
    this.originalPlan = originalPlan;
  }

  @Nullable
  @Override
  @Synchronized
  public Node poll() {
    Node node = originalPlan.poll();
    if (node == null) {
      return null;
    }
    this.itemsPulled.add(new DefaultNodeInfo.Builder(node).build());
    return node;
  }

  @NonNull
  @Override
  @Synchronized
  public Iterator<Node> iterator() {
    return new NodeIterator();
  }

  @Override
  @Synchronized
  public int size() {
    return originalPlan.size();
  }

  @Override
  @Synchronized
  public String toString() {
    List<DefaultNodeInfo> inQueue = new ArrayList<>();
    for (Node node : originalPlan) {
      inQueue.add(new DefaultNodeInfo.Builder(node).build());
    }

    return String.format(
        "%s(inQueue: %s, itemsPulled: %s)",
        originalPlan.getClass().getName(), inQueue, itemsPulled);
  }

  private class NodeIterator implements Iterator<Node> {
    @Override
    @Synchronized
    public boolean hasNext() {
      return !originalPlan.isEmpty();
    }

    @Override
    @Synchronized
    public Node next() {
      Node next = poll();
      if (next == null) {
        throw new NoSuchElementException();
      }
      return next;
    }
  }
}
