package org.apache.hadoop.realtime.dag;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An internal view of vertices.
 */
class InternalVertex<E> {
  Set<E> incoming;
  Set<E> outgoing;
  private Set<E> unmodifiableIncoming = null;
  private Set<E> unmodifiableOutgoing = null;

  InternalVertex() {
    incoming = new HashSet<E>();
    outgoing = new HashSet<E>();
  }

  /**
   * A lazy build of unmodifiable incoming edge set.
   * 
   * @return
   */
  public Set<E> getUnmodifiableIncomingEdges() {
    if (unmodifiableIncoming == null) {
      unmodifiableIncoming = Collections.unmodifiableSet(incoming);
    }
    return unmodifiableIncoming;
  }

  /**
   * A lazy build of unmodifiable outgoing edge set.
   * 
   * @return
   */
  public Set<E> getUnmodifiableOutgoingEdges() {
    if (unmodifiableOutgoing == null) {
      unmodifiableOutgoing = Collections.unmodifiableSet(outgoing);
    }

    return unmodifiableOutgoing;
  }

  public void addIncomingEdge(E e) {
    incoming.add(e);
  }

  public void addOutgoingEdge(E e) {
    outgoing.add(e);
  }

  public void removeIncomingEdge(E e) {
    incoming.remove(e);
  }

  public void removeOutgoingEdge(E e) {
    outgoing.remove(e);
  }
}