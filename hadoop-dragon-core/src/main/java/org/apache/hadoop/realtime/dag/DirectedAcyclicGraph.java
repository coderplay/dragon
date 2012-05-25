/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.realtime.dag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * <p>
 * DirectedAcyclicGraph implements a DAG that can be modified (vertices &amp;
 * edges added and removed), is guaranteed to remain acyclic, and provides fast
 * topological order iteration.
 * </p>
 * 
 * <p>
 * This class makes no claims to thread safety, and concurrent usage from
 * multiple threads will produce undefined results.
 * </p>
 */
public class DirectedAcyclicGraph<V, E> implements Serializable {
  private static final long serialVersionUID = -8352536912775689101L;

  /* vertex -> internal vertex */
  private Map<V, InternalVertex<E>> vertexMap =
      new LinkedHashMap<V, InternalVertex<E>>();
  /* edge -> internal edge */
  private Map<E, InternalEdge<V>> edgeMap =
      new LinkedHashMap<E, InternalEdge<V>>();

  private transient Set<E> unmodifiableEdgeSet = null;
  private transient Set<V> unmodifiableVertexSet = null;

  private EdgeFactory<V, E> edgeFactory;
  
  private TopoComparator<V> topoComparator;

  private TopoOrderMap<V> topoOrderMap;

  private int maxTopoIndex = 0;
  private int minTopoIndex = 0;

  // this update count is used to keep internal topological iterators honest
  private long topologyUpdateCount = 0;

  protected DirectedAcyclicGraph() {
  }

  /**
   * @param edgeClass
   */
  public DirectedAcyclicGraph(Class<? extends E> edgeClass) {
    this(new EdgeFactory<V, E>(edgeClass));
  }

  public DirectedAcyclicGraph(EdgeFactory<V, E> ef) {
    if (ef == null) {
      throw new NullPointerException();
    }
    edgeFactory = ef;
    initialize();
  }

  /**
   * set the topoOrderMap based on the current factory, and create the
   * comparator;
   */
  private void initialize()
  {
      topoOrderMap = TopoOrderMap.getInstance();
      topoComparator = new TopoComparator<V>(topoOrderMap);
  }
  
  /**
   * Ensures that the specified vertex exists in this graph, or else throws
   * exception.
   *
   * @param v vertex
   *
   * @return <code>true</code> if this assertion holds.
   *
   * @throws NullPointerException if specified vertex is <code>null</code>.
   * @throws IllegalArgumentException if specified vertex does not exist in
   * this graph.
   */
  protected boolean assertVertexExist(V v)
  {
      if (containsVertex(v)) {
          return true;
      } else if (v == null) {
          throw new NullPointerException();
      } else {
          throw new IllegalArgumentException("no such vertex in graph");
      }
  }

  /**
   * A lazy build of edge container for specified vertex.
   * 
   * @param vertex a vertex in this graph.
   */
  private InternalVertex<E> getInternalVertex(V vertex) {
    assertVertexExist(vertex);

    InternalVertex<E> ec = vertexMap.get(vertex);

    if (ec == null) {
      ec = new InternalVertex<E>();
      vertexMap.put(vertex, ec);
    }

    return ec;
  }

  public V getEdgeSource(E e) {
    InternalEdge<V> internalEdge = edgeMap.get(e);
    // nullable?
    return internalEdge.source;
  }

  public V getEdgeTarget(E e) {
    InternalEdge<V> internalEdge = edgeMap.get(e);
    // nullable?
    return internalEdge.target;
  }

  /**
   * iterator will traverse the vertices in topological order, meaning that
   * for a directed graph G = (V,E), if there exists a path from vertex va to
   * vertex vb then va is guaranteed to come before vertex vb in the iteration
   * order.
   *
   * @return an iterator that will traverse the graph in topological order
   */
  public Iterator<V> iterator() {
    return new TopoIterator();
  }

  /**
   * Adds the specified vertex to this graph if not already present, and puts it
   * at the top of the internal topological vertex ordering. More formally, adds
   * the specified vertex, <code>v</code>, to this graph if this graph contains
   * no vertex <code>u</code> such that <code>
   * u.equals(v)</code>. If this graph already contains such vertex, the call
   * leaves this graph unchanged and returns <tt>false</tt>. In combination with
   * the restriction on constructors, this ensures that graphs never contain
   * duplicate vertices.
   * 
   * @param v vertex to be added to this graph.
   * 
   * @return <tt>true</tt> if this graph did not already contain the specified
   *         vertex.
   * 
   * @throws NullPointerException if the specified vertex is <code>
   * null</code>.
   */
  public boolean addVertex(V v) {
    boolean added = addVertexInternal(v);
    if (added) {
      // add to the top
      ++maxTopoIndex;
      topoOrderMap.putVertex(maxTopoIndex, v);

      ++topologyUpdateCount;
    }

    return added;
  }

  protected boolean addVertexInternal(V v) {
    if (v == null) {
      throw new NullPointerException();
    } else if (containsVertex(v)) {
      return false;
    } else {
      vertexMap.put(v, new InternalVertex<E>());
      return true;
    }
  }

  /**
   * <p>
   * Adds the given edge and updates the internal topological order for
   * consistency IFF
   * 
   * <ul>
   * <li>there is not already an edge (fromVertex, toVertex) in the graph
   * <li>the edge does not induce a cycle in the graph
   * </ul>
   * </p>
   * 
   * @return null if the edge is already in the graph, else the created edge is
   *         returned
   * 
   * @throws IllegalArgumentException If either fromVertex or toVertex is not a
   *           member of the graph
   * @throws CycleFoundException if the edge would induce a cycle in the graph
   */
  public E addEdge(V fromVertex, V toVertex)
      throws CycleFoundException {
    // we don't need to check the existence of those vertices here
    addVertex(fromVertex);
    addVertex(toVertex);

    Integer lb = topoOrderMap.getTopologicalIndex(toVertex);
    Integer ub = topoOrderMap.getTopologicalIndex(fromVertex);

    if (lb < ub) {
      Set<V> df = new HashSet<V>();
      Set<V> db = new HashSet<V>();

      // Discovery
      Region affectedRegion = new Region(lb, ub);
      Visited visited = Visited.getInstance(affectedRegion);

      // throws CycleFoundException if there is a cycle
      dfsF(toVertex, df, visited, affectedRegion);

      dfsB(fromVertex, db, visited, affectedRegion);
      reorder(df, db, visited);
      ++topologyUpdateCount; // if we do a reorder, than the topology has
                             // been updated
    }

    return addEdgeInternal(fromVertex, toVertex);
  }


  protected E addEdgeInternal(V fromVertex, V toVertex) {
    E e = edgeFactory.createEdge(fromVertex, toVertex);
    if (containsEdge(e)) { // this restriction should stay!
      return null;
    } else {
      InternalEdge<V> internalEdge = new InternalEdge<V>(fromVertex, toVertex);
      edgeMap.put(e, internalEdge);
      vertexMap.get(fromVertex).addOutgoingEdge(e);
      vertexMap.get(toVertex).addIncomingEdge(e);
      return e;
    }
  }

  public boolean containsEdge(E e) {
    return edgeMap.containsKey(e);
  }

  public boolean containsVertex(V v) {
    return vertexMap.containsKey(v);
  }

  public boolean removeEdge(E e) {
    if (containsEdge(e)) {
      InternalEdge<V> ie = edgeMap.get(e);
      vertexMap.get(ie.getSource()).removeOutgoingEdge(e);
      vertexMap.get(ie.getTarget()).removeOutgoingEdge(e);
      edgeMap.remove(e);
      return true;
    } else {
      return false;
    }
  }

  /**
   */
  public boolean removeAllEdges(Collection<? extends E> edges) {
    boolean modified = false;

    for (E e : edges) {
      modified |= removeEdge(e);
    }

    return modified;
  }

  /**
   * Removes the specified vertex from this graph including all its touching
   * edges if present. More formally, if the graph contains a vertex <code>
   * u</code> such that <code>u.equals(v)</code>, the call removes all edges
   * that touch <code>u</code> and then removes <code>u</code> itself. If no
   * such <code>u</code> is found, the call leaves the graph unchanged.
   * Returns <tt>true</tt> if the graph contained the specified vertex. (The
   * graph will not contain the specified vertex once the call returns).
   *
   * <p>If the specified vertex is <code>null</code> returns <code>
   * false</code>.</p>
   *
   * @param v vertex to be removed from this graph, if present.
   *
   * @return <code>true</code> if the graph contained the specified vertex;
   * <code>false</code> otherwise.
   */
  public boolean removeVertex(V v) {
    boolean removed = removeVertexInternal(v);
    if (removed) {
      Integer topoIndex = topoOrderMap.removeVertex(v);

      // contract minTopoIndex as we are able
      if (topoIndex == minTopoIndex) {
        while ((minTopoIndex < 0)
            && (null == topoOrderMap.getVertex(minTopoIndex))) {
          ++minTopoIndex;
        }
      }

      // contract maxTopoIndex as we are able
      if (topoIndex == maxTopoIndex) {
        while ((maxTopoIndex > 0)
            && (null == topoOrderMap.getVertex(maxTopoIndex))) {
          --maxTopoIndex;
        }
      }

      ++topologyUpdateCount;
    }

    return removed;
  }
  
  protected boolean removeVertexInternal(V v) {
    if (containsVertex(v)) {
      InternalVertex<E> iv = vertexMap.get(v);
      removeAllEdges(iv.getUnmodifiableIncomingEdges());
      removeAllEdges(iv.getUnmodifiableOutgoingEdges());
      vertexMap.remove(v);
      return true;
    } else {
      return false;
    }
  }
  
  public boolean removeAllVertices(Collection<? extends V> vertices) {
    boolean removed = removeAllVerticesInternal(vertices);

    topoOrderMap.removeAllVertices();

    maxTopoIndex = 0;
    minTopoIndex = 0;

    ++topologyUpdateCount;

    return removed;
  }

  /**
   */
  public boolean removeAllVerticesInternal(Collection<? extends V> vertices) {
    boolean modified = false;

    for (V v : vertices) {
      modified |= removeVertex(v);
    }

    return modified;
  }

  /**
   * @see DirectedGraph#incomingEdges(Object)
   */
  public Set<E> incomingEdgesOf(V vertex) {
    return getInternalVertex(vertex).getUnmodifiableIncomingEdges();
  }

  /**
   * @see DirectedGraph#incomingEdges(Object)
   */
  public Set<E> outgoingEdgesOf(V vertex) {
    return getInternalVertex(vertex).getUnmodifiableOutgoingEdges();
  }

  
  public Set<V> vertexSet() {
    if (unmodifiableVertexSet == null) {
      unmodifiableVertexSet = Collections.unmodifiableSet(vertexMap.keySet());
    }

    return unmodifiableVertexSet;
  }

  public Set<E> edgeSet() {
    if (unmodifiableEdgeSet == null) {
      unmodifiableEdgeSet = Collections.unmodifiableSet(edgeMap.keySet());
    }

    return unmodifiableEdgeSet;
  }
  
  
  /**
   * Depth first search forward, building up the set (df) of forward-connected
   * vertices in the Affected Region
   *
   * @param vertex the vertex being visited
   * @param df the set we are populating with forward connected vertices in
   * the Affected Region
   * @param visited a simple data structure that lets us know if we already
   * visited a node with a given topo index
   * @param topoIndexMap for quick lookups, a map from vertex to topo index in
   * the AR
   * @param ub the topo index of the original fromVertex -- used for cycle
   * detection
   *
   * @throws CycleFoundException if a cycle is discovered
   */
  private void
      dfsF(V vertex, Set<V> df, Visited visited, Region affectedRegion)
          throws CycleFoundException {
    int topoIndex = topoOrderMap.getTopologicalIndex(vertex);

    // Assumption: vertex is in the AR and so it will be in visited
    visited.setVisited(topoIndex);

    df.add(vertex);

    for (E outEdge : outgoingEdgesOf(vertex)) {
      V nextVertex = getEdgeTarget(outEdge);
      Integer nextVertexTopoIndex =
          topoOrderMap.getTopologicalIndex(nextVertex);

      if (nextVertexTopoIndex.intValue() == affectedRegion.finish) {
        // reset visited
        try {
          for (V visitedVertex : df) {
            visited.clearVisited(topoOrderMap
                .getTopologicalIndex(visitedVertex));
          }
        } catch (UnsupportedOperationException e) {
          // okay, fine, some implementations (ones that automatically
          // clear themselves out) don't work this way
        }
        throw new CycleFoundException();
      }

      // note, order of checks is important as we need to make sure the
      // vertex is in the affected region before we check its visited
      // status (otherwise we will be causing an
      // ArrayIndexOutOfBoundsException).
      if (affectedRegion.isIn(nextVertexTopoIndex)
          && !visited.getVisited(nextVertexTopoIndex)) {
        dfsF(nextVertex, df, visited, affectedRegion); // recurse
      }
    }
  }

  /**
   * Depth first search backward, building up the set (db) of back-connected
   * vertices in the Affected Region
   * 
   * @param vertex the vertex being visited
   * @param db the set we are populating with back-connected vertices in the AR
   * @param visited
   * @param topoIndexMap
   */
  private void
      dfsB(V vertex, Set<V> db, Visited visited, Region affectedRegion) {
    // Assumption: vertex is in the AR and so we will get a topoIndex from
    // the map
    int topoIndex = topoOrderMap.getTopologicalIndex(vertex);
    visited.setVisited(topoIndex);

    db.add(vertex);

    for (E inEdge : incomingEdgesOf(vertex)) {
      V previousVertex = getEdgeSource(inEdge);
      Integer previousVertexTopoIndex =
          topoOrderMap.getTopologicalIndex(previousVertex);

      // note, order of checks is important as we need to make sure the
      // vertex is in the affected region before we check its visited
      // status (otherwise we will be causing an
      // ArrayIndexOutOfBoundsException).
      if (affectedRegion.isIn(previousVertexTopoIndex)
          && !visited.getVisited(previousVertexTopoIndex)) {
        // if prevousVertexTopoIndex != null, the vertex is in the
        // Affected Region according to our topoIndexMap

        dfsB(previousVertex, db, visited, affectedRegion);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void reorder(Set<V> df, Set<V> db, Visited visited) {
    List<V> topoDf = new ArrayList<V>(df);
    List<V> topoDb = new ArrayList<V>(db);

    Collections.sort(topoDf, topoComparator);
    Collections.sort(topoDb, topoComparator);

    // merge these suckers together in topo order

    SortedSet<Integer> availableTopoIndices = new TreeSet<Integer>();

    // we have to cast to the generic type, can't do "new V[size]" in java
    // 5;
    V[] bigL = (V[]) new Object[df.size() + db.size()];
    int lIndex = 0; // this index is used for the sole purpose of pushing
                    // into

    // the correct index of bigL

    // assume (for now) that we are resetting visited
    boolean clearVisited = true;

    for (V vertex : topoDb) {
      Integer topoIndex = topoOrderMap.getTopologicalIndex(vertex);

      // add the available indices to the set
      availableTopoIndices.add(topoIndex);

      bigL[lIndex++] = vertex;

      if (clearVisited) { // reset visited status if supported
        try {
          visited.clearVisited(topoIndex);
        } catch (UnsupportedOperationException e) {
          clearVisited = false;
        }
      }
    }

    for (V vertex : topoDf) {
      Integer topoIndex = topoOrderMap.getTopologicalIndex(vertex);

      // add the available indices to the set
      availableTopoIndices.add(topoIndex);
      bigL[lIndex++] = vertex;

      if (clearVisited) { // reset visited status if supported
        try {
          visited.clearVisited(topoIndex);
        } catch (UnsupportedOperationException e) {
          clearVisited = false;
        }
      }
    }

    lIndex = 0; // reusing lIndex
    for (Integer topoIndex : availableTopoIndices) {
      // assign the indexes to the elements of bigL in order
      V vertex = bigL[lIndex++]; // note the post-increment
      topoOrderMap.putVertex(topoIndex, vertex);
    }
  }
  
  /**
   * iterator which follows topological order
   */
  private class TopoIterator implements Iterator<V> {
    private int currentTopoIndex;
    private final long updateCountAtCreation;
    private Integer nextIndex = null;

    public TopoIterator() {
      updateCountAtCreation = topologyUpdateCount;
      currentTopoIndex = minTopoIndex - 1;
    }

    public boolean hasNext() {
      if (updateCountAtCreation != topologyUpdateCount) {
        throw new ConcurrentModificationException();
      }

      nextIndex = getNextIndex();
      return nextIndex != null;
    }

    public V next() {
      if (updateCountAtCreation != topologyUpdateCount) {
        throw new ConcurrentModificationException();
      }

      if (nextIndex == null) {
        // find nextIndex
        nextIndex = getNextIndex();
      }
      if (nextIndex == null) {
        throw new NoSuchElementException();
      }
      currentTopoIndex = nextIndex;
      nextIndex = null;
      return topoOrderMap.getVertex(currentTopoIndex); // topoToVertex.get(currentTopoIndex);
    }

    public void remove() {
      if (updateCountAtCreation != topologyUpdateCount) {
        throw new ConcurrentModificationException();
      }

      V vertexToRemove = null;
      if (null != (vertexToRemove = topoOrderMap.getVertex(currentTopoIndex))) {
        topoOrderMap.removeVertex(vertexToRemove);
      } else {
        // should only happen if next() hasn't been called
        throw new IllegalStateException();
      }
    }

    private Integer getNextIndex() {
      for (int i = currentTopoIndex + 1; i <= maxTopoIndex; i++) {
        if (null != topoOrderMap.getVertex(i)) {
          return i;
        }
      }
      return null;
    }
  }

}
