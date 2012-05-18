package org.apache.hadoop.realtime.dag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For performance and flexibility uses an ArrayList for topological index to
 * vertex mapping, and a HashMap for vertex to topological index mapping.
 */
class TopoOrderMap<V> {

  private final List<V> topoToVertex = new ArrayList<V>();
  private final Map<V, Integer> vertexToTopo =
      new HashMap<V, Integer>();

  public static <V> TopoOrderMap<V> getInstance() {
    return new TopoOrderMap<V>();
  }

  public void putVertex(Integer index, V vertex) {
    int translatedIndex = translateIndex(index);

    // grow topoToVertex as needed to accommodate elements
    while ((translatedIndex + 1) > topoToVertex.size()) {
      topoToVertex.add(null);
    }

    topoToVertex.set(translatedIndex, vertex);
    vertexToTopo.put(vertex, index);
  }

  public V getVertex(Integer index) {
    return topoToVertex.get(translateIndex(index));
  }

  public Integer getTopologicalIndex(V vertex) {
    return vertexToTopo.get(vertex);
  }

  public Integer removeVertex(V vertex) {
    Integer topoIndex = vertexToTopo.remove(vertex);
    if (topoIndex != null) {
      topoToVertex.set(translateIndex(topoIndex), null);
    }
    return topoIndex;
  }

  public void removeAllVertices() {
    vertexToTopo.clear();
    topoToVertex.clear();
  }

  /**
   * We translate the topological index to an ArrayList index. We have to do
   * this because topological indices can be negative, and we want to do it
   * because we can make better use of space by only needing an ArrayList of
   * size |AR|.
   * 
   * @param unscaledIndex
   * 
   * @return the ArrayList index
   */
  private final int translateIndex(int index) {
    if (index >= 0) {
      return 2 * index;
    }
    return -1 * ((index * 2) - 1);
  }
}