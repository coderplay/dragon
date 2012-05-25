package org.apache.hadoop.realtime.dag;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Note, this is a lazy and incomplete implementation, with assumptions that
 * inputs are in the given topoIndexMap
 */
class TopoComparator<V> implements Comparator<V>, Serializable{
  private static final long serialVersionUID = 7091580997211005045L;

  private TopoOrderMap<V> topoOrderMap;

  public TopoComparator(TopoOrderMap<V> topoOrderMap) {
    this.topoOrderMap = topoOrderMap;
  }

  public int compare(V v1, V v2) {
    return topoOrderMap.getTopologicalIndex(v1).compareTo(
        topoOrderMap.getTopologicalIndex(v2));
  }
}