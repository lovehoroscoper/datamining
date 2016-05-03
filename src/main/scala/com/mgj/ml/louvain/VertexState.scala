package com.mgj.ml.louvain

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable {
  var community = -1L
  var communitySigmaTot = 0L
  // self edges
  var internalWeight = 0L
  // out degree
  var nodeWeight = 0L
  var changed = false

  override def toString = s"VertexState($community, $communitySigmaTot, $internalWeight, $nodeWeight)"
}