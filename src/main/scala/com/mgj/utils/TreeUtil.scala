package com.mgj.utils

import java.util
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import scala.collection.JavaConversions._

/**
  * Created by xiaonuo on 3/11/16.
  */
object TreeUtil {
  def traversal(node: Node, splitValues: util.ArrayList[Double]): Unit = {
    if (node.isLeaf) {
      return
    } else {
      splitValues.add(node.split.get.threshold)
      traversal(node.leftNode.get, splitValues)
      traversal(node.rightNode.get, splitValues)
    }
  }

  def getSplitValue(model: DecisionTreeModel): List[Double] = {
    val list = new util.ArrayList[Double]()
    traversal(model.topNode, list)
    list.add(Double.MinValue)
    list.add(Double.MaxValue)
    val listSorted = list.sortWith((a, b) => a < b).toList
    return listSorted
  }
}
