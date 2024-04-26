package org.neo4j.driver

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{HasProperty, LynxNode, LynxPath, LynxRelationship}
import org.neo4j.driver.internal.{InternalNode, InternalPath, InternalRelationship}
import org.neo4j.driver.internal.value.{NodeValue, PathValue, RelationshipValue}
import org.neo4j.driver.types.Entity

import scala.collection.JavaConverters._

object PandaConverter {//TODO directly from protobuffer to neovalue

  def toNeoValue(lynxValue: LynxValue): Value = lynxValue match {
    case n: LynxNode => toNodeValue(n) //TODO lynxElement should have id.
    case r: LynxRelationship => toRelationValue(r)
    case p: LynxPath => new PathValue(new InternalPath(p.elements.map(e => toNeoValue(e.asInstanceOf[LynxValue]).asInstanceOf[Entity]).asJava)) //TODO lynxElement should be LynxValue.
    case lv: LynxValue => Values.value(lv.value) //todo add point, time etc
  }

  private def toNodeValue(n: LynxNode): NodeValue = {
    val id = n.id.toLynxInteger.v
    val labels = n.labels.map(_.value)
    val node = new InternalNode(id, labels.asJavaCollection, fetchEntituProps(n))
    new NodeValue(node)
  }

  private def toRelationValue(r: LynxRelationship): RelationshipValue = {
    val id = r.id.toLynxInteger.v
    val start = r.startNodeId.toLynxInteger.v
    val end = r.endNodeId.toLynxInteger.v
    val rType = r.relationType.get.value //TODO relations must have type
    val rel = new InternalRelationship(id, start, end, rType, fetchEntituProps(r))
    new RelationshipValue(rel)
  }

  private def fetchEntituProps(p: HasProperty): java.util.HashMap[String, Value] = {
    val props = new java.util.HashMap[String, Value]
    p.keys.map(k => props.put(k.value, Values.value(p.property(k).get.value)))
    props
  }

}
