package org.neo4j.driver

import com.google.protobuf.ByteString
import org.grapheco.lynx.lynxrpc.{LynxByteBufFactory, LynxValueDeserializer, LynxValueSerializer}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural.{HasProperty, LynxNode, LynxPath, LynxRelationship}
import org.grapheco.pandadb.network.Query.{QueryRequest, QueryResponse}
import org.neo4j.driver.internal.value.{NodeValue, PathValue, RelationshipValue}
import org.neo4j.driver.internal.{InternalNode, InternalPath, InternalRelationship}
import org.neo4j.driver.types.Entity

import scala.collection.JavaConverters._

object PandaConverter {//TODO directly from protobuffer to neovalue

  private lazy val lynxDeserializer = new LynxValueDeserializer()
  private lazy val lynxSerializer = new LynxValueSerializer()
  private lazy val byteBuf = LynxByteBufFactory.getByteBuf

  def toNeoValue(lynxValue: LynxValue): Value = lynxValue match {
    case n: LynxNode => toNodeValue(n) //TODO lynxElement should have id.
    case r: LynxRelationship => toRelationValue(r)
    case p: LynxPath => new PathValue(new InternalPath(p.elements.map(e => toNeoValue(e.asInstanceOf[LynxValue]).asInstanceOf[Entity]).asJava)) //TODO lynxElement should be LynxValue.
    case lv: LynxValue => Values.value(lv.value) //todo add point, time etc
  }

  def convertResponse2NeoRecord(r: QueryResponse): Record = {
    lynxDeserializer.decodeLynxValue(byteBuf.writeBytes(r.getResultInBytes.toByteArray)) match {
      case map: LynxMap => {
        if (map.value.isEmpty) return null
        new PandaRecord(map.value)
      }
      case _ => throw new Exception("QueryResponse is not a Map")
    }
  }


  def convertResponse2NeoValues(r: QueryResponse): Array[Value] = {
    lynxDeserializer.decodeLynxValue(byteBuf.writeBytes(r.getResultInBytes.toByteArray)) match {
      case map: LynxMap => {
        if (map.value.isEmpty) return Array.empty[Value]
        map.value.mapValues(toNeoValue(_)).values.toArray
      }
      case _ => throw new Exception("QueryResponse is not a Map")
    }
  }

  def convertQuery(query: Query): QueryRequest = {
    val qb = QueryRequest.newBuilder.setStatement(query.text)
    query.parameters.asMap().forEach((k, v) => {
      val nv = v match {
        case l: java.util.List[Any] => lynxSerializer.encodeAny(l.toArray) //TODO change lynx source code.
        case v => lynxSerializer.encodeAny(v)
      }
      qb.putParameters(k, ByteString.copyFrom(nv))
    })
    qb.build()
  }

  private def toNodeValue(n: LynxNode) = {
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
