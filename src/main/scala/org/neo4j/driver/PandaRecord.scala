package org.neo4j.driver

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.structural.{LynxNode, LynxPath, LynxRelationship}
import org.neo4j.driver.internal.value.NullValue
import org.neo4j.driver.types.Entity
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Path
import org.neo4j.driver.types.Relationship
import org.neo4j.driver.util.Pair
import org.neo4j.driver.internal.{InternalNode, InternalPair, InternalPath, InternalRelationship}

import java.util.NoSuchElementException
import java.util.function.Function
import scala.collection.JavaConverters._


class PandaRecord(private val lynxValueMap: Map[String, LynxValue]) extends Record {

  /**
   * Retrieve the keys of the underlying map
   *
   * @return all field keys in order
   */
  override def keys: java.util.List[String] = this.lynxValueMap.keys.toList.asJava

  /**
   * Check if the list of keys contains the given key
   *
   * @param key the key
   * @return {@code true} if this map keys contains the given key otherwise {@code false}
   */
  override def containsKey(key: String): Boolean = this.lynxValueMap.contains(key)

  /**
   * Retrieve the value of the property with the given key
   *
   * @param key the key of the property
   * @return the property's value or a {@link NullValue} if no such key exists
   * @throws ClientException if record has not been initialized
   */
  override def get(key: String): Value = this.lynxValueMap.get(key).map(lv => Values.value(lv.value)).getOrElse(Values.NULL)

  /**
   * Retrieve the number of entries in this map
   *
   * @return the number of entries in this map
   */
  override def size: Int = this.lynxValueMap.size

  /**
   * Retrieve the values of the underlying map
   *
   * @return all field keys in order
   */
  override def values: java.util.List[Value] = this.lynxValueMap.values.map(lv => Values.value(lv.value)).toList.asJava

  /**
   * Map and retrieve all values of the underlying collection
   *
   * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
   *                    as {@link Values# ofBoolean ( )}, {@link Values# ofList ( Function )}.
   * @return the result of mapping all values in unspecified order
   */
  override def values[T](mapFunction: Function[Value, T]): java.lang.Iterable[T] = this.lynxValueMap.values.map(lv => mapFunction(Values.value(lv.value))).asJava

  /**
   * Return the underlying map as a map of string keys and values converted using
   * {@link Value# asObject ( )}.
   * <p>
   * This is equivalent to calling {@link # asMap ( Function )} with {@link Values# ofObject ( )}.
   *
   * @return the value as a Java map
   */
  override def asMap: java.util.Map[String, AnyRef] = lynxValueMap.mapValues(_.value.asInstanceOf[AnyRef]).asJava

  /**
   * @param mapFunction a function to map from Value to T. See {@link Values} for some predefined functions, such
   *                    as {@link Values# ofBoolean ( )}, {@link Values# ofList ( Function )}.
   * @return the value as a map from string keys to values of type T obtained from mapping he original map values, if possible
   * @see Values for a long list of built-in conversion functions
   */
  override def asMap[T](mapFunction: Function[Value, T]): java.util.Map[String, T] = lynxValueMap.mapValues(lv => mapFunction(Values.value(lv.value))).asJava

  /**
   * Retrieve the index of the field with the given key
   *
   * @param key the give key
   * @return the index of the field as used by {@link # get ( int )}
   * @throws NoSuchElementException if the given key is not from {@link # keys ( )}
   */
  override def index(key: String) = this.keys.indexOf(key)

  /**
   * Retrieve the value at the given field index
   *
   * @param index the index of the value
   * @return the value or a {@link NullValue} if the index is out of bounds
   * @throws ClientException if record has not been initialized
   */
  override def get(index: Int): Value = this.lynxValueMap.get(this.keys.get(index)).map(lv => Values.value(lv.value)).getOrElse(Values.NULL)

  /**
   * Retrieve all record fields
   *
   * @return all fields in key order
   * @throws NoSuchRecordException if the associated underlying record is not available
   */
  override def fields: java.util.List[Pair[String, Value]] = this.lynxValueMap.map(kv => InternalPair.of(kv._1, Values.value(kv._2.value))).toList.asJava

  /**
   * Retrieve the value with the given key.
   * If no value found by the key, then the default value provided would be returned.
   *
   * @param key          the key of the value
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @return the value found by the key or the default value if no such key exists
   */
  override def get(key: String, defaultValue: Value): Value = this.lynxValueMap.get(key).map(lv => Values.value(lv.value)).getOrElse(defaultValue)

  /**
   * Retrieve the object with the given key.
   * If no object found by the key, then the default object provided would be returned.
   *
   * @param key          the key of the object
   * @param defaultValue the default object that would be returned if no object found by the key in the map
   * @return the object found by the key or the default object if no such key exists
   */
  override def get(key: String, defaultValue: AnyRef): AnyRef = this.lynxValueMap.get(key).map(_.value.asInstanceOf[AnyRef]).getOrElse(defaultValue)

  /**
   * Retrieve the number with the given key.
   * If no number found by the key, then the default number provided would be returned.
   *
   * @param key          the key of the number
   * @param defaultValue the default number that would be returned if no number found by the key in the map
   * @return the number found by the key or the default number if no such key exists
   */
  override def get(key: String, defaultValue: Number): Number = this.lynxValueMap.get(key).map(_.value.asInstanceOf[Number]).getOrElse(defaultValue)

  /**
   * Retrieve the entity with the given key.
   * If no entity found by the key, then the default entity provided would be returned.
   *
   * @param key          the key of the entity
   * @param defaultValue the default entity that would be returned if no entity found by the key in the map
   * @return the entity found by the key or the default entity if no such key exists
   */
  override def get(key: String, defaultValue: Entity): Entity = {
    this.lynxValueMap.get(key).map( le => le match {//TODO lynxElement should have id.
      case n: LynxNode => new InternalNode(n.id.toLynxInteger.v)
      case r: LynxRelationship => new InternalRelationship(r.id.toLynxInteger.v, r.startNodeId.toLynxInteger.v, r.endNodeId.toLynxInteger.v, r.relationType.get.value) //TODO relations must have type
      case _ => throw new NoSuchElementException("mismatched lynvalue")
    }).getOrElse(defaultValue)
  }

  /**
   * Retrieve the node with the given key.
   * If no node found by the key, then the default node provided would be returned.
   *
   * @param key          the key of the node
   * @param defaultValue the default node that would be returned if no node found by the key in the map
   * @return the node found by the key or the default node if no such key exists
   */
  override def get(key: String, defaultValue: Node): Node = {
    this.lynxValueMap.get(key).map( le => le match {
      case n: LynxNode =>
        val labels = n.labels.map(l => l.value).asJavaCollection
        val props = n.keys.map(p => p.value -> Values.value(n.property(p).get.value)).toMap.asJava
        new InternalNode(n.id.toLynxInteger.v, labels, props)
      case _ => throw new NoSuchElementException("mismatched lynvalue")
    }).getOrElse(defaultValue)
  }

  /**
   * Retrieve the path with the given key.
   * If no path found by the key, then the default path provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default path that would be returned if no path found by the key in the map
   * @return the path found by the key or the default path if no such key exists
   */
  override def get(key: String, defaultValue: Path): Path = {
    this.lynxValueMap.get(key).map( _ match {
      case p: LynxPath => {
        val entities: Seq[Entity] = p.elements.map(_ match {
          case n: LynxNode => {
            val labels = n.labels.map(l => l.value).asJavaCollection
            val props = n.keys.map(p => p.value -> Values.value(n.property(p).get.value)).toMap.asJava //todo do we need labels or props?
            new InternalNode(n.id.toLynxInteger.v, labels, props)
          }.asInstanceOf[Entity]
          case r: LynxRelationship => new InternalRelationship(r.id.toLynxInteger.v, r.startNodeId.toLynxInteger.v, r.endNodeId.toLynxInteger.v, r.relationType.get.value).asInstanceOf[Entity] //TODO relations must have type
          case _ => throw new NoSuchElementException("mismatched lynvalue")
        })
        new InternalPath(entities.asJava)
      }
      case _ => throw new NoSuchElementException("mismatched lynvalue")
      }).getOrElse(defaultValue)
  }

  /**
   * Retrieve the value with the given key.
   * If no value found by the key, then the default value provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @return the value found by the key or the default value if no such key exists
   */
  override def get(key: String, defaultValue: Relationship): Relationship = {
    this.lynxValueMap.get(key).map( le => le match { //TODO lynxElement should have id.
      case r: LynxRelationship =>{
        val props = r.keys.map(p => p.value -> Values.value(r.property(p).get.value)).toMap.asJava//todo do we need props?
        new InternalRelationship(r.id.toLynxInteger.v, r.startNodeId.toLynxInteger.v, r.endNodeId.toLynxInteger.v, r.relationType.get.value, props) //TODO relations must have type
        }
      case _ => throw new NoSuchElementException("mismatched lynvalue")
    }).getOrElse(defaultValue)
  }

  /**
   * Retrieve the list of objects with the given key.
   * If no value found by the key, then the default value provided would be returned.
   *
   * @param key          the key of the value
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @return the list of objects found by the key or the default value if no such key exists
   */
  override def get(key: String, defaultValue: java.util.List[AnyRef]): java.util.List[AnyRef] = {
    this.lynxValueMap.get(key).map(_.value.asInstanceOf[LynxList].v.map(_.value.asInstanceOf[AnyRef]).asJava).getOrElse(defaultValue)
  }

  /**
   * Retrieve the list with the given key.
   * If no value found by the key, then the default list provided would be returned.
   *
   * @param key          the key of the value
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @param mapFunc      the map function that defines how to map each element of the list from {@link Value} to T
   * @return the converted list found by the key or the default list if no such key exists
   */
  override def get[T](key: String, defaultValue: java.util.List[T], mapFunc: Function[Value, T]): java.util.List[T] = this.lynxValueMap.get(key).map(_.value.asInstanceOf[collection.immutable.List[LynxValue]].map(_.value.asInstanceOf[T]).asJava).getOrElse(defaultValue)

  /**
   * Retrieve the map with the given key.
   * If no value found by the key, then the default value provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @return the map found by the key or the default value if no such key exists
   */
  override def get(key: String, defaultValue: java.util.Map[String, AnyRef]): java.util.Map[String, AnyRef] = {
    this.lynxValueMap.get(key).map(_.value match {
      case m: Map[String, LynxValue] => m.mapValues(_.value.asInstanceOf[AnyRef]).asJava
    }).getOrElse(defaultValue)
  }

  /**
   * Retrieve the map with the given key.
   * If no value found by the key, then the default map provided would be returned.
   *
   * @param key          the key of the value
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @param mapFunc      the map function that defines how to map each value in map from {@link Value} to T
   * @return the converted map found by the key or the default map if no such key exists.
   */
  override def get[T](key: String, defaultValue: java.util.Map[String, T], mapFunc: Function[Value, T]): java.util.Map[String, T] = {
    this.lynxValueMap.get(key).map(_ match {
        case m: LynxMap => m.value.mapValues(mv => mapFunc(Values.value(mv.value))).asJava
        case _ => throw new NoSuchElementException("mismatched lynvalue")
    }).getOrElse(defaultValue)
  }

  /**
   * Retrieve the java integer with the given key.
   * If no integer found by the key, then the default integer provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default integer that would be returned if no integer found by the key in the map
   * @return the integer found by the key or the default integer if no such key exists
   */
  override def get(key: String, defaultValue: Int): Int = this.lynxValueMap.get(key).map(_.value.asInstanceOf[Int]).getOrElse(defaultValue)

  /**
   * Retrieve the java long number with the given key.
   * If no value found by the key, then the default value provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @return the java long number found by the key or the default value if no such key exists
   */
  override def get(key: String, defaultValue: Long): Long = this.lynxValueMap.get(key).map(_.value.asInstanceOf[Long]).getOrElse(defaultValue)

  /**
   * Retrieve the java boolean with the given key.
   * If no value found by the key, then the default value provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @return the java boolean found by the key or the default value if no such key exists
   */
  override def get(key: String, defaultValue: Boolean): Boolean = this.lynxValueMap.get(key).map(_.value.asInstanceOf[Boolean]).getOrElse(defaultValue)

  /**
   * Retrieve the java string with the given key.
   * If no string found by the key, then the default string provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default string that would be returned if no string found by the key in the map
   * @return the string found by the key or the default string if no such key exists
   */
  override def get(key: String, defaultValue: String): String = this.lynxValueMap.get(key).map(_.value.asInstanceOf[String]).getOrElse(defaultValue)

  /**
   * Retrieve the java float number with the given key.
   * If no value found by the key, then the default value provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @return the java float number found by the key or the default value if no such key exists
   */
  override def get(key: String, defaultValue: Float): Float = this.lynxValueMap.get(key).map(_.value.asInstanceOf[Float]).getOrElse(defaultValue)

  /**
   * Retrieve the java double number with the given key.
   * If no value found by the key, then the default value provided would be returned.
   *
   * @param key          the key of the property
   * @param defaultValue the default value that would be returned if no value found by the key in the map
   * @return the java double number found by the key or the default value if no such key exists
   */
  override def get(key: String, defaultValue: Double): Double = this.lynxValueMap.get(key).map(_.value.asInstanceOf[Double]).getOrElse(defaultValue)
}
