package org.neo4j.driver

import org.junit.jupiter.api.Test


object Neo4jDriverTest {
  private val driver = GraphDatabase.driver("bolt://panda143:7600", AuthTokens.basic("neo4j", "neo4j"))
}

class Neo4jDriverTest {
  @Test def sessionTest(): Unit = {
    try {
      val session = Neo4jDriverTest.driver.session
      try {
        val result = session.run("match (n) return count(n)")
        import scala.collection.JavaConversions._
        for (r <- result.list) {
          System.out.println(r.get("count(n)"))
        }
        session.close()
      } finally if (session != null) session.close()
    }
  }

  @Test def emptyResult(): Unit = {
    try {
      val session = Neo4jDriverTest.driver.session
      try {
        val result = session.run("match (n:nonExisted) return n")
        import scala.collection.JavaConversions._
        for (r <- result.list) {
          System.out.println(r.get("n"))
        }
      } finally if (session != null) session.close()
    }
    Neo4jDriverTest.driver.close()
  }

  @Test def nodeResult(): Unit = {
    try {
      val session = Neo4jDriverTest.driver.session
      try {
        val result = session.run("match (n) return n limit 1")
        System.out.println(result.single.get("n").asNode.asMap)
      } finally if (session != null) session.close()
    }
    Neo4jDriverTest.driver.close()
  }
}