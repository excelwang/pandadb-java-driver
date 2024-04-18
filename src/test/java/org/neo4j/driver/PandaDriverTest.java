package org.neo4j.driver;

import org.junit.jupiter.api.Test;

public class PandaDriverTest {
    private static Driver driver = GraphDatabase.driver("bolt://panda143:7600", AuthTokens.basic("neo4j", "neo4j"));
    @Test
    public void sessionTest() {
        try (Session session = driver.session()) {
            Result result = session.run("match (n) return count(n)");
            for (Record r:  result.list()) {
                System.out.println(r.get("count(n)"));
            }
            session.close();
        }
    }
    @Test
    public void emptyResult() {
        try (Session session = driver.session()) {
            Result result = session.run("match (n:nonExisted) return n");
            for (Record r:  result.list()) {
                System.out.println(r.get("n"));
            }
        }
        driver.close();
    }
    @Test
    public void nodeResult() {
        try (Session session = driver.session()) {
            Result result = session.run("match (n) return n limit 1");
            System.out.println(result.single().get("n").asNode().asMap());
        }
        driver.close();
    }
}
