package org.neo4j.driver;

import org.junit.jupiter.api.Test;

public class PandaDriverTest {
    private static Driver driver = GraphDatabase.driver("bolt://panda143:7600", AuthTokens.basic("neo4j", "neo4j"));
    @Test
    public void sessionTest() {
        try (Session session = driver.session()) {
            Result result = session.run("match (n) return count(n)");
            for (Record r:  result.list()) {
                System.out.println(r.get(0));
                break;
            }
        }
        driver.close();
    }
}
