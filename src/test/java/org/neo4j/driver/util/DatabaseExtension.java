/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.util;

import org.junit.jupiter.api.extension.*;
import org.neo4j.driver.*;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.types.TypeSystem;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.driver.util.Neo4jSettings.*;
import static org.neo4j.driver.util.Neo4jSettings.BoltTlsLevel.OPTIONAL;

public class DatabaseExtension implements ExecutionCondition, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private static final DatabaseExtension instance;
    private static final URI pandaUri = URI.create("bolt://localhost:7601");
    private static final AuthToken authToken;

    private static final Map<String, String> defaultConfig;

    private static final Driver driver;

    static {
        instance = new DatabaseExtension();
        defaultConfig = new HashMap<>();
        defaultConfig.put(SSL_POLICY_BOLT_ENABLED, "true");
        defaultConfig.put(SSL_POLICY_BOLT_CLIENT_AUTH, "NONE");
        defaultConfig.put(BOLT_TLS_LEVEL, OPTIONAL.toString());

        authToken = AuthTokens.basic("panda", "panda");
        driver = GraphDatabase.driver(pandaUri, authToken);
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return ConditionEvaluationResult.enabled("Server is available");
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
//        TestUtil.cleanDb(driver);
        System.out.println("beforeEach");
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {//todo notworking
        System.out.println("afterEach");
    }

    @Override
    public void afterAll(ExtensionContext context) {
        System.out.println("afterAll");
//        deleteAndStartNeo4j(Collections.emptyMap());
    }

    public String addImportFile(String prefix, String suffix, String contents) throws IOException {
        File tmpFile = File.createTempFile(prefix, suffix, null);
        tmpFile.deleteOnExit();
        try (PrintWriter out = new PrintWriter(tmpFile)) {
            out.println(contents);
        }
        return String.format("file:///%s", tmpFile.getName());
    }

    public void deleteAndStartNeo4j(Map<String, String> config) {
    }

    public Driver driver() {
        return driver;
    }

    public TypeSystem typeSystem() {
        return driver.defaultTypeSystem();
    }

    public URI uri() {
        return pandaUri;
    }

    public int boltPort() {
        return pandaUri.getPort();
    }

    public AuthToken authToken() {
        return authToken;
    }

    public String adminPassword() {
        return "panda";
    }

    public BoltServerAddress address() {
        return new BoltServerAddress(pandaUri);
    }

    public boolean isNeo4j44OrEarlier() {
        return true;
    }

    public static DatabaseExtension getInstance() {
        return instance;
    }

}
