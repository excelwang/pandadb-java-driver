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

import static org.neo4j.driver.util.Neo4jSettings.BOLT_TLS_LEVEL;
import static org.neo4j.driver.util.Neo4jSettings.BoltTlsLevel.OPTIONAL;
import static org.neo4j.driver.util.Neo4jSettings.SSL_POLICY_BOLT_CLIENT_AUTH;
import static org.neo4j.driver.util.Neo4jSettings.SSL_POLICY_BOLT_ENABLED;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import org.bouncycastle.asn1.x509.GeneralName;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.driver.util.CertificateUtil.CertificateKeyPair;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.shaded.com.google.common.base.Preconditions;
import org.testcontainers.utility.MountableFile;
import scala.xml.dtd.REQUIRED;

public class DatabaseExtensionBackup implements ExecutionCondition, BeforeEachCallback, AfterEachCallback, AfterAllCallback {
    private static final int PANDA_PORT = 7600;

    private static final String PANDA_VER = "0.4.8";


    private static final String PANDA_SERVER_BIN_DIR_ENV_NAME = "PANDA_SERVER_BIN_DIR";

    private static final boolean dockerAvailable;
    private static final DatabaseExtension instance;
    private static final URI pandaUri;
    private static final AuthToken authToken;
    private static final File cert;
    private static final File key;
    private static final Map<String, String> defaultConfig;

    private static GenericContainer<?> pandaContainer;
    private static Driver driver;

    static {
        dockerAvailable = isDockerAvailable();
        instance = new DatabaseExtension();
        defaultConfig = new HashMap<>();
        defaultConfig.put(SSL_POLICY_BOLT_ENABLED, "true");
        defaultConfig.put(SSL_POLICY_BOLT_CLIENT_AUTH, "NONE");
        defaultConfig.put(BOLT_TLS_LEVEL, OPTIONAL.toString());

        if (dockerAvailable) {
            CertificateKeyPair<File, File> pair = generateCertificateAndKey();
            cert = pair.cert();
            key = pair.key();

            pandaContainer = setupPandaContainer(cert, key, defaultConfig);
            pandaContainer.start();
            pandaUri = URI.create(String.format("panda://" + getHost() + ":" + getMappedPort(PANDA_PORT)));
            authToken = AuthTokens.basic("panda", "panda");
            driver = GraphDatabase.driver(pandaUri, authToken);
            waitForBoltAvailability();
        } else {
            // stub init, this is not usable when Docker is unavailable
            pandaUri = URI.create("");
            authToken = AuthTokens.none();
            cert = new File("");
            key = new File("");
        }
    }

    private static String getHost(){
       return DockerClientFactory.instance().dockerHostIpAddress();
    }

    private static int getMappedPort(int originalPort){
        Preconditions.checkState(pandaContainer.getContainerId() != null, "Mapped port can only be obtained after the container is started");
        Ports.Binding[] binding = new Ports.Binding[0];
        InspectContainerResponse containerInfo = pandaContainer.getContainerInfo();
        if (containerInfo != null) {
            binding = (Ports.Binding[])containerInfo.getNetworkSettings().getPorts().getBindings().get(new ExposedPort(originalPort));
        }

        if (binding != null && binding.length > 0 && binding[0] != null) {
            return Integer.valueOf(binding[0].getHostPortSpec());
        } else {
            throw new IllegalArgumentException("Requested port (" + originalPort + ") is not mapped");
        }
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return dockerAvailable
                ? ConditionEvaluationResult.enabled("Docker is available")
                : ConditionEvaluationResult.disabled("Docker is unavailable");
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        TestUtil.cleanDb(driver);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
//        System.out.println(pandaContainer.getLogs());
    }

    @Override
    public void afterAll(ExtensionContext context) {
//        deleteAndStartNeo4j(Collections.emptyMap());
    }

    public Driver driver() {
        return driver;
    }

    public TypeSystem typeSystem() {
        return driver.defaultTypeSystem();
    }

    public void deleteAndStartNeo4j(Map<String, String> config) {
        Map<String, String> updatedConfig = new HashMap<>(defaultConfig);
        updatedConfig.putAll(config);

        pandaContainer.stop();
        pandaContainer = setupPandaContainer(cert, key, updatedConfig);
        pandaContainer.start();
        if (REQUIRED.toString().equals(config.get(BOLT_TLS_LEVEL))) {
            driver = GraphDatabase.driver(
                    pandaUri,
                    authToken,
                    Config.builder()
                            .withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(cert))
                            .withEncryption()
                            .build());
        } else {
            driver = GraphDatabase.driver(pandaUri, authToken);
        }
        waitForBoltAvailability();
    }

    public String addImportFile(String prefix, String suffix, String contents) throws IOException {
        File tmpFile = File.createTempFile(prefix, suffix, null);
        tmpFile.deleteOnExit();
        try (PrintWriter out = new PrintWriter(tmpFile)) {
            out.println(contents);
        }
        Path tmpFilePath = tmpFile.toPath();
        Path targetPath =
                Paths.get("/var/lib/neo4j/import", tmpFilePath.getFileName().toString());
        pandaContainer.copyFileToContainer(MountableFile.forHostPath(tmpFilePath), targetPath.toString());
        return String.format("file:///%s", tmpFile.getName());
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

    public void updateEncryptionKeyAndCert(File key, File cert) {
        System.out.println("Updated neo4j key and certificate file.");
        pandaContainer.stop();
        pandaContainer = setupPandaContainer(cert, key, defaultConfig);
        pandaContainer.start();
        driver = GraphDatabase.driver(pandaUri, authToken);
        waitForBoltAvailability();
    }

    public File tlsCertFile() {
        return cert;
    }

//    public void startProxy() {
//        try {
//            nginx.execInContainer("nginx");
//        } catch (IOException | InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        nginxRunning = true;
//    }
//
//    public void stopProxy() {
//        try {
//            nginx.execInContainer("nginx", "-s", "stop");
//        } catch (IOException | InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        nginxRunning = false;
//    }

//    private boolean isNeo4jVersionOrEarlier(int major, int minor) {
//        try (Session session = driver.session()) {
//            String neo4jVersion = session.readTransaction(
//                    tx -> tx.run("CALL dbms.components() YIELD versions " + "RETURN versions[0] AS version")
//                            .single()
//                            .get("version")
//                            .asString());
//            String[] versions = neo4jVersion.split("\\.");
//            return parseInt(versions[0]) <= major && parseInt(versions[1]) <= minor;
//        }
//        return false;
//    }

    public boolean isNeo4j44OrEarlier() {
        return true;
    }

    public static DatabaseExtension getInstance() {
        return instance;
    }

    public static GeneralName getDockerHostGeneralName() {
        String host = DockerClientFactory.instance().dockerHostIpAddress();
        GeneralName generalName;
        try {
            generalName = new GeneralName(GeneralName.iPAddress, host);
        } catch (IllegalArgumentException e) {
            generalName = new GeneralName(GeneralName.dNSName, host);
        }
        return generalName;
    }

    private static CertificateKeyPair<File, File> generateCertificateAndKey() {
        try {
            return CertificateUtil.createNewCertificateAndKey(getDockerHostGeneralName());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static GenericContainer<?> setupPandaContainer(File cert, File key, Map<String, String> config) {
        var image = new ImageFromDockerfile()
                .withFileFromPath(".", FileSystems.getDefault().getPath("/home/wanghuajin/pandadb-bin")) //System.getenv(PANDA_SERVER_BIN_DIR_ENV_NAME)))
                .withDockerfileFromBuilder(builder ->
                        builder
                                .from("openjdk:11")
                                .copy("pandadb-server-"+PANDA_VER,"pandadb-server")
                                .entryPoint("/pandadb-server/bin/pandadb start && tail -f /pandadb-server/logs/pandadb.log")
                                .expose(PANDA_PORT)
                                .build());
        return new GenericContainer(image).
                withExposedPorts(PANDA_PORT).
                withReuse(true);
                //withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(DatabaseExtension.class)));
    }

//    private static GenericContainer<?> setupNginxContainer() {
//        ImageFromDockerfile extendedNginxImage = new ImageFromDockerfile()
//                .withDockerfileFromBuilder(builder -> builder.from("nginx:1.23.0-alpine")
//                        .copy("nginx.conf", "/etc/nginx/")
//                        .build())
//                .withFileFromClasspath("nginx.conf", "nginx.conf");
//
//        //noinspection rawtypes
//        return new GenericContainer(extendedNginxImage.get())
//                .withNetwork(network)
//                .withExposedPorts(BOLT_PORT, HTTP_PORT)
//                .withCommand("sh", "-c", "nginx && while sleep 3600; do :; done");
//    }

    private static void waitForBoltAvailability() {
        int maxAttempts = 600;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                driver.verifyConnectivity();
                return;
            } catch (RuntimeException verificationException) {
                if (attempt == maxAttempts - 1) {
                    throw new RuntimeException(
                            "Timed out waiting for Neo4j to become available over Bolt", verificationException);
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException interruptedException) {
                    interruptedException.addSuppressed(verificationException);
                    throw new RuntimeException(
                            "Interrupted while waiting for Neo4j to become available over Bolt", interruptedException);
                }
            }
        }
    }

    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }
}
