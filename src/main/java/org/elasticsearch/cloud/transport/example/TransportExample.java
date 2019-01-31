/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cloud.transport.example;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TransportExample implements Runnable {

    private Logger logger = ESLoggerFactory.getLogger(getClass().getCanonicalName());
    private TransportClient client;

    private TransportExample(String host, int port, String cluster, boolean enableSsl, boolean ip4Enabled, boolean ip6Enabled, boolean insecure) {
        logger.info("Connecting to cluster: [{}] via [{}:{}] using ssl:[{}]", cluster, host, port, enableSsl);

        // Build the settings for our client.
        Settings settings = Settings.builder()
                .put("client.transport.nodes_sampler_interval", "5s")
                .put("client.transport.sniff", false)
                .put("transport.tcp.compress", true)
                .put("cluster.name", cluster)
                .put("xpack.security.transport.ssl.enabled", enableSsl)
                .put("request.headers.X-Found-Cluster", "${cluster.name}")
                .put("xpack.security.user", System.getProperty("xpack.security.user"))
                // For testing in dev environments, similar to `curl -k` option
                .put("xpack.security.transport.ssl.verification_mode", insecure ? "none" : "full")
                .build();

        // Instantiate a TransportClient and add the cluster to the list of addresses to connect to.
        // Only port 9343 (SSL-encrypted) is currently supported. The use of x-pack security features is required.
        this.client = new PreBuiltXPackTransportClient(settings);
        try {

            // Note: If enabling IPv6, then you should ensure that your host and network can route it to the Cloud endpoint.
            // (eg Docker disables IPv6 routing by default) - see also the system property parsing code below.
            for (InetAddress address : InetAddress.getAllByName(host)) {
                if ((ip6Enabled && address instanceof Inet6Address)
                        || (ip4Enabled && address instanceof Inet4Address)) {
                    this.client.addTransportAddress(new TransportAddress(address, port));
                }
            }
        } catch (UnknownHostException e) {
            logger.error("Unable to get the host", e.getMessage());
        }
    }

    public static void main(String[] args)  {
        String host = System.getProperty("host");
        String hostBasedClusterName = host.split("\\.", 2)[0];

        TransportExample transporter = TransportExample.builder()
                .host(host)
                .port(System.getProperty("port", "9343"))
                .cluster(System.getProperty("cluster", hostBasedClusterName))
                .enableSsl(System.getProperty("ssl", "true"))
                .ip4Enabled(System.getProperty("ip4", "true"))
                .ip6Enabled(System.getProperty("ip6", "true"))
                .insecure(System.getProperty("insecure", "true"))
                .build();

        int threads = Integer.parseInt(System.getProperty("threads", "1"));

        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            threadPool.execute(transporter);
        }
        threadPool.shutdown();

        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    private static Builder builder() {
        return new Builder();
    }

    @Override
    public void run() {
        while(true) {
            try {
                logger.info("Getting cluster health... ");
                ActionFuture<ClusterHealthResponse> healthFuture = this.client.admin().cluster().health(Requests.clusterHealthRequest());
                ClusterHealthResponse healthResponse = healthFuture.get(5, TimeUnit.SECONDS);
                logger.info("Got cluster health response: [{}]", healthResponse.getStatus());
            } catch(Throwable t) {
                logger.error("Unable to get cluster health response: [{}]", t.getMessage());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) { ie.printStackTrace(); }
        }
    }

    private static class Builder {
        private String host;
        private int port;
        private String cluster;
        private boolean enableSsl;
        private boolean ip6Enabled;
        private boolean ip4Enabled;
        private boolean insecure;

        private Builder host(String host) {
            this.host = host;
            return this;
        }

        private Builder port(String port) {
            this.port = Integer.parseInt(port);
            return this;
        }

        private Builder cluster(String cluster) {
            this.cluster = cluster;
            return this;
        }

        private Builder enableSsl(String ssl) {
            this.enableSsl = Boolean.parseBoolean(ssl);
            return this;
        }

        private Builder ip4Enabled(String ip4) {
            this.ip4Enabled = Boolean.parseBoolean(ip4);
            return this;
        }

        private Builder ip6Enabled(String ip6) {
            this.ip6Enabled = Boolean.parseBoolean(ip6);
            return this;
        }

        private Builder insecure(String insecure) {
            this.insecure = Boolean.parseBoolean(insecure);
            return this;
        }

        private TransportExample build() {
            return new TransportExample(host, port, cluster, enableSsl, ip4Enabled, ip6Enabled, insecure);
        }
    }
}
