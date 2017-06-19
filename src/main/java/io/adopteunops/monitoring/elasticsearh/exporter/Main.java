/*
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
package io.adopteunops.monitoring.elasticsearh.exporter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.adopteunops.monitoring.prometheus.ExposePrometheusMetricsServer;
import io.prometheus.client.exporter.MetricsServlet;
import org.compuscene.metrics.prometheus.PrometheusMetricsCollector;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class Main {
    @Parameter(names = "--elasticsearch-hosts", description = "Elasticsearch hostnames", required = true)
    public List<String> elasticsearchHostnames;
    @Parameter(names = "--elasticsearch-port", description = "Elasticsearch port")
    public int elasticsearchPort = 9300;
    @Parameter(names = "--elasticsearch-cluster", required = true)
    public String clusterName;
    @Parameter(names = "--elasticsearch-username", required = true)
    public String username;
    @Parameter(names = "--elasticsearch-password", required = true)
    public String password;
    @Parameter(names = "--port", description = "Exporter port")
    public int port = 7979;
    @Parameter(names = "--help", help = true)
    public boolean help = false;

    public static void main(String... args) throws UnknownHostException {
        Main main = new Main();
        JCommander jCommander = new JCommander(main);

        jCommander.parse(args);

        if (main.help) {
            jCommander.usage();
        } else {
            Settings.Builder settingsBuilder = Settings.builder();
            settingsBuilder.put("cluster.name", main.clusterName);
//            settingsBuilder.put("xpack.security.user", main.username + ":" + main.password);
            TransportClient client = new PreBuiltTransportClient(settingsBuilder.build());
            for (String elasticsearchHostname : main.elasticsearchHostnames) {
                client = client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticsearchHostname), main.elasticsearchPort));
            }
            PrometheusMetricsCollector collector = new PrometheusMetricsCollector(Settings.EMPTY, client);
            MetricsServlet metricsServlet = new MetricsServlet() {
                @Override
                protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                    collector.updateMetrics();
                    super.doGet(req, resp);
                }
            };

            ExposePrometheusMetricsServer prometheusMetricServlet = new ExposePrometheusMetricsServer(main.port, metricsServlet);
            prometheusMetricServlet.start();

        }
    }


}
