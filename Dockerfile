FROM java:8u111-jdk

ENV ELASTICSEARCH_HOSTS elasticsearch
ENV ELASTICSERCH_PORT 9300
ENV ELASTICSEARCH_CLUSTER docker-cluster
ENV ELASTICSEARCH_USERNAME elastic
ENV ELASTICSEARCH_PASSWORD changeme
ENV SCRAPE_PERIOD 1
ENV SCRAPE_PERIOD_UNIT MINUTES
ENV PROMETHEUS_PORT 7979

ADD startup.sh /usr/bin/startup.sh

ENTRYPOINT ["/usr/bin/startup.sh"]

# Add the service itself
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/prometheus-exporter/elasticsearch-exporter.jar
