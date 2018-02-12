#!/bin/bash
set -e

/usr/bin/java -jar /usr/share/prometheus-exporter/elasticsearch-exporter.jar \
  --elasticsearch-hosts ${ELASTICSEARCH_HOSTS} \
  --elasticsearch-port ${ELASTICSEARCH_PORT} \
  --elasticsearch-cluster ${ELASTICSEARCH_CLUSTER} \
  --elasticsearch-username ${ELASTICSEARCH_USERNAME} \
  --elasticsearch-password ${ELASTICSEARCH_PASSWORD} \
  --scrape-period ${SCRAPE_PERIOD} \
  --scrape-period-unit ${SCRAPE_PERIOD_UNIT} \
  --port ${PROMETHEUS_PORT}
