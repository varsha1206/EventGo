FROM confluentinc/cp-kafka-connect:7.6.0

# Install the file connector
COPY plugins/filestream /usr/share/confluent-hub-components/filestream
COPY plugins/mongo /usr/share/confluent-hub-components/mongo

# Switch to root to create directories
USER root
 
RUN mkdir -p /data/output && \
    chown -R appuser:appuser /data/output
 
# Switch back to default user
USER appuser

