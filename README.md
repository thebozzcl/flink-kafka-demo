# Apache Flink demo

This is a simple, self-contained Apache Flink demo I put together. It runs a simple pipeline that:

1. Reads raw text from a Kafka topic
2. Splits the text into words
3. Aggregates words in a word-count tuple over fixed 15-second windows
4. Outputs the word counts to a Kafka topic

## Sources

I wrote the docker-compose.yaml file based on the following examples:
* [Apache Flink documentation](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/resource-providers/standalone/docker/#flink-with-docker-compose)
* [Kafka-UI example config](https://github.com/provectus/kafka-ui/blob/master/documentation/compose/kafka-ui.yaml)
* Random bits and pieces found online

I copied `build.gradle` from [the official Flink documentation](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/configuration/overview/#getting-started).

I generated the pipeline code using Claude 3.7, then tweaked it and fixed some issues.

## Instructions

Before you start, you need to install the following:

* Docker
* Docker Compose
* Some flavor of Java 17 (I like GraalVM, personally)

Once that's ready, follow these instructions to run the demo:

1. Build the pipeline jar with `./gradlew build shadowJar`
2. Deploy the Docker resources with `docker compose up -d`
3. Wait until everything starts. The `jobmanager` container might fail once before starting successfully because Docker Compose isn't great at handling container startup order
4. Your Flink cluster can be accessed at `http://localhost:8081`. You can use it to view the status of the job.
5. The Kafka UI can be accessed at `http://localhost:8080`. Here you can manipulate two topics:
   * `input-topic`: you can push messages with any text you like
   * `output-topic`: the word-counted output will show up here
