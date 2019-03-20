# Google Cloud Search Norconex HTTP Collector Indexer Plugin

The Google Cloud Search Norconex HTTP Collector indexer plugin extends the [Norconex HTTP Collector](https://www.norconex.com/collectors/collector-http/)
to crawl and index content to Google Cloud Search with support for ACLs and metadata.

This connector is an implementation of the [Norconex Committer API](https://www.norconex.com/collectors/committer-core/).

## Build instructions

1. Clone the connector repository from GitHub:
   ```
   git clone https://github.com/google-cloudsearch/norconex-committer-plugin.git
   cd norconex-committer-plugin
   ```

2. Checkout the desired version of the connector and build the ZIP file:
   ```
   git checkout tags/v1-0.0.4
   mvn package
   ```
   (To skip the tests when building the connector, use `mvn package -DskipTests`)

For further information on configuration and deployment of the indexer plugin, see
[Deploy a Norconex HTTP Collector Indexer
Plugin](https://developers.google.com/cloud-search/docs/guides/norconex-http-connector).
