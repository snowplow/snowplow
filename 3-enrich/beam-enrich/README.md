# beam-enrich

## Raison d'être:

TODO: your project description

## Features:

This project comes with number of preconfigured features, including:

### sbt-native-packager

Use `sbt-native-packager` instead of `sbt-assembly` to:
 * reduce build time
 * enable efficient dependency caching
 * reduce job submission time

To build package run:

```
sbt universal:packageBin
```

### Running

Once unzipped the artifact can be run as follows:

```bash
/bin/snowplow-beam-enrich \
  --runner=DataFlowRunner \
  --streaming=true \
  --zone=europe-west2-a \
  --gcpTempLocation=gs://location/ \
  --input=projects/project/topics/raw-topic \
  --output=projects/project/topics/enriched-topic \
  --bad=projects/project/topics/bad-topic \
  --resolver=iglu_resolver.json \
  --enrichments=enrichments/
```

Note that, for the enrichments relying on local files, the files need to be accessible from Google
Cloud Storage, e.g. for the IP lookups enrichment:

```json
{
  "schema": "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
  "data": {
    "name": "ip_lookups",
    "vendor": "com.snowplowanalytics.snowplow",
    "enabled": true,
    "parameters": {
      "geo": {
        "database": "GeoLite2-City.mmdb",
        "uri": "gs://ben-sp-test/maxmind"
      }
    }
  }
}
```

### Testing

This template comes with an example of a test, to run tests:

```
sbt test
```

### Scala style

Find style configuration in `scalastyle-config.xml`. To enforce style run:

```
sbt scalastyle
```

### REPL

To experiment with current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply:

```
sbt repl/run
```

---

This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
