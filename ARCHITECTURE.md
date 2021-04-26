# The typical architecture of a Snowplow pipeline

[![Snowplow architecture][architecture-image]][docs]

The diagram above illustrates a typical Snowplow pipeline with data flowing left to right.

- The [Snowplow Trackers][tracker-docs] enable highly customizable collection of raw, un-opinionated event data. Additionally, web hooks allow third-party software to send their own internal event streams to your Collector for further processing.
- Events hit the [Collector][collector], which saves the raw events to storage (S3 on AWS and Google Cloud Storage on GCP) and then sends them down the pipeline to Enrich.
- The [Enrichment][enrichment] process validates these events against a JSONSchema - to guarantee a high quality dataset - and adds information via both standard and custom enrichments. When an event fails to validate it will feed into a bad data stream which contains all of your Failed Events – in this way the Snowplow pipeline is non-lossy as all failed events can be reprocessed.
- Once validated, the data is then made available in-stream for real-time processing, and can also be loaded to blob storage and data warehouse for analysis.
- [Loaders][loaders] then load your data off the real-time streams into the various destinations you have set up for your pipeline. Typically these include a Data Warehouse (Redshift, Snowflake, BigQuery) but can also include many other destinations.
- The Snowplow atomic data acts as an immutable log of all the actions that occurred across your digital products. You can now start your analysis and event processing with our [Analytics SDKs][analytics-sdks] or setup [data models][data-models] to take that data and transform it into a set of derived tables optimized for analysis.

## Why do we use this architecture?

Snowplow’s distinctive architecture has been informed by a set of key design principles:

1. **Extreme scalability** – Snowplow should be able to scale to tracking billions of customer events without affecting the performance of your client (e.g. website) or making it difficult to subsequently analyze all of those events
2. **Permanent event history** – Snowplow events should be stored in a simple, non-relational, immutable data store
3. **Direct access to individual events** – you should have direct access to your raw Snowplow event data at the atomic level
4. **Separation of concerns** – event tracking and event analysis should be two separate systems, only loosely-coupled
5. **Support any analysis** – Snowplow should make it easy for business analysts, data scientists and engineers to answer any business question they want, using as wide a range of analytical tools as possible

The Snowplow approach has several technical advantages over more conventional web analytics approaches. In no particular order, these advantages are:

- **Scalable, fast tracking** – using the scalable Snowplow Collector for event tracking reduces complexity
- **Never lose your raw data** – your raw event data is never compacted, overwritten or otherwise corrupted by Snowplow
- **Direct access to events** – not intermediated by a third-party vendor, or a slow API, or an interface offering aggregates only
- **Analysis tool agnostic** – Snowplow can be used to feed whatever analytics process you want (e.g. BI, Kafka, Hive, R)
- **Integrable with other data sources** – join Snowplow data into your other data sources (e.g. e-commerce, CRM) at the event level
- **Clean separation of tracking and analysis** – new analyses will not require re-tagging of your site or app

[architecture-image]: media/snowplow_architecture.png
[docs]: https://docs.snowplowanalytics.com

[tracker-docs]: https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/
[collector]: https://docs.snowplowanalytics.com/docs/getting-started-on-snowplow-open-source/setup-snowplow-on-aws/setup-the-snowplow-collector/
[enrichment]: https://docs.snowplowanalytics.com/docs/enriching-your-data/what-is-enrichment/
[loaders]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/loaders-storage-targets/
[analytics-sdks]: https://docs.snowplowanalytics.com/docs/modeling-your-data/analytics-sdk/
[data-models]: https://github.com/snowplow/data-models
