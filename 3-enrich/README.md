# Enrich

![architecture][architecture-image]

**Enrich** phase takes Snowplow raw events logged by the [collector][collector],
cleans them up, enriches them and puts them in the queue ready for [storage][storage].

For GCP Beam Enrich ([docs][docs-beam-enrich]) is used
while on AWS Steam Enrich ([docs][docs-stream-enrich]) is used.
They are both part on [enrich][enrich] project.

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-3-enrichment.png

[collector]: https://github.com/snowplow/stream-collector/

[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage

[docs-beam-enrich]: https://docs.snowplowanalytics.com/docs/setup-snowplow-on-gcp/setup-validation-and-enrich-beam-enrich/
[docs-stream-enrich]: https://docs.snowplowanalytics.com/docs/setup-snowplow-on-aws/setup-validation-and-enrich/
[enrich]: https://github.com/snowplow/enrich