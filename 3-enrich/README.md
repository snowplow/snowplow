# Enrich

![architecture][architecture-image]

**Enrich** phase takes Snowplow raw events logged by the [collector][collector],
cleans them up, enriches them and puts them in the queue ready for [storage][storage].

For GCP, Enrich PubSub ([docs][docs-gcp-enrich]) is used
while on AWS, Enrich Kinesis ([docs][docs-aws-enrich]) is used.
They are both part on [enrich][enrich] project.

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-3-enrichment.png

[collector]: https://github.com/snowplow/stream-collector/

[storage]: https://github.com/snowplow/snowplow/tree/master/4-storage

[docs-gcp-enrich]: https://docs.snowplow.io/docs/getting-started-on-snowplow-open-source/setup-snowplow-on-gcp/setup-validation-and-enrich/
[docs-aws-enrich]: https://docs.snowplow.io/docs/getting-started-on-snowplow-open-source/setup-snowplow-on-aws/setup-validation-enrich/
[enrich]: https://github.com/snowplow/enrich