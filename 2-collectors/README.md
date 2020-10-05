# Collectors

![architecture][architecture-image]

The [collector][collector] receives Snowplow events from one or more [trackers][trackers].

A Collector captures and logs these events in their raw form, ready to be processed by Snowplow [enrich][enrich] ([AWS][enrich-AWS] or [GCP][enrich-GCP]) phase.

[architecture-image]: https://d3i6fms1cm1j0i.cloudfront.net/github-wiki/images/snowplow-architecture-2-collectors.png

[trackers]: https://docs.snowplowanalytics.com/docs/setup-snowplow-on-aws/setup-trackers/

[enrich]: https://github.com/snowplow/enrich
[enrich-AWS]:https://docs.snowplowanalytics.com/docs/setup-snowplow-on-aws/setup-validation-and-enrich/
[enrich-GCP]: https://docs.snowplowanalytics.com/docs/setup-snowplow-on-gcp/setup-validation-and-enrich-beam-enrich/
