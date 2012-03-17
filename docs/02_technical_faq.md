# Technical FAQ

## Questions

1. [Is SnowPlow real-time?](#rt)
2. [What's next on the roadmap?](#roadmap)
3. [I want to use SnowPlow but not Amazon CloudFront - how?](#nocloudfront)
4. [How can I contribute to SnowPlow?](#contribute)

<a name="rt"/>
## Is SnowPlow real-time?

No, currently SnowPlow is not a real-time analytics solution. This is because SnowPlow depends on CloudFront's [access logging] [cloudfrontlog] capability, and it can take 20-60 minutes (and sometimes even longer) for CloudFront access logs to appear in your Amazon S3 logging bucket. This makes the current version of SnowPlow better suited to "after the fact", batch-based analysis.

The SnowPlow team are exploring other (non-CloudFront) architectures to support a real-time analytics capability alongside (**not** replacing) the current SnowPlow platform.

<a name="roadmap"/>
## What's next on the roadmap?

Lots! We need to open-source our Hive SerDes (serializer-deserializers) for SnowPlow, and we will then start to share the first of the "recipes" for Hive analyses on SnowPlow's clickstream data.

On the ad serving analytics front, we are working on an architecture to support click-tracking.

## I want to use SnowPlow but not Amazon CloudFront - how?

<a name="nocloudfront"/>
It is possible to use SnowPlow with a self-hosted [Nginx] [nginx] webserver to handle pixel tracking rather than CloudFront. [Contact the SnowPlow team] [contact] if you want to find out more.

## How can I contribute to SnowPlow?

The SnowPlow team welcome contributions! The core team (funded by [Keplar] [keplar]) is small so we would love more people to join in and help realise our objectives of building the world's most powerful analytics platform. Stay tuned for a more detailed update on how best you can contribute to SnowPlow. 

[cloudfrontlog]: http://aws.amazon.com/cloudfront/faqs/#Can_I_get_access_to_request_logs_for_content_delivered_through_CloudFront
[nginx]: http://nginx.org
[contact]: mailto:snowplow@keplarllp.com 
[keplar]: http://www.keplarllp.com