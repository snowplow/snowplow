# Technical FAQ

## Questions

1. [Is SnowPlow real-time?](#rt)
2. [Does SnowPlow have a graphical user interface](#gui) to enable me to analyse and visualise web analytics data?
3. [What's next on the roadmap?](#roadmap)
4. [I want to use SnowPlow but not Amazon CloudFront - how?](#nocloudfront)
5. [How can I contribute to SnowPlow?](#contribute)
6. [Does implementing SnowPlow impact the performance of my site e.g. page load times?](#performance)

<a name="rt"/>
## Is SnowPlow real-time?

No, currently SnowPlow is not a real-time analytics solution. This is because SnowPlow depends on CloudFront's [access logging] [cloudfrontlog] capability, and it can take 20-60 minutes (and sometimes even longer) for CloudFront access logs to appear in your Amazon S3 logging bucket. This makes the current version of SnowPlow better suited to "after the fact", batch-based analysis.

The SnowPlow team are exploring other (non-CloudFront) architectures to support a real-time analytics capability alongside (**not** replacing) the current SnowPlow platform.

<a name="gui"/>
## Does SnowPlow have a graphical user interface to enable me to analyse and visualise web analytics data?

No, currently SnowPlow does not have a GUI. Analysts who want to query data collected by SnowPlow can use Hive, Pig or write MapReduce tasks in Java / Hadoop.

There are a number of companies working to build GUIs to work on top of Hadoop. We are watching these developments closely, and hope that to make it easy to integrate these front-ends with SnowPlow in the future, to enable analysts less comfortable with e.g. Hive to use SnowPlow.

We are also looking at possibilities of building GUIs to perform repeatable analyses that we see are popular amongst the SnowPlow community. However, we do not believe in general purpose GUIs for web analytics: the whole point of SnowPlow is to free the experienced analyst from the constraints of GUIs (with all their assumptions about how the analyst does and does not want to slice the data), so analysts can have maximum flexibility to slice, dice and model data to his / her heart's content.

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

<a name="performance"/>
## Does implementing SnowPlow on my site effect site performance e.g. page load times?

SnowPlow will have an impact on site performance, just as implementing any javascript-based tracking (e.g. another web analytics package) will impact site performance. However, we have done everything we can to minimise the effect on site performance.

Pages tracked using SnowPlow have to load the SnowPlow.js file. By hosting this page on Amazon's Cloudfront, the time takent to load the javascript is minimised. In addition, users have the choice to implement syncronous and asyncrounous tracking tags: if users wants to minimise the impact on page load times, for example, they should employ async tracking.