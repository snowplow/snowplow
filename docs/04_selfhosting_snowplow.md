# Self-hosting SnowPlow

## Table of Contents

1. [Introduction](#intro)
2. [Self-hosting the tracking pixel](#pixelsh)
3. [Self-hosting snowplow.js](#jssh)
5. [A note on privacy](#privacy)

<a name="intro"/>
## Introduction

This guide takes you through the process for self-hosting SnowPlow. There are two distinct aspects to self-hosting:

1. **Self-hosting the S3 pixel** - so that the tracking pixel is served from your Amazon S3 account, rather than the SnowPlow team's account 
2. **Self-hosting snowplow.js** - so that the SnowPlow JavaScript is hosted and served by your web server(s) 

SnowPlow makes it easy for you to self-host either the S3 pixel or `snowplow.js`, or both. We look at each in turn below - but first please make sure that you have read and understood [Integrating `snowplow.js` into your site] [integrating] - the below will make little sense without.

<a name="pixelsh"/>
## Self-hosting the tracking pixel

### Overview

If the SnowPlow team provide you with an account ID, then the data collected by SnowPlow will be stored within our Amazon S3 account, rather than yours. If you prefer, you can self-host the tracking pixel and store your SnowPlow data within your own Amazon S3 account. This is straightforward to do, and we will explore how to do it in the rest of this section.

### Pre-requisites

If you want to self-host the tracking pixel, you will need the following:

* An account with [Amazon Web Services] [aws]
* S3 and CloudFront enabled within your AWS account
* Some level of technical ability _e_, where `noob < e < ninja`

Once you have those ready, please read on...

### Self-hosting instructions

#### 1. Create a bucket for the pixel

First create a new bucket within your Amazon S3 account to store the pixel. Call this bucket `snowplow-static`:

[!pixelbucket]

A couple of notes on this:

* Don't enable logging on this bucket
* Because we will be using CloudFront, it doesn't particularly matter which data center you choose (although see [A note on privacy](#privacy) below)

#### 2. Create a bucket for the CloudFront logging

Now let's create a second bucket - this will store our CloudFront logs - i.e. our actual SnowPlow data. Call this bucket `snowplow-logs`:

[!logbucket]

Again, no need to enable logging on this bucket.

#### 3. Upload a tracking pixel

You can obtain a 1x1 transparent tracking pixel by right-clicking [this image file] [pixel] and selecting **Save Link As...**, or if you prefer run:

    $ wget https://github.com/snowplow/snowplow/raw/master/tracker/static/ice.png 	

Now you're ready to upload the pixel into S3:

[!uploadpixel]

Once uploaded, make sure that the permissions on this image allow anyone to Read:

[!pixelsecurity]

#### 4. Create your CloudFront distribution

Now create the CloudFront distribution:

[!createdistrib]

Write down your CloudFront distribution's URL - e.g. `http://d1x5tduoxffdr7.cloudfront.net`. You will need this in the next step.

That's it - you now have a CloudFront distribution which will serve your tracking pixel fast to anybody anywhere in the world and log the request to Amazon S3 in your `snowplow-logs` bucket. 

#### 5. Testing your CloudFront distribution

Before testing, take a 10 minute coffee or brandy break (that's how long CloudFront takes to synchronize).

Done? Now just check that you can access your pixel using a browser, `wget` or `curl`:

    http://{{SUBDOMAIN}}.cloudfront.net/ice.png
    https://{{SUBDOMAIN}}.cloudfront.net/ice.png

If you have any problems, then double-check your CloudFront distribution's URL, and check the permissions on your pixel: it must be publicly Readable.

#### 6. Update your header script

Now you need to update the JavaScript code for SnowPlow in your website's `<head>` section to your custom tracking pixel. We're assuming here that you have already followed the steps in [Integrating `snowplow.js` into your site] [integrating].

The secret is to realise that SnowPlow's `setAccount()` method actually just takes a CloudFront subdomain as its argument. So using your own CloudFront distribution is super simple. If you are using asynchronous tracking and your CloudFront distribution's URL is `http://d1x5tduoxffdr7.cloudfront.net`, then update your header script to look like this:

```html
<!-- SnowPlow starts plowing -->
<script type="text/javascript">
var _snaq = _snaq || [];

_snaq.push(['setAccount', 'd1x5tduoxffdr7']);
...
```

Whereas if you are using synchronous tracking, then 

**Yali to add**

The secret is to understand that `snowplow.js` logs every page view and tracking event by making a `GET` request to a single transparent 1x1 pixel (`ice.png`) which is hosted on a CloudFront distribution. When you are setting your account with `setAccount()` in the SnowPlow header script, you are really just specifying which CloudFront distribution subdomain to fetch the pixel from: 

    http://{{ACCOUNT}}.cloudfront.net/ice.png

In other words: it's easy to 

**To write**

### Handling HTTPS (SSL)

**To write**

<a name="jssh"/>
## Self-hosting snowplow.js

JavaScript can be advisable for a vareif you have your own preferred JavaScript minification scheme or prefer not to use third-party JavaScripts. The other nice thing about

<a name="tt"/>
## Testing and troubleshooting

<a name="privacy"/>
## A note on privacy

[aws]: http://aws.amazon.com/
[pixel]: https://github.com/snowplow/snowplow-js/raw/master/tracker/static/ice.png 	
[selfhosting]: /snowplow/snowplow/blob/master/docs/04_selfhosting_snowplow.md