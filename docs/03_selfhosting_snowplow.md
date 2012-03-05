# Self-hosting SnowPlow

## Table of Contents

1. [Introduction](#intro)
2. [Self-hosting the S3 pixel](#pixelsh)
3. [Self-hosting snowplow.js](#jssh)

<a name="intro"/>
## Introduction

This guide takes you through the process for self-hosting SnowPlow. There are two distinct aspects to self-hosting:

1. **Self-hosting the S3 pixel** - so the tracking pixel is served from your Amazon S3 account, rather than the SnowPlow team's account 
2. **Self-hosting snowplow.js** - this means that the SnowPlow JavaScript is hosted and served by your web server(s) 

SnowPlow makes it easy for you to self-host either the S3 pixel or `snowplow.js`, or both. We look at each in turn below.

<a name="pixelsh"/>
## Self-hosting the S3 pixel

### Overview

`snowplow.js` logs every page view and tracking event by making a `GET` request to a single transparent 1x1 pixel; by default this pixel is made available at:

    http://{{CLIENT}}.snplow.com/pxl.png

Where `{{CLIENT}} is the sub-domain assigned to you by SnowPlow. This approach means that the data collected by SnowPlow is stored within our Amazon S3 account, rather than yours. If you prefer to self-host the S3 pixel and store your SnowPlow data within your own Amazon S3 account, that's straightforward, and we will explore how to do it in the rest of this section.

### Pre-requisites

If you want to self-host the S3 pixel, you will need the following:

* An account with [Amazon Web Services] [aws] account, with S3 and CloudFront enabled
* The ability to setup a (sub)domain for SnowPlow, and set a CloudFront CNAME entry for it
* Some level of technical ability _e_, where `noob < e < ninja`

Once you have those ready, please read on...

### Self-hosting instructions

#### 1. Create a bucket for the pixel

First create a new bucket within your Amazon S3 account to store the pixel:

[!createbucket]

Some notes on this:

* There is no need to enable logging on this bucket
* Because we will be using CloudFront, it doesn't particularly matter which data center you choose (although see [A note on privacy](#privacy) below)

#### 2. Add the pixel

First you need a pixel to serve - you can obtain one by right-clicking [this image file] [pixel] and selecting **Save Link As...**, or alternatively run:

```bash
wget https://github.com/tychosoftworks/snowplow-js/raw/master/pxl/pxl.png 	
```

Now you're ready to upload the pixel into S3:

[!uploadpixel]

Once uploaded, make sure that the permissions on this image are set to read:

[!pixelsecurity]

#### 3. Configure CloudFront

**To write**

#### 4. Update your header script

**To write**

### Handling HTTPS (SSL)

**To write**

<a name="privacy"/>
### A note on privacy



<a name="jssh"/>
## Self-hosting snowplow.js

JavaScript can be advisable for a vareif you have your own preferred JavaScript minification scheme or prefer not to use third-party JavaScripts. The other nice thing about

[aws]: http://aws.amazon.com/
[pixel]: https://github.com/tychosoftworks/snowplow-js/raw/master/pxl/pxl.png