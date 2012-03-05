# Self-hosting SnowPlow

## Table of Contents

1. [Introduction](#intro)
2. []()

<a name="intro"/>
## Introduction

This guide takes you through the process for self-hosting SnowPlow. There are two distinct aspects to self-hosting:

1. **Self-hosting the S3 pixel** - so the tracking pixel is served from your Amazon S3 account, rather than the SnowPlow team's account 
2. **Self-hosting snowplow.js** - this means that the SnowPlow JavaScript is hosted and served by your web server(s) 

SnowPlow makes it easy for you to self-host either the S3 pixel or `snowplow.js`, or both. We look at each in turn below.

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

### Setting up Amazon S3 and CloudFront

#### 1. Download the pixel

First you need a pixel to serve - you can get one by right-clicking [this image file] [pixel] and downloading the image, or alternatively try:

    wget 



### Handling HTTPS (SSL)

**To write**


## Self-hosting snowplow.js

JavaScript can be advisable for a vareif you have your own preferred JavaScript minification scheme or prefer not to use third-party JavaScripts. The other nice thing about


[aws]: http://aws.amazon.com/
[pixel]