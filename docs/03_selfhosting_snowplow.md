# Self-hosting SnowPlow

## Table of Contents

1. [Introduction](#intro)
2. []()

<a name="intro"/>
## Introduction

This guide takes you through the process for self-hosting SnowPlow. There are two distinct aspects to self-hosting:

1. **Self-hosting the S3 pixel** - self-hosting the S3 pixel means that the SnowPlow tracking logs are stored within _your_ Amazon S3 account, rather than SnowPlow's
2. **Self-hosting `snowplow.js`** - this means that the SnowPlow JavaScript is delivered by your servers as part of your JavaScript asset pipeline 

SnowPlow makes it easy for you to self-host either the S3 pixel or `snowplow.js`, or both. We look at each in turn below.

## Self-hosting the S3 pixel

### Overview

`snowplow.js` logs a page view or tracking event by making a `GET` request to a single 1x1 pixel; by default this pixel is made available at:

```html
http://{{CLIENT}}.snplow.com/pxl.png
```

where `{{CLIENT}} is the sub-domain assigned to you by SnowPlow. If you prefer to self-host a S3 pixel, that's straightforward too, and we will explore how to do it in the rest of this section.

### Pre-requisites

If you want to self-host the S3 pixel, you will need the following:

* An account with [Amazon Web Services] account, with S3 and CloudFront enabled
* The ability to create a subdomain on your website for SnowPlow, and set a CNAME entry for it
* Some level of technical expertise _e_, where noob < _e_ < ninja 

The advantages and disadvantages to self-hosting the S3 pixel are listed below:

| **+ives of self-hosting**    | **-ives** |
|:-----------:|:--------------|
| Keep your clickstream data in your own Amazon S3 account | Have to setup your own Amazon S3 account
| No commercial relationship required with SnowPlow | Have to manually configure CloudFront (explained below)          |
| Can perform any analysis you want | xx         |
|    xx | xx           |
|  xx | xx           |
|     xx | xx            |

If you want to self-host the S3 pixel, read-on

This guide assumes that you are working with the hosted version of `snowplow.js` - you will need to make some adjustments if you are bundling `snowplow.js` into your own site's JavaScript; these adjustments are discussed separately in the [Self-Hosting Guide] [selfhosted]

JavaScript can be advisable for a vareif you have your own preferred JavaScript minification scheme or prefer not to use third-party JavaScripts. The other nice thing about