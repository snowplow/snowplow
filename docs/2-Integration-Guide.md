# snowplow.js Integration Guide

## Introduction

This guide takes you through the process for integration SnowPlow's JavaScript tracker (`snowplow.js`) into your website or web app.

This guide assumes that you are working with the hosted version of `snowplow.js` - you will need to make some adjustments if you are bundling `snowplow.js` into your own site JavaScript; these adjustments are discussed separately in the [Self-Hosting Guide] [selfhosted]

The exact integration steps required vary depending on whether you choose to use `snowplow.js` in a synchronous or an asynchronous manner. Each option is covered separately below.

## Asynchronous integration

### The header script

To use `snowplow.js` in an 'async' manner, first add the following script into your website template's `<head>` section:

```html
<!-- SnowPlow starts plowing -->
<script type="text/javascript">
var _spq = _spq || [];

_spq.push(['setTrackerUrl', 'http://{{CLIENT}}.snplow.com/pxl.png']);
_spq.push(['trackPageView']);

(function() {
var sp = document.createElement('script'); sp.type = 'text/javascript'; sp.async = true; sp.defer = true;
sp.src = ('https:' == document.location.protocol ? 'https' : 'http') + '://js.snplow.com/sp.js';
var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(sp, s);
})();
 </script>
<!-- SnowPlow stops plowing -->

To explain a few things about this code:

* You must update `{{CLIENT}}` to be your specific sub-domain provided by the SnowPlow team
* This code work with both https and http pages
* The `trackPageView` line logs the page load 

## Event tracking

Having set up a 

## Synchronous integration

This section still to write

[selfhosted]: http://todo