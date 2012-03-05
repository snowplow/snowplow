# Integrating snowplow.js

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
```

To explain a few things about this code:

* You must update `{{CLIENT}}` to be your specific sub-domain provided by the SnowPlow team
* This code work with both `https` (SSL-secured) and `http` pages
* The `trackPageView` command logs the page load 

### Event tracking

Having set up the asynchronous tracking above, you can now add SnowPlow's asynchronous event tracking into your website or webapp.

Here is an example of SnowPlow event tracking attached to a simple JavaScript action:

```html
<!-- Website event with SnowPlow tracking -->
<script type="text/javascript">
    function playVideo(){
        alert("Playing a video")
        _spq.push(['trackEvent', 'Videos', 'Play', 'Fargo', '320x200'])
    }
</script>
```

The four arguments to the event tracking command may be familiar to you from the Google Analytics API - however there are some small differences, so please refer to the section **Event tracking** below for more information.

Any problems? Please consult the **Testing and troubleshooting** section at the bottom of this guide.

## Synchronous integration

This section still to write.

## Event tracking

This section still to write.

## Testing and troubleshooting

This section still to write.

[selfhosted]: http://todo