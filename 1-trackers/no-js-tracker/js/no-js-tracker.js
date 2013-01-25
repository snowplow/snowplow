$(function() {
	/**
	 * The following function generates the embed code when the visitor clicks on the "Generate embed code" button
	 */
	$(".button").click(function() {
		// First clear the output div of any contents (e.g. from tags that were inserted last time the form was submitted)
		$('#output').html("");
		alert('starting function!');

		// Now pass the variables in each of the fields into the Javascript, so we can use them to generate the tracking tag
		var applicationId = $("input#applicationId").val();

		var isHttps;
		if ($("input#is-https").is(':checked')) {
			isHttps = true;
		} else {
			isHttps = false;
		};
		alert('isHttps = ' + isHttps);

		var pageTitle = $("input#pageTitle").val();
		var pageUrl = $("input#pageUrl").val();

		var isCloudfrontCollector
		if ($("input#is-cloudfront-collector").is(':checked')) {
			isCloudfrontCollector = true;
		} else {
			isCloudfrontCollector = false
		}

		var cloudfrontSubdomain = $("input#cloudfrontSubdomain").val();
		var selfHostedCollectorUrl = $("input#selfHostedCollectorUrl").val();
	
		alert('variables grabbed');

		var embedCode = generateNoJsTag(applicationId, isHttps, pageTitle, pageUrl, isCloudfrontCollector, cloudfrontSubdomain, selfHostedCollectorUrl);
		alert('embed code generated');
		$('#output').append($('<h3>The No-JS tracking tag for this page is:</h3><br /><br /><h2>' + embedCode + '</h2>'));

		// Return false because we do NOT want the page to reload. 
		// (Which would cause the values entered in the fields to be lost, and the embed code to disappear)
		return false;
	});

	/**
	 * Generates the tag, based on the values inputted on the form above
	 */
	function generateNoJsTag(appId, isHttps, pageTitle, pageUrl, isCloudfrontCollector, cloudFrontSubDomain, collectorDomain ){
		// 1st, let's set the endpoint
		var configCollectorUrl;

		if (isCloudfrontCollector) {
			configCollectorUrl = collectorUrlFromCfDist(cloudFrontSubDomain, isHttps);
		} else {
			configCollectorUrl = asCollectorUrl(collectorDomain, isHttps);
		}

		// 2nd generate the request string
		request = generateRequestString(appId, pageTitle, pageUrl);

		// 3rd assemble the tag out of the above two
		tag = '<img src="' + configCollectorUrl + '?' + request + '" />' ;

		// 4th return the tag, html-escaped so it prints to the screen, rather than actually executing in the browser
		return htmlEscape(tag);
	};

	/**
	 * Builds a collector URL from a CloudFront distribution.
	 */
	function collectorUrlFromCfDist(distSubdomain, isHttps) {
		return asCollectorUrl(distSubdomain + '.cloudfront.net', isHttps);
	}

	/** 
	 * Returns the collector end point based on the raw URL
	 */
	function asCollectorUrl(rawUrl, isHttps) {
		// Add an option in the form to see if page being tracked is HTTPS (so need to replace the `http` below with `https`)?
		return 'http' + (isHttps ? 's' : '') + '://' + rawUrl + '/i';
	}

	function generateRequestString(appId, pageTitle, pageUrl){
		// Need to fixup pageTitle AND encode URL prior to code below
		sb = new requestStringBuilder();

		sb.add('e','pv'); // 'pv' for Page View	
		sb.add('page', pageTitle); 
		sb.add('url', pageUrl);
		sb.add('aid', appId); 

		sb.add('p', 'web');
		sb.add('tv', 'no-js-0.1.0') // Update to set this in a config file rather than hardcode, and display on the web page

		var request = sb.build();

		return request;
	}


	/**
	 * A helper to build a SnowPlow request string from an
	 * an optional initial value plus a set of individual
	 * name-value pairs, provided using the add method.
	 *
	 * @param string initialValue The initial querystring, ready to have additional key-value pairs added
	 *
	 * @return object The request string builder, with add, addRaw and build methods
	 */
	function requestStringBuilder(initialValue) {
		var str = initialValue || '';
		var addNvPair = function(key, value, encode) {
			if (value !== undefined && value !== '') {
				str += '&' + key + '=' + (encode ? window.encodeURIComponent(value) : value);
			}
		};
		return {
			add: function(key, value) {
				addNvPair(key, value, true);
			},
			addRaw: function(key, value) {
				addNvPair(key, value, false);
			},
			build: function() {
				return str;
			}
		}
	}


	/**
	 * Escapes HTML - used so that the tag is printed to the 
	 * screen (so it can be copied / pasted), rather than actually executed by the browser
	 */
	function htmlEscape(str) {
    return String(str)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }
	
});