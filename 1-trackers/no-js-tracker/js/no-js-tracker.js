$(function() {
	var trackerVersion = 'no-js-0.0.1';

	/**
	 * On page load... 
	 */
	// 1. Display the title including tracker version
	$('#title').append($('<h1>No-Javascript Tracker for SnowPlow</h1><h2>No-JS tracking tag generator version ' + trackerVersion + '</h2>'));
	
	// 2. Set the radio buttons to default options i.e. scheme = HTTP and collectorType = cloudfront...
	$('input:radio[name=pageScheme]:nth(0)').attr('checked',true);
	$('input:radio[name=collectorType]:nth(0)').attr('checked',true);
	
	// 3.  Hide the div with the other collector input field
	$('#other-collector-div').hide();

	/**
	 * When a user selects 'other collector type' then hide the input fields that are only relevant for the 
	 * Cloudfront collector and display the fields that are relevant for the self-hosted collector type
	 */
	$("#otherCollectorType").click(function(){
		$('#other-collector-div').show();
		$('#cloudfront-collector-div').hide();
	});

	/**
	 * Conversley, when a user selects Cloudfront collector type then show the relevant fields and hide the irrelevant fields
	 */
	$("#cloudfrontCollectorType").click(function(){
		$('#other-collector-div').hide();
		$('#cloudfront-collector-div').show();
	});

	/**
	 * The following function generates the embed code when the visitor clicks on the "Generate embed code" button
	 */
	$(".button").click(function() {
		// First clear the output div of any contents (e.g. from tags that were inserted last time the form was submitted)
		$('#output').html("");
		
		// Now pass the variables in each of the fields into the Javascript, so we can use them to generate the tracking tag
		var applicationId = $("input#applicationId").val();
		// var pageScheme = $("input#pageScheme").val();
		var pageScheme = $('input:radio[name=pageScheme]:checked').val();
		
		var pageTitle = $("input#pageTitle").val();
		var pageUrl = $("input#pageUrl").val();

		// var collectorType = $("input#collectorType").val();
		var collectorType = $('input:radio[name=collectorType]:checked').val();
		
		var cloudfrontSubdomain = $("input#cloudfrontSubdomain").val();
		var selfHostedCollectorUrl = $("input#selfHostedCollectorUrl").val();
	
		
		var embedCode = generateNoJsTag(applicationId, pageScheme, pageTitle, pageUrl, collectorType, cloudfrontSubdomain, selfHostedCollectorUrl);
		alert('embed code generated');
		$('#output').append($('<h3>The No-JS tracking tag for this page is:</h3><h2>' + embedCode + '</h2>'));

		// Return false because we do NOT want the page to reload. 
		// (Which would cause the values entered in the fields to be lost, and the embed code to disappear)
		return false;
	});

	/**
	 * Generates the tag, based on the values inputted on the form above
	 */
	function generateNoJsTag(appId, pageScheme, pageTitle, pageUrl, collectorType, cloudFrontSubDomain, collectorDomain ){
		// 1st, let's set the endpoint
		var configCollectorUrl;

		if (collectorType == 'cloudfront') {
			configCollectorUrl = collectorUrlFromCfDist(cloudFrontSubDomain, pageScheme);
		} else {
			configCollectorUrl = asCollectorUrl(collectorDomain, pageScheme);
		}

		// 2nd generate the request string
		request = generateRequestString(appId, pageTitle, pageUrl, pageScheme);

		// 3rd assemble the tag out of the above two
		tag = '<img src="' + configCollectorUrl + '?' + request + '" />' ;

		// 4th return the tag, html-escaped so it prints to the screen, rather than actually executing in the browser
		return htmlEscape(tag);
	};

	/**
	 * Builds a collector URL from a CloudFront distribution.
	 */
	function collectorUrlFromCfDist(distSubdomain, pageScheme) {
		return asCollectorUrl(distSubdomain + '.cloudfront.net', pageScheme);
	}

	/** 
	 * Returns the collector end point based on the raw URL
	 */
	function asCollectorUrl(rawUrl, pageScheme) {
		// Add an option in the form to see if page being tracked is HTTPS (so need to replace the `http` below with `https`)?
		return pageScheme + '://' + rawUrl + '/i';
	}

	function generateRequestString(appId, pageTitle, pageUrl, pageScheme){
		// Need to fixup pageTitle AND encode URL prior to code below
		sb = new requestStringBuilder();

		sb.add('e','pv'); // 'pv' for Page View	
		sb.add('page', pageTitle); 
		sb.add('url', (pageScheme + '://' + pageUrl));
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