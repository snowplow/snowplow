$(function() {
	$(".button").click(function() {
		// First clear the output div of any contents (e.g. from tags that were inserted last time the form was submitted)
		$('#output').html("");
		alert('starting function!');
		var applicationId = $("input#applicationId").val();
		var cloudfrontSubdomain = $("input#cloudfrontSubdomain").val();
		var selfHostedCollectorUrl = $("input#selfHostedCollectorUrl").val();
		var pageTitle = $("input#pageTitle").val();
		var pageUrl = $("input#pageUrl").val();
		alert('variables grabbed');
		var embedCode = generateNoJsTag(applicationId, selfHostedCollectorUrl, pageTitle, pageUrl);
		alert('embed code generated');
		$('#output').append($('<h3>The embed code is:</h3><br /><br /><h2>' + embedCode + '</h2>'));

		return false;
	});


	function generateNoJsTag(appId, collectorDomain, pageTitle, pageUrl){
		// 1st, let's generate the collectorUrl
		configCollectorUrl = asCollectorUrl(collectorDomain);

		// 2nd generate the request string
		request = generateRequestString(appId, pageTitle, pageUrl);

		// 3rd assemble the tag out of the above two
		tag = '<img src="' + configCollectorUrl + '?' + request + '" />' ;

		// 4th return the tag, html-escaped so it prints to the screen, rather than actually executing in the browser
		return htmlEscape(tag);
	};

	function asCollectorUrl(rawUrl) {
		// Add an option in the form to see if page being tracked is HTTPS (so need to replace the `http` below with `https`)?
		return ( 'http://' + rawUrl + '/i' );               
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