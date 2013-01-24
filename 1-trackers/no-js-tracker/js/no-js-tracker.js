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
		$('#output').append($('<h2>The embed code is:' + embedCode + '</h2>'));

		return false;
	});


	function generateNoJsTag(appId, collectorDomain, pageTitle, pageUrl){
		// 1st, let's generate the collectorUrl
		configCollectorUrl = asCollectorUrl(collectorDomain);

		request = generateRequestString(appId, pageTitle, pageUrl);

		return configCollectorUrl + '?' + request;
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
		sb.add('tv', 'nojs-0.1.0') // Update to set this in a config file rather than hardcode, and display on the web page

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
				str += '&' + key + '=' + value;
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
});