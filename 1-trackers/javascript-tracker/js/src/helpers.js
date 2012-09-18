/*
 * Is property defined?
 */
SnowPlow.isDefined = function (property) {
	return typeof property !== 'undefined';
}

/*
 * Is property a function?
 */
SnowPlow.isFunction = function (property) {
	return typeof property === 'function';
}

/*
 * Is property an object?
 *
 * @return bool Returns true if property is null, an Object, or subclass of Object (i.e., an instanceof String, Date, etc.)
 */
SnowPlow.isObject = function (property) {
	return typeof property === 'object';
}

/*
 * Is property a string?
 */
SnowPlow.isString = function (property) {
	return typeof property === 'string' || property instanceof String;
}

/*
 * UTF-8 encoding
 */
SnowPlow.encodeUtf8 = function (argString) {
	return SnowPlow.urldecode(SnowPlow.encodeWrapper(argString));
}


/*
 * Fix-up URL when page rendered from search engine cache or translated page.
 * TODO: it would be nice to generalise this and/or move into the ETL phase.
 */
SnowPlow.urlFixup = function (hostName, href, referrer) {
	/*
	 * Extract parameter from URL
	 */
	function getParameter(url, name) {
		// scheme : // [username [: password] @] hostame [: port] [/ [path] [? query] [# fragment]]
		var e = new RegExp('^(?:https?|ftp)(?::/*(?:[^?]+)[?])([^#]+)'),
			matches = e.exec(url),
			f = new RegExp('(?:^|&)' + name + '=([^&]*)'),
			result = matches ? f.exec(matches[1]) : 0;

		return result ? SnowPlow.decodeWrapper(result[1]) : '';
	}

	/*
	 * Extract hostname from URL
	 */
	function getHostName(url) {
		// scheme : // [username [: password] @] hostame [: port] [/ [path] [? query] [# fragment]]
		var e = new RegExp('^(?:(?:https?|ftp):)/*(?:[^@]+@)?([^:/#]+)'),
			matches = e.exec(url);

		return matches ? matches[1] : url;
	}


	if (hostName === 'translate.googleusercontent.com') {		// Google
		if (referrer === '') {
			referrer = href;
		}
		href = getParameter(href, 'u');
		hostName = getHostName(href);
	} else if (hostName === 'cc.bingj.com' ||					// Bing
			hostName === 'webcache.googleusercontent.com' ||	// Google
			hostName.slice(0, 5) === '74.6.') {					// Yahoo (via Inktomi 74.6.0.0/16)
		href = SnowPlow.documentAlias.links[0].href;
		hostName = getHostName(href);
	}
	return [hostName, href, referrer];
}

/*
 * Get page referrer
 */
SnowPlow.getReferrer = function () {
	var referrer = '';

	try {
		referrer = SnowPlow.windowAlias.top.document.referrer;
	} catch (e) {
		if (SnowPlow.windowAlias.parent) {
			try {
				referrer = SnowPlow.windowAlias.parent.document.referrer;
			} catch (e2) {
				referrer = '';
			}
		}
	}
	if (referrer === '') {
		referrer = SnowPlow.documentAlias.referrer;
	}

	return referrer;
}

/*
 * Fix-up domain
 */
SnowPlow.domainFixup = function (domain) {
	var dl = domain.length;

	// remove trailing '.'
	if (domain.charAt(--dl) === '.') {
		domain = domain.slice(0, dl);
	}
	// remove leading '*'
	if (domain.slice(0, 2) === '*.') {
		domain = domain.slice(1);
	}
	return domain;
}

/*
 * Cross-browser helper function to add event handler
 */
SnowPlow.addEventListener = function (element, eventType, eventHandler, useCapture) {
	if (element.addEventListener) {
		element.addEventListener(eventType, eventHandler, useCapture);
		return true;
	}
	if (element.attachEvent) {
		return element.attachEvent('on' + eventType, eventHandler);
	}
	element['on' + eventType] = eventHandler;
}

/*
 * Get cookie value
 */
SnowPlow.getCookie = function (cookieName) {
	var cookiePattern = new RegExp('(^|;)[ ]*' + cookieName + '=([^;]*)'),
			cookieMatch = cookiePattern.exec(SnowPlow.documentAlias.cookie);

	return cookieMatch ? SnowPlow.decodeWrapper(cookieMatch[2]) : 0;
}

/*
 * Set cookie value
 */
SnowPlow.setCookie = function (cookieName, value, msToExpire, path, domain, secure) {
	var expiryDate;

	// relative time to expire in milliseconds
	if (msToExpire) {
		expiryDate = new Date();
		expiryDate.setTime(expiryDate.getTime() + msToExpire);
	}

	SnowPlow.documentAlias.cookie = cookieName + '=' + SnowPlow.encodeWrapper(value) +
		(msToExpire ? ';expires=' + expiryDate.toGMTString() : '') +
		';path=' + (path || '/') +
		(domain ? ';domain=' + domain : '') +
		(secure ? ';secure' : '');
}

/*
 * Call plugin hook methods
 */
SnowPlow.executePluginMethod = function (methodName, callback) {
	var result = '',
			i,
			pluginMethod;

	for (i in SnowPlow.plugins) {
		if (Object.prototype.hasOwnProperty.call(SnowPlow.plugins, i)) {
			pluginMethod = SnowPlow.plugins[i][methodName];
			if (SnowPlow.isFunction(pluginMethod)) {
				result += pluginMethod(callback);
			}
		}
	}

	return result;
}

