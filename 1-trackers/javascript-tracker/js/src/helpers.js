/*
 * JavaScript tracker for Snowplow: helpers.js
 * 
 * Significant portions copyright 2010 Anthon Pang. Remainder copyright 
 * 2012-2013 Snowplow Analytics Ltd. All rights reserved. 
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are 
 * met: 
 *
 * * Redistributions of source code must retain the above copyright 
 *   notice, this list of conditions and the following disclaimer. 
 *
 * * Redistributions in binary form must reproduce the above copyright 
 *   notice, this list of conditions and the following disclaimer in the 
 *   documentation and/or other materials provided with the distribution. 
 *
 * * Neither the name of Anthon Pang nor Snowplow Analytics Ltd nor the
 *   names of their contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY 
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
 * Is property a date?
 */
SnowPlow.isDate = function (property) {
	return Object.prototype.toString.call(property) === "[object Date]";
}

/*
 * UTF-8 encoding
 */
SnowPlow.encodeUtf8 = function (argString) {
	return SnowPlow.decodeUrl(SnowPlow.encodeWrapper(argString));
}

/**
 * Cleans up the page title
 */
SnowPlow.fixupTitle = function (title) {
	if (!SnowPlow.isString(title)) {
		title = title.text || '';

		var tmp = SnowPlow.documentAlias.getElementsByTagName('title');
		if (tmp && SnowPlow.isDefined(tmp[0])) {
			title = tmp[0].text;
		}
	}
	return title;
}

/*
 * Extract hostname from URL
 */
SnowPlow.getHostName = function (url) {
	// scheme : // [username [: password] @] hostname [: port] [/ [path] [? query] [# fragment]]
	var e = new RegExp('^(?:(?:https?|ftp):)/*(?:[^@]+@)?([^:/#]+)'),
		matches = e.exec(url);

	return matches ? matches[1] : url;
}

/*
 * Extract suffix from a property
 */
SnowPlow.getPropertySuffix = function (property) {
	var e = new RegExp('\\$(.[^\\$]+)$'),
	    matches = e.exec(property);

	if (matches) return matches[1];
}

/*
 * Converts a date object to Unix timestamp with or without milliseconds
 */
SnowPlow.toTimestamp = function (date, milliseconds) {
	return milliseconds ? date / 1 : Math.floor(date / 1000);
}

/*
 * Converts a date object to Unix datestamp (number of days since epoch)
 */
SnowPlow.toDatestamp = function (date) {
	return Math.floor(date / 86400000);
}

/*
 * Translates a value of an unstructured date property
 */
SnowPlow.translateDateValue = function (date, type) {
  switch (type) {
    case 'tms':
      return SnowPlow.toTimestamp(date, true);
    case 'ts':
      return SnowPlow.toTimestamp(date, false);
    case 'dt':
      return SnowPlow.toDatestamp(date);
    default:
      return date;
  }
}

  
/*
 * Fix-up URL when page rendered from search engine cache or translated page.
 * TODO: it would be nice to generalise this and/or move into the ETL phase.
 */
SnowPlow.fixupUrl = function (hostName, href, referrer) {
	/*
	 * Extract parameter from URL
	 */
	function getParameter(url, name) {
		// scheme : // [username [: password] @] hostname [: port] [/ [path] [? query] [# fragment]]
		var e = new RegExp('^(?:https?|ftp)(?::/*(?:[^?]+)[?])([^#]+)'),
			matches = e.exec(url),
			f = new RegExp('(?:^|&)' + name + '=([^&]*)'),
			result = matches ? f.exec(matches[1]) : 0;

		return result ? SnowPlow.decodeWrapper(result[1]) : '';
	}

	if (hostName === 'translate.googleusercontent.com') {		// Google
		if (referrer === '') {
			referrer = href;
		}
		href = getParameter(href, 'u');
		hostName = SnowPlow.getHostName(href);
	} else if (hostName === 'cc.bingj.com' ||					// Bing
			hostName === 'webcache.googleusercontent.com' ||	// Google
			hostName.slice(0, 5) === '74.6.') {					// Yahoo (via Inktomi 74.6.0.0/16)
		href = SnowPlow.documentAlias.links[0].href;
		hostName = SnowPlow.getHostName(href);
	}
	return [hostName, href, referrer];
}

/*
 * Fix-up domain
 */
SnowPlow.fixupDomain = function (domain) {
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

