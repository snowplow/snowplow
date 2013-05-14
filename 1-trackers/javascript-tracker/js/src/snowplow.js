/*
 * JavaScript tracker for Snowplow: snowplow.js
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

/*jslint browser:true, plusplus:true, vars:true, nomen:true, evil:true */
/*global window */
/*global unescape */
/*global ActiveXObject */
/*global _snaq:true */
/*members encodeURIComponent, decodeURIComponent, getElementsByTagName,
	shift, unshift,
	addEventListener, attachEvent, removeEventListener, detachEvent,
	cookie, domain, readyState, documentElement, doScroll, title, text,
	location, top, document, referrer, parent, links, href, protocol, GearsFactory,
	event, which, button, srcElement, type, target,
	parentNode, tagName, hostname, className,
	userAgent, cookieEnabled, platform, mimeTypes, enabledPlugin, javaEnabled,
	XDomainRequest, XMLHttpRequest, ActiveXObject, open, setRequestHeader, onreadystatechange, setRequestHeader, send, readyState, status,
	getTime, getTimeAlias, setTime, toGMTString, getHours, getMinutes, getSeconds,
	toLowerCase, charAt, indexOf, lastIndexOf, split, slice, toUpperCase,
	onload, src,
	round, random,
	exec,
	res, width, height,
	pdf, qt, realp, wma, dir, fla, java, gears, ag,
	hook, getHook, getVisitorId, getVisitorInfo,
	setCollectorCf, setCollectorUrl, setSiteId, setAppId,
	setDownloadExtensions, addDownloadExtensions,
	setDomains, setIgnoreClasses, setRequestMethod,
	setReferrerUrl, setCustomUrl, setDocumentTitle,
	setDownloadClasses, setLinkClasses,
	discardHashTag,
	setCookieNamePrefix, setCookieDomain, setCookiePath, setVisitorIdCookie,
	setVisitorCookieTimeout, setSessionCookieTimeout, setReferralCookieTimeout,
	doNotTrack, setDoNotTrack, msDoNotTrack, getTimestamp, getCookieValue,
	detectTimezone, detectViewport,
	addListener, enableLinkTracking, enableActivityTracking, setLinkTrackingTimer,
	enableDarkSocialTracking,
	killFrame, redirectFile, setCountPreRendered,
	trackEvent, trackLink, trackPageView, trackImpression,
	addPlugin, getAsyncTracker
*/

SnowPlow.build = function () {
		"use strict";

		/************************************************************
		 * Private methods
		 ************************************************************/

		/*
		 * apply wrapper
		 *
		 * @param array parameterArray An array comprising either:
		 *      [ 'methodName', optional_parameters ]
		 * or:
		 *      [ functionObject, optional_parameters ]
		 */
		function apply() {
			var i, f, parameterArray;

			for (i = 0; i < arguments.length; i += 1) {
				parameterArray = arguments[i];
				f = parameterArray.shift();

				if (SnowPlow.isString(f)) {
					SnowPlow.asyncTracker[f].apply(SnowPlow.asyncTracker, parameterArray);
				} else {
					f.apply(SnowPlow.asyncTracker, parameterArray);
				}
			}
		}

		/*
		 * Handle beforeunload event
		 *
		 * Subject to Safari's "Runaway JavaScript Timer" and
		 * Chrome V8 extension that terminates JS that exhibits
		 * "slow unload", i.e., calling getTime() > 1000 times
		 */
		function beforeUnloadHandler() {
			var now;

			SnowPlow.executePluginMethod('unload');

			/*
			 * Delay/pause (blocks UI)
			 */
			if (SnowPlow.expireDateTime) {
				// the things we do for backwards compatibility...
				// in ECMA-262 5th ed., we could simply use:
				//     while (Date.now() < SnowPlow.expireDateTime) { }
				do {
					now = new Date();
				} while (now.getTimeAlias() < SnowPlow.expireDateTime);
			}
		}

		/*
		 * Handler for onload event
		 */
		function loadHandler() {
			var i;

			if (!SnowPlow.hasLoaded) {
				SnowPlow.hasLoaded = true;
				SnowPlow.executePluginMethod('load');
				for (i = 0; i < SnowPlow.registeredOnLoadHandlers.length; i++) {
					SnowPlow.registeredOnLoadHandlers[i]();
				}
			}
			return true;
		}

		/*
		 * Add onload or DOM ready handler
		 */
		function addReadyListener() {
			var _timer;

			if (SnowPlow.documentAlias.addEventListener) {
				SnowPlow.addEventListener(SnowPlow.documentAlias, 'DOMContentLoaded', function ready() {
					SnowPlow.documentAlias.removeEventListener('DOMContentLoaded', ready, false);
					loadHandler();
				});
			} else if (SnowPlow.documentAlias.attachEvent) {
				SnowPlow.documentAlias.attachEvent('onreadystatechange', function ready() {
					if (SnowPlow.documentAlias.readyState === 'complete') {
						SnowPlow.documentAlias.detachEvent('onreadystatechange', ready);
						loadHandler();
					}
				});

				if (SnowPlow.documentAlias.documentElement.doScroll && SnowPlow.windowAlias === SnowPlow.windowAlias.top) {
					(function ready() {
						if (!SnowPlow.hasLoaded) {
							try {
								SnowPlow.documentAlias.documentElement.doScroll('left');
							} catch (error) {
								setTimeout(ready, 0);
								return;
							}
							loadHandler();
						}
					}());
				}
			}

			// sniff for older WebKit versions
			if ((new RegExp('WebKit')).test(SnowPlow.navigatorAlias.userAgent)) {
				_timer = setInterval(function () {
					if (SnowPlow.hasLoaded || /loaded|complete/.test(SnowPlow.documentAlias.readyState)) {
						clearInterval(_timer);
						loadHandler();
					}
				}, 10);
			}

			// fallback
			SnowPlow.addEventListener(SnowPlow.windowAlias, 'load', loadHandler, false);
		}


		/************************************************************
		 * Proxy object
		 * - this allows the caller to continue push()'ing to _snaq
		 *   after the Tracker has been initialized and loaded
		 ************************************************************/

		function TrackerProxy() {
			return {
				push: apply
			};
		}

		/************************************************************
		 * Constructor
		 ************************************************************/

		// initialize the SnowPlow singleton
		SnowPlow.addEventListener(SnowPlow.windowAlias, 'beforeunload', beforeUnloadHandler, false);
		addReadyListener();

		Date.prototype.getTimeAlias = Date.prototype.getTime;

		SnowPlow.asyncTracker = new SnowPlow.Tracker();

		for (var i = 0; i < _snaq.length; i++) {
			apply(_snaq[i]);
		}

		// replace initialization array with proxy object
		_snaq = new TrackerProxy();


		/************************************************************
		 * Public data and methods
		 ************************************************************/

	return {
		/**
		* Add plugin
		*
		* @param string pluginName
		* @param Object pluginObj
		*/
		addPlugin: function (pluginName, pluginObj) {
			SnowPlow.plugins[pluginName] = pluginObj;
		},

		/**
		* Returns a Tracker object, configured with a
		* CloudFront collector.
		*
		* @param string distSubdomain The subdomain on your CloudFront collector's distribution
		*/
		getTrackerCf: function (distSubdomain) {
			return new SnowPlow.Tracker({cf: distSubdomain});
		},

		/**
		* Returns a Tracker object, configured with the
		* URL to the collector to use.
		*
		* @param string rawUrl The collector URL minus protocol and /i
		*/
		getTrackerUrl: function (rawUrl) {
			return new SnowPlow.Tracker({url: rawUrl});
		},

		/**
		* Get internal asynchronous tracker object
		*
		* @return Tracker
		*/
		getAsyncTracker: function () {
			return SnowPlow.asyncTracker;
		}
	};
};

