/*
 * SnowPlow Tracker class
 *
 * accountId is an optional argument to the constructor
 *
 * See: Tracker.setCollectorUrl() and Tracker.setAccount()
 */
SnowPlow.Tracker = function Tracker(accountId) {

	/************************************************************
	 * Private members
	 ************************************************************/

	var
/*<DEBUG>*/
		/*
		 * registered test hooks
		 */
		registeredHooks = {},
/*</DEBUG>*/

		// Current URL and Referrer URL
		locationArray = SnowPlow.urlFixup(SnowPlow.documentAlias.domain, SnowPlow.windowAlias.location.href, SnowPlow.getReferrer()),
		domainAlias = SnowPlow.domainFixup(locationArray[0]),
		locationHrefAlias = locationArray[1],
		configReferrerUrl = locationArray[2],

		// Request method is always GET for SnowPlow
		configRequestMethod = 'GET',

		// Tracker URL
		configCollectorUrl = collectorUrlFromAccountId(accountId), // Updated for SnowPlow

		// Site ID
		configTrackerSiteId = '', // Updated for SnowPlow. Starting long road to full removal

		// Document URL
		configCustomUrl,

		// Document title
		configTitle = SnowPlow.documentAlias.title,

		// Extensions to be treated as download links
		configDownloadExtensions = '7z|aac|ar[cj]|as[fx]|avi|bin|csv|deb|dmg|doc|exe|flv|gif|gz|gzip|hqx|jar|jpe?g|js|mp(2|3|4|e?g)|mov(ie)?|ms[ip]|od[bfgpst]|og[gv]|pdf|phps|png|ppt|qtm?|ra[mr]?|rpm|sea|sit|tar|t?bz2?|tgz|torrent|txt|wav|wm[av]|wpd||xls|xml|z|zip',

		// Hosts or alias(es) to not treat as outlinks
		configHostsAlias = [domainAlias],

		// HTML anchor element classes to not track
		configIgnoreClasses = [],

		// HTML anchor element classes to treat as downloads
		configDownloadClasses = [],

		// HTML anchor element classes to treat at outlinks
		configLinkClasses = [],

		// Maximum delay to wait for web bug image to be fetched (in milliseconds)
		configTrackerPause = 500,

		// Minimum visit time after initial page view (in milliseconds)
		configMinimumVisitTime,

		// Recurring heart beat after initial ping (in milliseconds)
		configHeartBeatTimer,

		// Disallow hash tags in URL
		configDiscardHashTag,

		// First-party cookie name prefix
		configCookieNamePrefix = '_sp_',

		// First-party cookie domain
		// User agent defaults to origin hostname
		configCookieDomain,

		// First-party cookie path
		// Default is user agent defined.
		configCookiePath,

		// Do Not Track
		configDoNotTrack,

		// Count sites which are pre-rendered
		configCountPreRendered,

		// Life of the visitor cookie (in milliseconds)
		configVisitorCookieTimeout = 63072000000, // 2 years

		// Life of the session cookie (in milliseconds)
		configSessionCookieTimeout = 1800000, // 30 minutes

		// Life of the referral cookie (in milliseconds)
		configReferralCookieTimeout = 15768000000, // 6 months

		// Browser language (or Windows language for IE). Imperfect but CloudFront doesn't log the Accept-Language header
		configBrowserLanguage = SnowPlow.navigatorAlias.userLanguage || SnowPlow.navigatorAlias.language,

		// Should cookies have the secure flag set
		cookieSecure = SnowPlow.documentAlias.location.protocol === 'https',

		// Browser features via client-side data collection
		browserFeatures = {},

		// Guard against installing the link tracker more than once per Tracker instance
		linkTrackingInstalled = false,

		// Guard against installing the activity tracker more than once per Tracker instance
		activityTrackingInstalled = false,

		// Last activity timestamp
		lastActivityTime,

		// Internal state of the pseudo click handler
		lastButton,
		lastTarget,

		// Hash function
		hash = SnowPlow.sha1,

		// Domain hash value
		domainHash,

		// Visitor UUID
		visitorUUID,

		// Ecommerce transaction data
		// Will be committed, sent and emptied by a call to trackTrans.
		ecommerceTransaction;

	function ecommerceTransactionTemplate() {
		return { transaction: {}, items: [] }
	}

	/*
	 * Removes hash tag from the URL
	 *
	 * URLs are purified before being recorded in the cookie,
	 * or before being sent as GET parameters
	 */
	function purify(url) {
		var targetPattern;

		if (configDiscardHashTag) {
			targetPattern = new RegExp('#.*');
			return url.replace(targetPattern, '');
		}
		return url;
	}

	/*
	 * Extract scheme/protocol from URL
	 */
	function getProtocolScheme(url) {
		var e = new RegExp('^([a-z]+):'),
		matches = e.exec(url);

		return matches ? matches[1] : null;
	}

	/*
	 * Resolve relative reference
	 *
	 * Note: not as described in rfc3986 section 5.2
	 */
	function resolveRelativeReference(baseUrl, url) {
		var protocol = getProtocolScheme(url),
			i;

		if (protocol) {
			return url;
		}

		if (url.slice(0, 1) === '/') {
			return getProtocolScheme(baseUrl) + '://' + getHostName(baseUrl) + url;
		}

		baseUrl = purify(baseUrl);
		if ((i = baseUrl.indexOf('?')) >= 0) {
			baseUrl = baseUrl.slice(0, i);
		}
		if ((i = baseUrl.lastIndexOf('/')) !== baseUrl.length - 1) {
			baseUrl = baseUrl.slice(0, i + 1);
		}

		return baseUrl + url;
	}

	/*
	 * Is the host local? (i.e., not an outlink)
	 */
	function isSiteHostName(hostName) {
		var i,
			alias,
			offset;

		for (i = 0; i < configHostsAlias.length; i++) {
			alias = domainFixup(configHostsAlias[i].toLowerCase());

			if (hostName === alias) {
				return true;
			}

			if (alias.slice(0, 1) === '.') {
				if (hostName === alias.slice(1)) {
					return true;
				}

				offset = hostName.length - alias.length;
				if ((offset > 0) && (hostName.slice(offset) === alias)) {
					return true;
				}
			}
		}
		return false;
	}

	/*
	 * Send image request to Piwik server using GET.
	 * A web bug/beacon is a transparent, single pixel (1x1) image
	 */
	function getImage(request) {
		var image = new Image(1, 1);

		image.onload = function () { };
		image.src = configCollectorUrl + '?' + request;
	}

	/*
	 * Send request
	 */
	function sendRequest(request, delay) {
		var now = new Date();

		if (!configDoNotTrack) {
			getImage(request);
			SnowPlow.expireDateTime = now.getTime() + delay;
		}
	}

	/*
	 * Get cookie name with prefix and domain hash
	 */
	function getCookieName(baseName) {
		// NOTE: If the cookie name is changed, we must also update the PiwikTracker.php which
		// will attempt to discover first party cookies. eg. See the PHP Client method getVisitorId()
		return configCookieNamePrefix + baseName + '.' + configTrackerSiteId + '.' + domainHash;
	}

	/*
	 * Does browser have cookies enabled (for this site)?
	 */
	function hasCookies() {
		var testCookieName = getCookieName('testcookie');

		if (!SnowPlow.isDefined(SnowPlow.navigatorAlias.cookieEnabled)) {
			SnowPlow.setCookie(testCookieName, '1');
			return SnowPlow.getCookie(testCookieName) === '1' ? '1' : '0';
		}

		return SnowPlow.navigatorAlias.cookieEnabled ? '1' : '0';
	}

	/*
	 * Update domain hash
	 */
	function updateDomainHash() {
		domainHash = hash((configCookieDomain || domainAlias) + (configCookiePath || '/')).slice(0, 4); // 4 hexits = 16 bits
	}

	/*
	 * Process all "activity" events.
	 * For performance, this function must have low overhead.
	 */
	function activityHandler() {
		var now = new Date();

		lastActivityTime = now.getTime();
	}

	/*
	 * Sets the Visitor ID cookie: either the first time loadVisitorIdCookie is called
	 * or when there is a new visit or a new page view
	 */
	function setVisitorIdCookie(uuid, createTs, visitCount, nowTs, lastVisitTs) {
		SnowPlow.setCookie(getCookieName('id'), uuid + '.' + createTs + '.' + visitCount + '.' + nowTs + '.' + lastVisitTs, configVisitorCookieTimeout, configCookiePath, configCookieDomain, cookieSecure);
	}

	/*
	 * Load visitor ID cookie
	 */
	function loadVisitorIdCookie() {
		var now = new Date(),
			nowTs = Math.round(now.getTime() / 1000),
			id = SnowPlow.getCookie(getCookieName('id')),
			tmpContainer;

		if (id) {
			tmpContainer = id.split('.');

			// New visitor set to 0 now
			tmpContainer.unshift('0');
		} else {
			// uuid - generate a pseudo-unique ID to fingerprint this user;
			// Note: this isn't a RFC4122-compliant UUID
			if (!visitorUUID) {
				visitorUUID = hash(
					(SnowPlow.navigatorAlias.userAgent || '') +
						(SnowPlow.navigatorAlias.platform || '') +
						JSON2.stringify(browserFeatures) + nowTs
				).slice(0, 16); // 16 hexits = 64 bits
			}

			tmpContainer = [
				// new visitor
				'1',

				// uuid
				visitorUUID,

				// creation timestamp - seconds since Unix epoch
				nowTs,

				// visitCount - 0 = no previous visit
				0,

				// current visit timestamp
				nowTs,

				// last visit timestamp - blank meaning no previous visit
				''
			];
		}
		return tmpContainer;
	}

	/*
	 * Returns the URL to call piwik.php,
	 * with the standard parameters (plugins, resolution, url, referrer, etc.).
	 * Sends the pageview and browser settings with every request in case of race conditions.
	 */
	function getRequest(request, pluginMethod) {
		var i,
			now = new Date(),
			nowTs = Math.round(now.getTime() / 1000),
			newVisitor,
			uuid,
			visitCount,
			createTs,
			currentVisitTs,
			lastVisitTs,
			referralTs,
			referralUrl,
			referralUrlMaxLength = 1024,
			currentReferrerHostName,
			originalReferrerHostName,
			idname = getCookieName('id'),
			sesname = getCookieName('ses'),
			id = loadVisitorIdCookie(),
			ses = SnowPlow.getCookie(sesname),
			currentUrl = configCustomUrl || locationHrefAlias,
			featurePrefix;

		if (configDoNotTrack) {
			SnowPlow.setCookie(idname, '', -1, configCookiePath, configCookieDomain);
			SnowPlow.setCookie(sesname, '', -1, configCookiePath, configCookieDomain);
			return '';
		}

		newVisitor = id[0];
		uuid = id[1];
		createTs = id[2];
		visitCount = id[3];
		currentVisitTs = id[4];
		lastVisitTs = id[5];

		// New session
		if (!ses) {
			// New session (aka new visit)
			visitCount++;

			// Update the last visit timestamp
			lastVisitTs = currentVisitTs;
		}

		// Build out the rest of the request
		request += 
			'&tid=' + String(Math.random()).slice(2, 8) +
			'&uid=' + uuid +
			'&vid=' + visitCount +
			(configTrackerSiteId.length ? '&said=' + SnowPlow.encodeWrapper(configTrackerSiteId) : '') +
			'&lang=' + configBrowserLanguage +
			(configReferrerUrl.length ? '&refr=' + SnowPlow.encodeWrapper(purify(configReferrerUrl)) : '');

		// Browser features. Cookie and resolution don't get prepended with f_ (because they're not optional features)
		for (i in browserFeatures) {
			if (Object.prototype.hasOwnProperty.call(browserFeatures, i)) {
				featurePrefix = (i === 'res' || i === 'cookie') ? '&' : '&f_';
				request += featurePrefix + i + '=' + browserFeatures[i];
			}
		}

		// Finally add the page URL
		request += '&url=' + SnowPlow.encodeWrapper(purify(window.location));

		// Update cookies
		setVisitorIdCookie(uuid, createTs, visitCount, nowTs, lastVisitTs);
		SnowPlow.setCookie(sesname, '*', configSessionCookieTimeout, configCookiePath, configCookieDomain, cookieSecure);

		// Tracker plugin hook
		request += SnowPlow.executePluginMethod(pluginMethod);

		return request;
	}

	/**
	 * Adds the protocol in front of our collector URL, and ice.png to the end
	 *
	 * @param string rawUrl The collector URL without protocol
	 *
	 * @return string collectorUrl The tracker URL with protocol
	 */
	// TODO: update this to use /i instead of /ice.png (BREAKING CHANGE)
	function asCollectorUrl(rawUrl) {
			return ('https:' == document.location.protocol ? 'https' : 'http') + '://' + rawUrl + '/ice.png';               
	}

	/**
	 * Builds a collector URL from an account ID.
	 * The trick here is that each account ID is in fact the subdomain on a specific Amazon CloudFront URL.
	 * We don't bother to support custom CNAMEs because Amazon CloudFront doesn't support that for SSL.
	 *
	 * @param string account The account ID to build the tracker URL from
	 *
	 * @return string The URL on which the collector is hosted
	 */
	function collectorUrlFromAccountId(accountId) {
			return asCollectorUrl(accountId + '.cloudfront.net');
	}

	/**
	 * A helper to build a SnowPlow request string from an
	 * an optional initial value plus a set of individual
	 * key-value pairs, provided using the add method.
	 *
	 * @param string initialValue The initial querystring, ready to have additional key-value pairs added
	 *
	 * @return object The request string builder, with add and build methods
	 */
	function requestStringBuilder(initialValue) {
		var str = initialValue || '';
		return {
			add: function(key, value) {
				if (value !== undefined && value !== '') {
					str += '&' + key + '=' + SnowPlow.encodeWrapper(value);
				}
			},
			build: function() {
				return str;
			}
		}
	}

	/**
	 * Log an event happening on this page
	 *
	 * @param string category The name you supply for the group of objects you want to track
	 * @param string action A string that is uniquely paired with each category, and commonly used to define the type of user interaction for the web object
	 * @param string label (optional) An optional string to provide additional dimensions to the event data
	 * @param int|float|string value (optional) An integer that you can use to provide numerical data about the user event
	 */
	// TODO: update to use requestStringBuilder
	function logEvent(category, action, label, property, value) {

			// All events have a category and an action
			var request = 'ev_ca=' + SnowPlow.encodeWrapper(category)
									+ '&ev_ac=' + SnowPlow.encodeWrapper(action);

			// Label, property and value are optional
			if (String(label).length) {
					request += '&ev_la=' + SnowPlow.encodeWrapper(label);
			}
			if (String(property).length) {
					request += '&ev_pr=' + SnowPlow.encodeWrapper(property);
			}
			if (String(value).length) {
					request += '&ev_va=' + SnowPlow.encodeWrapper(value);
			}

			request = getRequest(request, 'event');
			sendRequest(request, configTrackerPause);
	}

	/**
	 * Log an ad impression
	 *
	 * @param string bannerId Identifier for the ad banner displayed
	 * @param string campaignId (optional) Identifier for the campaign which the banner belongs to
	 * @param string advertiserId (optional) Identifier for the advertiser which the campaign belongs to
	 * @param string userId (optional) Ad server identifier for the viewer of the banner
	 */
	// TODO: update to use requestStringBuilder
	function logImpression(bannerId, campaignId, advertiserId, userId) {

			// All events have a banner ID
			var request = 'ad_ba=' + SnowPlow.encodeWrapper(bannerId);

			// Campaign, advertiser and user IDs are optional
			if (String(campaignId).length) {
					request += '&ad_ca=' + SnowPlow.encodeWrapper(campaignId);
			}
			if (String(advertiserId).length) {
					request += '&ad_ad=' + SnowPlow.encodeWrapper(advertiserId);
			}
			if (String(userId).length) {
					request += '&ad_uid=' + SnowPlow.encodeWrapper(userId);
			}

			request = getRequest(request, configCustomData, 'adimp');
			sendRequest(request, configTrackerPause);
	}

	/**
	 * Log ecommerce transaction metadata
	 */
	// TODO: add params to comment
	function logTransaction(orderId, affiliation, total, tax, shipping, city, state, country) {
		var sb = requestStringBuilder();
		sb.add('tr_id', orderId);
		sb.add('tr_af', affiliation);
		sb.add('tr_tt', total);
		sb.add('tr_tx', tax);
		sb.add('tr_sh', shipping);
		sb.add('tr_ci', city);
		sb.add('tr_st', state);
		sb.add('tr_co', country);
		var params = sb.build();
		var request = getRequest(params, 'ecommerceTransaction');
		sendRequest(request, configTrackerPause);
	}

	/**
	 * Log ecommerce transaction item
	 */
	// TODO: add params to comment
	function logTransactionItem(orderId, sku, name, category, price, quantity) {
		var sb = requestStringBuilder();
		sb.add('ti_id', orderId);
		sb.add('ti_sk', sku);
		sb.add('ti_na', name);
		sb.add('ti_ca', category);
		sb.add('ti_pr', price);
		sb.add('ti_qu', quantity);
		var params = sb.build();
		var request = getRequest(params, 'ecommerceTransactionItem');
		sendRequest(request, configTrackerPause);
	}

	/*
	 * Log the page view / visit
	 */
	function logPageView(customTitle) {
		function titleFixup(title) {
			if (!SnowPlow.isString(title)) {
				title = title.text || '';

				var tmp = SnowPlow.documentAlias.getElementsByTagName('title');
				if (tmp && SnowPlow.isDefined(tmp[0])) {
					title = tmp[0].text;
				}
			}
			return title;
		}

		var now = new Date(),
			request = getRequest('page=' + SnowPlow.encodeWrapper(titleFixup(customTitle || configTitle)), 'log');

		sendRequest(request, configTrackerPause);

		// send ping
		if (configMinimumVisitTime && configHeartBeatTimer && !activityTrackingInstalled) {
			activityTrackingInstalled = true;

			// add event handlers; cross-browser compatibility here varies significantly
			// @see http://quirksmode.org/dom/events
			addEventListener(SnowPlow.documentAlias, 'click', activityHandler);
			addEventListener(SnowPlow.documentAlias, 'mouseup', activityHandler);
			addEventListener(SnowPlow.documentAlias, 'mousedown', activityHandler);
			addEventListener(SnowPlow.documentAlias, 'mousemove', activityHandler);
			addEventListener(SnowPlow.documentAlias, 'mousewheel', activityHandler);
			addEventListener(SnowPlow.windowAlias, 'DOMMouseScroll', activityHandler);
			addEventListener(SnowPlow.windowAlias, 'scroll', activityHandler);
			addEventListener(SnowPlow.documentAlias, 'keypress', activityHandler);
			addEventListener(SnowPlow.documentAlias, 'keydown', activityHandler);
			addEventListener(SnowPlow.documentAlias, 'keyup', activityHandler);
			addEventListener(SnowPlow.windowAlias, 'resize', activityHandler);
			addEventListener(SnowPlow.windowAlias, 'focus', activityHandler);
			addEventListener(SnowPlow.windowAlias, 'blur', activityHandler);

			// periodic check for activity
			lastActivityTime = now.getTime();
			setTimeout(function heartBeat() {
				var now = new Date(),
					request;

				// There was activity during the heart beat period;
				// on average, this is going to overstate the visitDuration by configHeartBeatTimer/2
				if ((lastActivityTime + configHeartBeatTimer) > now.getTime()) {
					// send ping if minimum visit time has elapsed
					if (configMinimumVisitTime < now.getTime()) {
						request = getRequest('ping=1', 'ping');

						sendRequest(request, configTrackerPause);
					}

					// resume heart beat
					setTimeout(heartBeat, configHeartBeatTimer);
				}
				// Else heart beat cancelled due to inactivity
			}, configHeartBeatTimer);
		}
	}

	/*
	 * Log the link or click with the server
	 */
	function logLink(url, linkType) {
		var request = getRequest(linkType + '=' + SnowPlow.encodeWrapper(purify(url)), 'link');

		sendRequest(request, configTrackerPause);
	}

	/*
	 * Browser prefix
	 */
	function prefixPropertyName(prefix, propertyName) {
		
		if (prefix !== '') {
			return prefix + propertyName.charAt(0).toUpperCase() + propertyName.slice(1);
		}

		return propertyName;
	}

	/*
	 * Check for pre-rendered web pages, and log the page view/link
	 * according to the configuration and/or visibility
	 *
	 * @see http://dvcs.w3.org/hg/webperf/raw-file/tip/specs/PageVisibility/Overview.html
	 */
	function trackCallback(callback) {
		var isPreRendered,
			i,
			// Chrome 13, IE10, FF10
			prefixes = ['', 'webkit', 'ms', 'moz'],
			prefix;

		if (!configCountPreRendered) {
			for (i = 0; i < prefixes.length; i++) {
				prefix = prefixes[i];

				// does this browser support the page visibility API?
				if (Object.prototype.hasOwnProperty.call(SnowPlow.documentAlias, prefixPropertyName(prefix, 'hidden'))) {
					// if pre-rendered, then defer callback until page visibility changes
					if (SnowPlow.documentAlias[prefixPropertyName(prefix, 'visibilityState')] === 'prerender') {
						isPreRendered = true;
					}
					break;
				}
			}
		}

		if (isPreRendered) {
			// note: the event name doesn't follow the same naming convention as vendor properties
			addEventListener(SnowPlow.documentAlias, prefix + 'visibilitychange', function ready() {
				SnowPlow.documentAlias.removeEventListener(prefix + 'visibilitychange', ready, false);
				callback();
			});
			return;
		}

		// configCountPreRendered === true || isPreRendered === false
		callback();
	}

	/*
	 * Construct regular expression of classes
	 */
	function getClassesRegExp(configClasses, defaultClass) {
		var i,
			classesRegExp = '(^| )(piwik[_-]' + defaultClass;

		if (configClasses) {
			for (i = 0; i < configClasses.length; i++) {
				classesRegExp += '|' + configClasses[i];
			}
		}
		classesRegExp += ')( |$)';

		return new RegExp(classesRegExp);
	}

	/*
	 * Link or Download?
	 */
	function getLinkType(className, href, isInLink) {
		// outlinks
		if (!isInLink) {
			return 'link';
		}

		// does class indicate whether it is an (explicit/forced) outlink or a download?
		var downloadPattern = getClassesRegExp(configDownloadClasses, 'download'),
			linkPattern = getClassesRegExp(configLinkClasses, 'link'),

			// does file extension indicate that it is a download?
			downloadExtensionsPattern = new RegExp('\\.(' + configDownloadExtensions + ')([?&#]|$)', 'i');

		// optimization of the if..elseif..else construct below
		return linkPattern.test(className) ? 'link' : (downloadPattern.test(className) || downloadExtensionsPattern.test(href) ? 'download' : 0);
	}

	/*
	 * Process clicks
	 */
	function processClick(sourceElement) {
		var parentElement,
			tag,
			linkType;

		while ((parentElement = sourceElement.parentNode) !== null &&
				SnowPlow.isDefined(parentElement) && // buggy IE5.5
				((tag = sourceElement.tagName.toUpperCase()) !== 'A' && tag !== 'AREA')) {
			sourceElement = parentElement;
		}

		if (SnowPlow.isDefined(sourceElement.href)) {
			// browsers, such as Safari, don't downcase hostname and href
			var originalSourceHostName = sourceElement.hostname || getHostName(sourceElement.href),
				sourceHostName = originalSourceHostName.toLowerCase(),
				sourceHref = sourceElement.href.replace(originalSourceHostName, sourceHostName),
				scriptProtocol = new RegExp('^(javascript|vbscript|jscript|mocha|livescript|ecmascript|mailto):', 'i');

			// ignore script pseudo-protocol links
			if (!scriptProtocol.test(sourceHref)) {
				// track outlinks and all downloads
				linkType = getLinkType(sourceElement.className, sourceHref, isSiteHostName(sourceHostName));
				if (linkType) {
					// urldecode %xx
					sourceHref = urldecode(sourceHref);
					logLink(sourceHref, linkType);
				}
			}
		}
	}

	/*
	 * Handle click event
	 */
	function clickHandler(evt) {
		var button,
			target;

		evt = evt || SnowPlow.windowAlias.event;
		button = evt.which || evt.button;
		target = evt.target || evt.srcElement;

		// Using evt.type (added in IE4), we avoid defining separate handlers for mouseup and mousedown.
		if (evt.type === 'click') {
			if (target) {
				processClick(target);
			}
		} else if (evt.type === 'mousedown') {
			if ((button === 1 || button === 2) && target) {
				lastButton = button;
				lastTarget = target;
			} else {
				lastButton = lastTarget = null;
			}
		} else if (evt.type === 'mouseup') {
			if (button === lastButton && target === lastTarget) {
				processClick(target);
			}
			lastButton = lastTarget = null;
		}
	}

	/*
	 * Add click listener to a DOM element
	 */
	function addClickListener(element, enable) {
		if (enable) {
			// for simplicity and performance, we ignore drag events
			addEventListener(element, 'mouseup', clickHandler, false);
			addEventListener(element, 'mousedown', clickHandler, false);
		} else {
			addEventListener(element, 'click', clickHandler, false);
		}
	}

	/*
	 * Add click handlers to anchor and AREA elements, except those to be ignored
	 */
	function addClickListeners(enable) {
		if (!linkTrackingInstalled) {
			linkTrackingInstalled = true;

			// iterate through anchor elements with href and AREA elements

			var i,
				ignorePattern = getClassesRegExp(configIgnoreClasses, 'ignore'),
				linkElements = SnowPlow.documentAlias.links;

			if (linkElements) {
				for (i = 0; i < linkElements.length; i++) {
					if (!ignorePattern.test(linkElements[i].className)) {
						addClickListener(linkElements[i], enable);
					}
				}
			}
		}
	}

	/*
	 * Browser features (plugins, resolution, cookies)
	 */
	function detectBrowserFeatures() {
		var i,
			mimeType,
			pluginMap = {
				// document types
				pdf: 'application/pdf',

				// media players
				qt: 'video/quicktime',
				realp: 'audio/x-pn-realaudio-plugin',
				wma: 'application/x-mplayer2',

				// interactive multimedia
				dir: 'application/x-director',
				fla: 'application/x-shockwave-flash',

				// RIA
				java: 'application/x-java-vm',
				gears: 'application/x-googlegears',
				ag: 'application/x-silverlight'
			};

		// general plugin detection
		if (SnowPlow.navigatorAlias.mimeTypes && SnowPlow.navigatorAlias.mimeTypes.length) {
			for (i in pluginMap) {
				if (Object.prototype.hasOwnProperty.call(pluginMap, i)) {
					mimeType = SnowPlow.navigatorAlias.mimeTypes[pluginMap[i]];
					browserFeatures[i] = (mimeType && mimeType.enabledPlugin) ? '1' : '0';
				}
			}
		}

		// Safari and Opera
		// IE6/IE7 navigator.javaEnabled can't be aliased, so test directly
		if (typeof navigator.javaEnabled !== 'unknown' &&
				SnowPlow.isDefined(SnowPlow.navigatorAlias.javaEnabled) &&
				SnowPlow.navigatorAlias.javaEnabled()) {
			browserFeatures.java = '1';
		}

		// Firefox
		if (SnowPlow.isFunction(SnowPlow.windowAlias.GearsFactory)) {
			browserFeatures.gears = '1';
		}

		// other browser features
		browserFeatures.res = SnowPlow.screenAlias.width + 'x' + SnowPlow.screenAlias.height;
		browserFeatures.cookie = hasCookies();
	}

/*<DEBUG>*/
	/*
	 * Register a test hook. Using eval() permits access to otherwise
	 * privileged members.
	 */
	function registerHook(hookName, userHook) {
		var hookObj = null;

		if (isString(hookName) && !SnowPlow.isDefined(registeredHooks[hookName]) && userHook) {
			if (isObject(userHook)) {
				hookObj = userHook;
			} else if (isString(userHook)) {
				try {
					eval('hookObj =' + userHook);
				} catch (e) { }
			}

			registeredHooks[hookName] = hookObj;
		}
		return hookObj;
	}
/*</DEBUG>*/

	/************************************************************
	 * Constructor
	 ************************************************************/

	/*
	 * initialize tracker
	 */
	detectBrowserFeatures();
	updateDomainHash();
	ecommerceTransaction = ecommerceTransactionTemplate();

/*<DEBUG>*/
	/*
	 * initialize test plugin
	 */
	SnowPlow.executePluginMethod('run', registerHook);
/*</DEBUG>*/

	/************************************************************
	 * Public data and methods
	 ************************************************************/

	return {
/*<DEBUG>*/
		/*
		 * Test hook accessors
		 */
		hook: registeredHooks,
		getHook: function (hookName) {
			return registeredHooks[hookName];
		},
/*</DEBUG>*/

		/**
		 * Get visitor ID (from first party cookie)
		 *
		 * @return string Visitor ID in hexits (or null, if not yet known)
		 */
		getVisitorId: function () {
			return (loadVisitorIdCookie())[1];
		},

		/**
		 * Get the visitor information (from first party cookie)
		 *
		 * @return array
		 */
		getVisitorInfo: function () {
			return loadVisitorIdCookie();
		},

		/**
		 * Specify the site ID
		 *
		 * @param int|string siteId
		 */
		setSiteId: function (siteId) {
			configTrackerSiteId = siteId;
		},

		/**
		 * Set delay for link tracking (in milliseconds)
		 *
		 * @param int delay
		 */
		setLinkTrackingTimer: function (delay) {
			configTrackerPause = delay;
		},

		/**
		 * Set list of file extensions to be recognized as downloads
		 *
		 * @param string extensions
		 */
		setDownloadExtensions: function (extensions) {
			configDownloadExtensions = extensions;
		},

		/**
		 * Specify additional file extensions to be recognized as downloads
		 *
		 * @param string extensions
		 */
		addDownloadExtensions: function (extensions) {
			configDownloadExtensions += '|' + extensions;
		},

		/**
		 * Set array of domains to be treated as local
		 *
		 * @param string|array hostsAlias
		 */
		setDomains: function (hostsAlias) {
			configHostsAlias = isString(hostsAlias) ? [hostsAlias] : hostsAlias;
			configHostsAlias.push(domainAlias);
		},

		/**
		 * Set array of classes to be ignored if present in link
		 *
		 * @param string|array ignoreClasses
		 */
		setIgnoreClasses: function (ignoreClasses) {
			configIgnoreClasses = isString(ignoreClasses) ? [ignoreClasses] : ignoreClasses;
		},

		/**
		 * Override referrer
		 *
		 * @param string url
		 */
		setReferrerUrl: function (url) {
			configReferrerUrl = url;
		},

		/**
		 * Override url
		 *
		 * @param string url
		 */
		setCustomUrl: function (url) {
			configCustomUrl = resolveRelativeReference(locationHrefAlias, url);
		},

		/**
		 * Override document.title
		 *
		 * @param string title
		 */
		setDocumentTitle: function (title) {
			configTitle = title;
		},

		/**
		 * Set array of classes to be treated as downloads
		 *
		 * @param string|array downloadClasses
		 */
		setDownloadClasses: function (downloadClasses) {
			configDownloadClasses = isString(downloadClasses) ? [downloadClasses] : downloadClasses;
		},

		/**
		 * Set array of classes to be treated as outlinks
		 *
		 * @param string|array linkClasses
		 */
		setLinkClasses: function (linkClasses) {
			configLinkClasses = isString(linkClasses) ? [linkClasses] : linkClasses;
		},

		/**
		 * Strip hash tag (or anchor) from URL
		 *
		 * @param bool enableFilter
		 */
		discardHashTag: function (enableFilter) {
			configDiscardHashTag = enableFilter;
		},

		/**
		 * Set first-party cookie name prefix
		 *
		 * @param string cookieNamePrefix
		 */
		setCookieNamePrefix: function (cookieNamePrefix) {
			configCookieNamePrefix = cookieNamePrefix;
		},

		/**
		 * Set first-party cookie domain
		 *
		 * @param string domain
		 */
		setCookieDomain: function (domain) {
			configCookieDomain = domainFixup(domain);
			updateDomainHash();
		},

		/**
		 * Set first-party cookie path
		 *
		 * @param string domain
		 */
		setCookiePath: function (path) {
			configCookiePath = path;
			updateDomainHash();
		},

		/**
		 * Set visitor cookie timeout (in seconds)
		 *
		 * @param int timeout
		 */
		setVisitorCookieTimeout: function (timeout) {
			configVisitorCookieTimeout = timeout * 1000;
		},

		/**
		 * Set session cookie timeout (in seconds)
		 *
		 * @param int timeout
		 */
		setSessionCookieTimeout: function (timeout) {
			configSessionCookieTimeout = timeout * 1000;
		},

		/**
		 * Set referral cookie timeout (in seconds)
		 *
		 * @param int timeout
		 */
		setReferralCookieTimeout: function (timeout) {
			configReferralCookieTimeout = timeout * 1000;
		},

		/**
		 * Handle do-not-track requests
		 *
		 * @param bool enable If true, don't track if user agent sends 'do-not-track' header
		 */
		setDoNotTrack: function (enable) {
			var dnt = SnowPlow.navigatorAlias.doNotTrack || SnowPlow.navigatorAlias.msDoNotTrack;

			configDoNotTrack = enable && (dnt === 'yes' || dnt === '1');
		},

		/**
		 * Add click listener to a specific link element.
		 * When clicked, Piwik will log the click automatically.
		 *
		 * @param DOMElement element
		 * @param bool enable If true, use pseudo click-handler (mousedown+mouseup)
		 */
		addListener: function (element, enable) {
			addClickListener(element, enable);
		},

		/**
		 * Install link tracker
		 *
		 * The default behaviour is to use actual click events. However, some browsers
		 * (e.g., Firefox, Opera, and Konqueror) don't generate click events for the middle mouse button.
		 *
		 * To capture more "clicks", the pseudo click-handler uses mousedown + mouseup events.
		 * This is not industry standard and is vulnerable to false positives (e.g., drag events).
		 *
		 * There is a Safari/Chrome/Webkit bug that prevents tracking requests from being sent
		 * by either click handler.  The workaround is to set a target attribute (which can't
		 * be "_self", "_top", or "_parent").
		 *
		 * @see https://bugs.webkit.org/show_bug.cgi?id=54783
		 *
		 * @param bool enable If true, use pseudo click-handler (mousedown+mouseup)
		 */
		enableLinkTracking: function (enable) {
			if (SnowPlow.hasLoaded) {
				// the load event has already fired, add the click listeners now
				addClickListeners(enable);
			} else {
				// defer until page has loaded
				SnowPlow.registeredOnLoadHandlers.push(function () {
					addClickListeners(enable);
				});
			}
		},

		/**
		 * Set heartbeat (in seconds)
		 *
		 * @param int minimumVisitLength
		 * @param int heartBeatDelay
		 */
		setHeartBeatTimer: function (minimumVisitLength, heartBeatDelay) {
			var now = new Date();

			configMinimumVisitTime = now.getTime() + minimumVisitLength * 1000;
			configHeartBeatTimer = heartBeatDelay * 1000;
		},

		/**
		 * Frame buster
		 */
		killFrame: function () {
			if (SnowPlow.windowAlias.location !== SnowPlow.windowAlias.top.location) {
				SnowPlow.windowAlias.top.location = SnowPlow.windowAlias.location;
			}
		},

		/**
		 * Redirect if browsing offline (aka file: buster)
		 *
		 * @param string url Redirect to this URL
		 */
		redirectFile: function (url) {
			if (SnowPlow.windowAlias.location.protocol === 'file:') {
				SnowPlow.windowAlias.location = url;
			}
		},

		/**
		 * Count sites in pre-rendered state
		 *
		 * @param bool enable If true, track when in pre-rendered state
		 */
		setCountPreRendered: function (enable) {
			configCountPreRendered = enable;
		},

		/**
		 * Manually log a click from your own code
		 *
		 * @param string sourceUrl
		 * @param string linkType
		 */
		trackLink: function (sourceUrl, linkType) {
			trackCallback(function () {
				logLink(sourceUrl, linkType);
			});
		},

		/**
		 * Log visit to this page
		 *
		 * @param string customTitle
		 */
		trackPageView: function (customTitle) {
			trackCallback(function () {
				logPageView(customTitle);
			});
		},

		/**
		 * Specify the SnowPlow tracking account. We use the account ID
		 * to define the tracker URL for SnowPlow. 
		 *
		 * @param string accountId
		 */
		// TODO: change to setAccountId for consistency (BREAKING CHANGE)
		setAccount: function (accountId) {
			configCollectorUrl = collectorUrlFromAccountId(accountId);
		},

		/**
		 *
		 * Specify the SnowPlow collector URL. No need to include HTTP
		 * or HTTPS - we will add this.
		 * 
		 * @param string rawUrl The collector URL minus protocol and /ice.png
		 */
		setCollectorUrl: function (rawUrl) {
			configCollectorUrl = asCollectorUrl(rawUrl);
		},

		/**
		 * Track an event happening on this page
		 *
		 * @param string category The name you supply for the group of objects you want to track
		 * @param string action A string that is uniquely paired with each category, and commonly used to define the type of user interaction for the web object
		 * @param string label (optional) An optional string to provide additional dimensions to the event data
		 * @param int|float|string value (optional) An integer that you can use to provide numerical data about the user event
		 */
		trackEvent: function (category, action, label, property, value) {
			logEvent(category, action, label, property, value);                   
		},

		/**
		 * Track an ad being served
		 *
		 * @param string bannerId Identifier for the ad banner displayed
		 * @param string campaignId (optional) Identifier for the campaign which the banner belongs to
		 * @param string advertiserId (optional) Identifier for the advertiser which the campaign belongs to
		 * @param string userId (optional) Ad server identifier for the viewer of the banner
		 */
		 trackImpression: function (bannerId, campaignId, advertiserId, userId) {
				 logImpression(bannerId, campaignId, advertiserId, userId);
		 },

		/**
		 * Track an ecommerce transaction
		 *
		 * @param string orderId Required. Internal unique order id number for this transaction.
		 * @param string affiliation Optional. Partner or store affiliation.
		 * @param string total Required. Total amount of the transaction.
		 * @param string tax Optional. Tax amount of the transaction.
		 * @param string shipping Optional. Shipping charge for the transaction.
		 * @param string city Optional. City to associate with transaction.
		 * @param string state Optional. State to associate with transaction.
		 * @param string country Optional. Country to associate with transaction.
		 */
		 addTrans: function(orderId, affiliation, total, tax, shipping, city, state, country) {
			 ecommerceTransaction.transaction = {
				 orderId: orderId,
				 affiliation: affiliation,
				 total: total,
				 tax: tax,
				 shipping: shipping,
				 city: city,
				 state: state,
				 country: country};
		 },

		/**
		 * Track an ecommerce transaction item
		 *
		 * @param string orderId Required Order ID of the transaction to associate with item.
		 * @param string sku Required. Item's SKU code.
		 * @param string name Optional. Product name.
		 * @param string category Optional. Product category.
		 * @param string price Required. Product price.
		 * @param string quantity Required. Purchase quantity.
		 */
		 addItem: function(orderId, sku, name, category, price, quantity) {
			 ecommerceTransaction.items.push({
						orderId: orderId,
						sku: sku,
						name: name,
						category: category,
						price: price,
						quantity: quantity});
		 },

		/**
		 * Commit the ecommerce transaction
		 *
		 * This call will send the data specified with addTrans,
		 * addItem methods to the tracking server.
		 */
		 trackTrans: function() {
			 logTransaction(
					 ecommerceTransaction.transaction.orderId,
					 ecommerceTransaction.transaction.affiliation,
					 ecommerceTransaction.transaction.total,
					 ecommerceTransaction.transaction.tax,
					 ecommerceTransaction.transaction.shipping,
					 ecommerceTransaction.transaction.city,
					 ecommerceTransaction.transaction.state,
					 ecommerceTransaction.transaction.country
					);
			 ecommerceTransaction.items.forEach(function(item) {
				 logTransactionItem(
					 item.orderId,
					 item.sku,
					 item.name,
					 item.category,
					 item.price,
					 item.quantity
					 );
			 });

			 ecommerceTransaction = ecommerceTransactionTemplate();
		 }

	};
}
