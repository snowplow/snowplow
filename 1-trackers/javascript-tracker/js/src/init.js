// SnowPlow Asynchronous Queue
var _snaq = _snaq || [];

/**
 * SnowPlow namespace.
 * Add classes and functions in this namespace.
 */
var SnowPlow = function() {
	var windowAlias = window;
	return {
		expireDateTime: null,

		/* plugins */
		plugins: {},

		/* DOM Ready */
		hasLoaded: false,
		registeredOnLoadHandlers: [],

		/* alias frequently used globals for added minification */
		documentAlias: document,
		windowAlias: windowAlias,
		navigatorAlias: navigator,
		screenAlias: screen,

		/* encode */
		encodeWrapper: windowAlias.encodeURIComponent,

		/* decode */
		decodeWrapper: windowAlias.decodeURIComponent,

		/* urldecode */
		urldecode: unescape,

		/* asynchronous tracker */
		asyncTracker: null,
	}
}();

