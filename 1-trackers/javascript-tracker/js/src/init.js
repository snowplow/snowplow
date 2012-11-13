// SnowPlow Asynchronous Queue
var _snaq = _snaq || [];

/**
 * SnowPlow namespace.
 * Add classes and functions in this namespace.
 */
var SnowPlow = SnowPlow || function() {
	var windowAlias = window;
	return {

		/* Tracker identifier with version */
		version: 'js-0.7.0',

		expireDateTime: null,

		/* Plugins */
		plugins: {},

		/* DOM Ready */
		hasLoaded: false,
		registeredOnLoadHandlers: [],

		/* Alias frequently used globals for added minification */
		documentAlias: document,
		windowAlias: windowAlias,
		navigatorAlias: navigator,
		screenAlias: screen,

		/* Encode */
		encodeWrapper: windowAlias.encodeURIComponent,

		/* Decode */
		decodeWrapper: windowAlias.decodeURIComponent,

		/* Urldecode */
		urldecode: unescape,

		/* Asynchronous tracker */
		asyncTracker: null,
	}
}();

