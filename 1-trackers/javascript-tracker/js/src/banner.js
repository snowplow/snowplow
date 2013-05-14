/*!
 * Snowplow - The world's most powerful web analytics platform
 *
 * @description JavaScript tracker for Snowplow
 * @version     0.11.2
 * @author      Alex Dean, Simon Andersson, Anthon Pang
 * @copyright   Anthon Pang, Snowplow Analytics Ltd
 * @license     Simplified BSD
 */

/*
 * For technical documentation:
 * https://github.com/snowplow/snowplow/wiki/javascript-tracker
 *
 * For the setup guide:
 * https://github.com/snowplow/snowplow/wiki/javascript-tracker-setup
 * /

/*
 * Browser [In]Compatibility
 * - minimum required ECMAScript: ECMA-262, edition 3
 *
 * Incompatible with these (and earlier) versions of:
 * - IE4 - try..catch and for..in introduced in IE5
 * - IE5 - named anonymous functions, array.push, encodeURIComponent, decodeURIComponent, and getElementsByTagName introduced in IE5.5
 * - Firefox 1.0 and Netscape 8.x - FF1.5 adds array.indexOf, among other things
 * - Mozilla 1.7 and Netscape 6.x-7.x
 * - Netscape 4.8
 * - Opera 6 - Error object (and Presto) introduced in Opera 7
 * - Opera 7
 */

