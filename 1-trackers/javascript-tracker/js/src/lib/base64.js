/*
 * base64.js
 *
 * @description Base64 encoding function for JavaScript
 * @version     N/A
 * @author      Various (see below)
 * @license     MIT / GPL v2
 * @link        http://phpjs.org/functions/base64_encode
 *
 * Modifications:
 * - Various (see below)
 *
 * Authors & Modifications:
 * http://kevin.vanzonneveld.net
 *   original by: Tyler Akins (http://rumkin.com)
 *   improved by: Bayron Guevara
 *   improved by: Thunder.m
 *   improved by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
 *   bugfixed by: Pellentesque Malesuada
 *   improved by: Kevin van Zonneveld (http://kevin.vanzonneveld.net)
 *   improved by: Rafał Kukawski (http://kukawski.pl)
 */

SnowPlow.base64encode = function sha1(data) {
  if (!data) return data;
  if (typeof window['btoa'] == 'function') return btoa(data);

  var b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
  var o1, o2, o3, h1, h2, h3, h4, bits,
      i = 0,
      ac = 0,
      enc = "",
      tmp_arr = [];

  do {
    o1 = data.charCodeAt(i++);
    o2 = data.charCodeAt(i++);
    o3 = data.charCodeAt(i++);

    bits = o1 << 16 | o2 << 8 | o3;

    h1 = bits >> 18 & 0x3f;
    h2 = bits >> 12 & 0x3f;
    h3 = bits >> 6 & 0x3f;
    h4 = bits & 0x3f;

    tmp_arr[ac++] = b64.charAt(h1) + b64.charAt(h2) + b64.charAt(h3) + b64.charAt(h4);
  } while (i < data.length);

  enc = tmp_arr.join('');
  var r = data.length % 3;
  return (r ? enc.slice(0, r - 3) : enc) + '==='.slice(r || 3);
};

/*
 * Bas64 encode data with URL and Filename Safe Alphabet (base64url)
 *
 * See: http://tools.ietf.org/html/rfc4648#page-7
 */
SnowPlow.base64urlencode = function(data) {
  if (!data) return data;

  var enc = SnowPlow.base64encode(data);
  return enc.replace(/=/g, '')
            .replace(/\+/g, '-')
            .replace(/\//g, '_');
};
