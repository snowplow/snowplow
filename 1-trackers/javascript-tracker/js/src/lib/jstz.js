/*jslint undef: true */
/*global console*/
/*global exports*/
/*version 2012-05-10*/

(function(root) {
  /**
   * Namespace to hold all the code for timezone detection.
   */
  var jstz = (function () {
      'use strict';
      var HEMISPHERE_SOUTH = 's',

          /**
           * Gets the offset in minutes from UTC for a certain date.
           * @param {Date} date
           * @returns {Number}
           */
          get_date_offset = function (date) {
              var offset = -date.getTimezoneOffset();
              return (offset !== null ? offset : 0);
          },

          get_january_offset = function () {
              return get_date_offset(new Date(2010, 0, 1, 0, 0, 0, 0));
          },

          get_june_offset = function () {
              return get_date_offset(new Date(2010, 5, 1, 0, 0, 0, 0));
          },

          /**
           * Private method.
           * Checks whether a given date is in daylight savings time.
           * If the date supplied is after june, we assume that we're checking
           * for southern hemisphere DST.
           * @param {Date} date
           * @returns {Boolean}
           */
          date_is_dst = function (date) {
              var base_offset = ((date.getMonth() > 5 ? get_june_offset()
                                                  : get_january_offset())),
                  date_offset = get_date_offset(date);

              return (base_offset - date_offset) !== 0;
          },

          /**
           * This function does some basic calculations to create information about
           * the user's timezone.
           *
           * Returns a key that can be used to do lookups in jstz.olson.timezones.
           *
           * @returns {String}
           */

          lookup_key = function () {
              var january_offset = get_january_offset(),
                  june_offset = get_june_offset(),
                  diff = get_january_offset() - get_june_offset();

              if (diff < 0) {
                  return january_offset + ",1";
              } else if (diff > 0) {
                  return june_offset + ",1," + HEMISPHERE_SOUTH;
              }

              return january_offset + ",0";
          },

          /**
           * Uses get_timezone_info() to formulate a key to use in the olson.timezones dictionary.
           *
           * Returns a primitive object on the format:
           * {'timezone': TimeZone, 'key' : 'the key used to find the TimeZone object'}
           *
           * @returns Object
           */
          determine = function () {
              var key = lookup_key();
              return new jstz.TimeZone(jstz.olson.timezones[key]);
          };

      return {
          determine_timezone : function () {
              if (typeof console !== 'undefined') {
                  console.log("jstz.determine_timezone() is deprecated and will be removed in an upcoming version. Please use jstz.determine() instead.");
              }
              return determine();
          },
          determine: determine,
          date_is_dst : date_is_dst
      };
  }());

  /**
   * Simple object to perform ambiguity check and to return name of time zone.
   */
  jstz.TimeZone = function (tz_name) {
      'use strict';
      var timezone_name = null,

          name = function () {
              return timezone_name;
          },

          /**
           * Checks if a timezone has possible ambiguities. I.e timezones that are similar.
           *
           * For example, if the preliminary scan determines that we're in America/Denver.
           * We double check here that we're really there and not in America/Mazatlan.
           *
           * This is done by checking known dates for when daylight savings start for different
           * timezones during 2010 and 2011.
           */
          ambiguity_check = function () {
              var ambiguity_list = jstz.olson.ambiguity_list[timezone_name],
                  length = ambiguity_list.length,
                  i = 0,
                  tz = ambiguity_list[0];

              for (; i < length; i += 1) {
                  tz = ambiguity_list[i];

                  if (jstz.date_is_dst(jstz.olson.dst_start_dates[tz])) {
                      timezone_name = tz;
                      return;
                  }
              }
          },

          /**
           * Checks if it is possible that the timezone is ambiguous.
           */
          is_ambiguous = function () {
              return typeof (jstz.olson.ambiguity_list[timezone_name]) !== 'undefined';
          };



      timezone_name = tz_name;
      if (is_ambiguous()) {
          ambiguity_check();
      }

      return {
          name: name
      };
  };

  jstz.olson = {};

  /*
   * The keys in this dictionary are comma separated as such:
   *
   * First the offset compared to UTC time in minutes.
   *
   * Then a flag which is 0 if the timezone does not take daylight savings into account and 1 if it
   * does.
   *
   * Thirdly an optional 's' signifies that the timezone is in the southern hemisphere,
   * only interesting for timezones with DST.
   *
   * The mapped arrays is used for constructing the jstz.TimeZone object from within
   * jstz.determine_timezone();
   */
  jstz.olson.timezones = {
      '-720,0'   : 'Etc/GMT+12',
      '-660,0'   : 'Pacific/Pago_Pago',
      '-600,1'   : 'America/Adak',
      '-600,0'   : 'Pacific/Honolulu',
      '-570,0'   : 'Pacific/Marquesas',
      '-540,0'   : 'Pacific/Gambier',
      '-540,1'   : 'America/Anchorage',
      '-480,1'   : 'America/Los_Angeles',
      '-480,0'   : 'Pacific/Pitcairn',
      '-420,0'   : 'America/Phoenix',
      '-420,1'   : 'America/Denver',
      '-360,0'   : 'America/Guatemala',
      '-360,1'   : 'America/Chicago',
      '-360,1,s' : 'Pacific/Easter',
      '-300,0'   : 'America/Bogota',
      '-300,1'   : 'America/New_York',
      '-270,0'   : 'America/Caracas',
      '-240,1'   : 'America/Halifax',
      '-240,0'   : 'America/Santo_Domingo',
      '-240,1,s' : 'America/Asuncion',
      '-210,1'   : 'America/St_Johns',
      '-180,1'   : 'America/Godthab',
      '-180,0'   : 'America/Argentina/Buenos_Aires',
      '-180,1,s' : 'America/Montevideo',
      '-120,0'   : 'America/Noronha',
      '-120,1'   : 'Etc/GMT+2',
      '-60,1'    : 'Atlantic/Azores',
      '-60,0'    : 'Atlantic/Cape_Verde',
      '0,0'      : 'Etc/UTC',
      '0,1'      : 'Europe/London',
      '60,1'     : 'Europe/Berlin',
      '60,0'     : 'Africa/Lagos',
      '60,1,s'   : 'Africa/Windhoek',
      '120,1'    : 'Asia/Beirut',
      '120,0'    : 'Africa/Johannesburg',
      '180,1'    : 'Europe/Moscow',
      '180,0'    : 'Asia/Baghdad',
      '210,1'    : 'Asia/Tehran',
      '240,0'    : 'Asia/Dubai',
      '240,1'    : 'Asia/Yerevan',
      '270,0'    : 'Asia/Kabul',
      '300,1'    : 'Asia/Yekaterinburg',
      '300,0'    : 'Asia/Karachi',
      '330,0'    : 'Asia/Kolkata',
      '345,0'    : 'Asia/Kathmandu',
      '360,0'    : 'Asia/Dhaka',
      '360,1'    : 'Asia/Omsk',
      '390,0'    : 'Asia/Rangoon',
      '420,1'    : 'Asia/Krasnoyarsk',
      '420,0'    : 'Asia/Jakarta',
      '480,0'    : 'Asia/Shanghai',
      '480,1'    : 'Asia/Irkutsk',
      '525,0'    : 'Australia/Eucla',
      '525,1,s'  : 'Australia/Eucla',
      '540,1'    : 'Asia/Yakutsk',
      '540,0'    : 'Asia/Tokyo',
      '570,0'    : 'Australia/Darwin',
      '570,1,s'  : 'Australia/Adelaide',
      '600,0'    : 'Australia/Brisbane',
      '600,1'    : 'Asia/Vladivostok',
      '600,1,s'  : 'Australia/Sydney',
      '630,1,s'  : 'Australia/Lord_Howe',
      '660,1'    : 'Asia/Kamchatka',
      '660,0'    : 'Pacific/Noumea',
      '690,0'    : 'Pacific/Norfolk',
      '720,1,s'  : 'Pacific/Auckland',
      '720,0'    : 'Pacific/Tarawa',
      '765,1,s'  : 'Pacific/Chatham',
      '780,0'    : 'Pacific/Tongatapu',
      '780,1,s'  : 'Pacific/Apia',
      '840,0'    : 'Pacific/Kiritimati'
  };


  /**
   * This object contains information on when daylight savings starts for
   * different timezones.
   *
   * The list is short for a reason. Often we do not have to be very specific
   * to single out the correct timezone. But when we do, this list comes in
   * handy.
   *
   * Each value is a date denoting when daylight savings starts for that timezone.
   */
  jstz.olson.dst_start_dates = {
      'America/Denver' : new Date(2011, 2, 13, 3, 0, 0, 0),
      'America/Mazatlan' : new Date(2011, 3, 3, 3, 0, 0, 0),
      'America/Chicago' : new Date(2011, 2, 13, 3, 0, 0, 0),
      'America/Mexico_City' : new Date(2011, 3, 3, 3, 0, 0, 0),
      'Atlantic/Stanley' : new Date(2011, 8, 4, 7, 0, 0, 0),
      'America/Asuncion' : new Date(2011, 9, 2, 3, 0, 0, 0),
      'America/Santiago' : new Date(2011, 9, 9, 3, 0, 0, 0),
      'America/Campo_Grande' : new Date(2011, 9, 16, 5, 0, 0, 0),
      'America/Montevideo' : new Date(2011, 9, 2, 3, 0, 0, 0),
      'America/Sao_Paulo' : new Date(2011, 9, 16, 5, 0, 0, 0),
      'America/Los_Angeles' : new Date(2011, 2, 13, 8, 0, 0, 0),
      'America/Santa_Isabel' : new Date(2011, 3, 5, 8, 0, 0, 0),
      'America/Havana' : new Date(2011, 2, 13, 2, 0, 0, 0),
      'America/New_York' : new Date(2011, 2, 13, 7, 0, 0, 0),
      'Asia/Gaza' : new Date(2011, 2, 26, 23, 0, 0, 0),
      'Asia/Beirut' : new Date(2011, 2, 27, 1, 0, 0, 0),
      'Europe/Minsk' : new Date(2011, 2, 27, 2, 0, 0, 0),
      'Europe/Helsinki' : new Date(2011, 2, 27, 4, 0, 0, 0),
      'Europe/Istanbul' : new Date(2011, 2, 28, 5, 0, 0, 0),
      'Asia/Damascus' : new Date(2011, 3, 1, 2, 0, 0, 0),
      'Asia/Jerusalem' : new Date(2011, 3, 1, 6, 0, 0, 0),
      'Africa/Cairo' : new Date(2010, 3, 30, 4, 0, 0, 0),
      'Asia/Yerevan' : new Date(2011, 2, 27, 4, 0, 0, 0),
      'Asia/Baku'    : new Date(2011, 2, 27, 8, 0, 0, 0),
      'Pacific/Auckland' : new Date(2011, 8, 26, 7, 0, 0, 0),
      'Pacific/Fiji' : new Date(2010, 11, 29, 23, 0, 0, 0),
      'America/Halifax' : new Date(2011, 2, 13, 6, 0, 0, 0),
      'America/Goose_Bay' : new Date(2011, 2, 13, 2, 1, 0, 0),
      'America/Miquelon' : new Date(2011, 2, 13, 5, 0, 0, 0),
      'America/Godthab' : new Date(2011, 2, 27, 1, 0, 0, 0)
  };

  /**
   * The keys in this object are timezones that we know may be ambiguous after
   * a preliminary scan through the olson_tz object.
   *
   * The array of timezones to compare must be in the order that daylight savings
   * starts for the regions.
   */
  jstz.olson.ambiguity_list = {
      'America/Denver' : ['America/Denver', 'America/Mazatlan'],
      'America/Chicago' : ['America/Chicago', 'America/Mexico_City'],
      'America/Asuncion' : ['Atlantic/Stanley', 'America/Asuncion', 'America/Santiago', 'America/Campo_Grande'],
      'America/Montevideo' : ['America/Montevideo', 'America/Sao_Paulo'],
      'Asia/Beirut' : ['Asia/Gaza', 'Asia/Beirut', 'Europe/Minsk', 'Europe/Helsinki', 'Europe/Istanbul', 'Asia/Damascus', 'Asia/Jerusalem', 'Africa/Cairo'],
      'Asia/Yerevan' : ['Asia/Yerevan', 'Asia/Baku'],
      'Pacific/Auckland' : ['Pacific/Auckland', 'Pacific/Fiji'],
      'America/Los_Angeles' : ['America/Los_Angeles', 'America/Santa_Isabel'],
      'America/New_York' : ['America/Havana', 'America/New_York'],
      'America/Halifax' : ['America/Goose_Bay', 'America/Halifax'],
      'America/Godthab' : ['America/Miquelon', 'America/Godthab']
  };

  if (typeof exports !== 'undefined') {
    exports.jstz = jstz;
  } else {
    root.jstz = jstz;
  }
})(this);
