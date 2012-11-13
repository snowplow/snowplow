/* Extend SnowPlow object */
(function() {
	var snowPlow = SnowPlow.build();
	for (prop in snowPlow) {
		if (snowPlow.hasOwnProperty(prop)) {
			if (SnowPlow[prop] === undefined) {
				SnowPlow[prop] = snowPlow[prop];
			}
		}
	}
}());