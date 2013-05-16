
    $ sbt
    snowplow-avro > avro:generate

TODO: adapt JSON format used in SnowCannon:

	event.push( {
	        "hostname" : hostname,
	        "date" : timestamp.split('T')[0],
	        "time" : timestamp.split('T')[1].split('.')[0],
	        "uuid" : cookies.sp,
	        "url" : request.url,
	        "cookies" : cookies,
	        "headers" : request.headers,
	        "collector" : collector
	    });
