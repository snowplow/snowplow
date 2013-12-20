#!/bin/sh
#
# manual-test.sh
# Manually test the Scala collector.

sendEvent() {
  local HOST=$1
  echo "======= Sending to $HOST"
  curl -v "$HOST/i?e=pv&page=Snowplow%20-%20the%20most%20powerful%2C%20scalable%2C%20flexible%20web%20analytics%20platform%20in%20the%20world.%20-%20Snowplow%20Analytics&dtm=1387495468819&tid=233567&vp=681x290&ds=968x627&vid=35&duid=9769ffc588846a41&p=web&tv=js-0.12.0&fp=800335550&aid=snowplowweb&lang=en-US&cs=UTF-8&tz=America%2FNew_York&refr=http%3A%2F%2Fsnowplowanalytics.com%2Fblog.html&f_pdf=1&f_qt=1&f_realp=0&f_wma=1&f_dir=0&f_fla=1&f_java=1&f_gears=0&f_ag=0&res=1366x768&cd=24&cookie=1&url=http%3A%2F%2Fsnowplowanalytics.com%2Fproduct%2Findex.html" \
    -H 'Accept-Encoding: gzip,deflate,sdch' \
    -H 'Host: collector.snplow.com' \
    -H 'Accept-Language: en-US,en;q=0.8,et;q=0.6' \
    -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36' \
    -H 'Accept: image/webp,*/*;q=0.8' \
    -H 'Referer: http://snowplowanalytics.com/product/index.html' \
    -H 'Cookie: sp=ffef18c6-f499-4514-bc0c-ea6fabd0e404' \
    -H 'Connection: keep-alive' \
    --compressed > tmp
  hexdump tmp
  rm tmp
}

sendEvent http://collector.snplow.com
sendEvent 127.0.0.1:8080
