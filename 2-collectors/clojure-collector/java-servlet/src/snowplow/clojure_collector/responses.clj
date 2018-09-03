;;;; Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
;;;;
;;;; This program is licensed to you under the Apache License Version 2.0,
;;;; and you may not use this file except in compliance with the Apache License Version 2.0.
;;;; You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
;;;;
;;;; Unless required by applicable law or agreed to in writing,
;;;; software distributed under the Apache License Version 2.0 is distributed on an
;;;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;;; See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

;;;; Author:    Alex Dean (mailto:support@snowplowanalytics.com)
;;;; Copyright: Copyright (c) 2012-2018 Snowplow Analytics Ltd
;;;; License:   Apache License Version 2.0

(ns snowplow.clojure-collector.responses
  "Holds the different HTTP responses sent by clojure-collector"
  (:import (org.apache.commons.codec.binary Base64)
           (java.io ByteArrayInputStream)
           (org.joda.time DateTime)
           (java.util UUID)))

(def ^:const cookie-name "sp")
(def ^:const id-name "uid")

(def pixel (Base64/decodeBase64 (.getBytes "R0lGODlhAQABAPAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw=="))) ; Can't define ^:const on this as per http://stackoverflow.com/questions/13109958/why-cant-i-use-clojures-const-with-a-java-byte-array
(def ^:const pixel-length (str (alength pixel)))

(defn- uuid
  "Returns a string representation of new type 4 UUID"
  []
  (str (UUID/randomUUID)))

(defn- now-plus
  "Creates a DateTime `duration` seconds from now"
  [duration]
  (-> (new DateTime) (.plusDays duration)))

(defn- set-cookie
  "Sets a Snowplow cookie with visitor `id`,
   to last `duration` seconds for `domain` at `path`.
   If domain is nil, leave out so the FQDN
   of the host can be used instead.
   If path is nil, path will be /."
  [id duration domain path]
  (merge
    {:value    id
     :expires (now-plus duration)
     :path (if (nil? path)
       "/"
       path)}
   (when-let [d domain]
    {:domain   d})))

(defn- generate-id
  "Checks `cookies` and generates a uuid
   for the visitor as necessary"
  [cookies]
  (get (cookies cookie-name) :value (uuid)))

(defn- attach-id
  "Attach a user `id` to the redirect `url`"
  [url id]
    (str url "&" id-name "=" id))

(defn- send-cookie-pixel
  "Respond with a transparent pixel,
   attaching `cookies` and `headers`"
  [cookies headers]
    {:status  200
     :headers (assoc headers
                    "Content-Type"   "image/gif"
                    "Content-Length"  pixel-length)
     :cookies cookies
     :body    (ByteArrayInputStream. pixel)})

(defn- send-cookie-200
  "Respond with a 200,
   attaching `cookies` and `headers`"
  [cookies headers]
    {:status  200
     :headers headers
     :cookies cookies})

(defn- send-redirect
  "If our params map contains `u`, 302 redirect to that URI,
   else return a 400 for Bad Request (malformed syntax)"
  [cookies headers params]
    (let [{url "u"} params]
      (if (nil? url)
        {:status  400
         :headers headers
         :cookies cookies}
        {:status  302
         :headers (merge headers {"Location" url})
         :cookies cookies
         :body    ""})))

(defn send-cookie-pixel-or-200-or-redirect
  "Respond with the cookie and either a
   transparent pixel, a 200 or a redirect"
  [cookies duration domain path p3p-header pixel vendor params]
  (let [id      (generate-id cookies)
        cookies (if (= duration 0)
                  {}
                  {cookie-name (set-cookie id duration domain path)})
        headers {"P3P" p3p-header}]
    (if (= vendor "r")
      (send-redirect cookies headers params)
      (if pixel
        (send-cookie-pixel cookies headers)
        (send-cookie-200 cookies headers)))))

(defn send-preflight-response
  "Respond to CORS request with an appropriate Access-Control-Allow-Origin header"
  [headers]
  (let [origin-header (get headers "origin" "*")]
    {:status 200
     :headers {"Access-Control-Allow-Origin" origin-header
      "Access-Control-Allow-Crendentials" "true"
      "Access-Control-Allow-Headers" "Content-Type"}}))

(def send-404
  "Respond with a 404"
  {:status  404
   :headers {"Content-Type" "text/plain"}
   :body    "404 Not found"})


(def send-200
  "Respond with a 200"
  {:status  200
   :headers {"Content-Type" "text/plain"}
   :body    "OK"})


(defn send-flash-crossdomain
  "Send the configured Flash security settings as per
   http://www.adobe.com/devnet/articles/crossdomain_policy_file_spec.html"
  [cross-domain-policy-domain cross-domain-policy-secure]
  {:status  200
   :headers {"Content-Type" "text/xml"}
   :body    (str "<?xml version=\"1.0\"?>\n<cross-domain-policy>\n  <allow-access-from domain=\""
    cross-domain-policy-domain "\" secure=\"" cross-domain-policy-secure "\" />\n</cross-domain-policy>")})
