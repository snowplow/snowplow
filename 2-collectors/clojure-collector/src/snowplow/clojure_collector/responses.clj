;;;; Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
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
;;;; Copyright: Copyright (c) 2012 SnowPlow Analytics Ltd
;;;; License:   Apache License Version 2.0

(ns snowplow.clojure-collector.responses
  "Holds the different HTTP responses sent by clojure-collector"
  (:import (org.apache.commons.codec.binary Base64)
           (java.io ByteArrayInputStream)
           (org.joda.time DateTime)
           (java.util UUID)))

(def ^:const cookie-name "sp")

(def pixel (Base64/decodeBase64 (.getBytes "R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw=="))) ; Can't define ^:const on this as per http://stackoverflow.com/questions/13109958/why-cant-i-use-clojures-const-with-a-java-byte-array
(def ^:const pixel-length (str (alength pixel)))

(defn- uuid
  "Returns a string representation of new type 4 UUID"
  []
  (str (UUID/randomUUID)))

(defn- now-plus
  "Creates a DateTime `duration` seconds from now"
  [duration]
  (-> (new DateTime) (.plusSeconds duration)))

(defn- set-cookie
  "Sets a SnowPlow cookie with visitor `id`,
   to last `duration` seconds for `domain`.
   If domain is nil, leave out so the FQDN
   of the host can be used instead"
  [id duration domain]
  (merge
    {:value    id
     :expires (now-plus duration)}
   (when-let [d nil]
    {:domain   d}))


(defn- generate-id
  "Checks `cookies` and generates a uuid for the visitor as necessary"
  [cookies]
  (get (cookies cookie-name) :value (uuid)))

(defn send-cookie-and-pixel
  "Respond with a transparent pixel and the cookie"
  [cookies duration domain p3p-header]
  (let [id (generate-id cookies)
        cookie-contents (set-cookie id duration domain)]
    {:status  200
     :headers {"Content-Type"   "image/gif"
               "P3P"             p3p-header
               "Content-Length"  pixel-length}
     :cookies {cookie-name cookie-contents}
     :body    (ByteArrayInputStream. pixel)}))


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