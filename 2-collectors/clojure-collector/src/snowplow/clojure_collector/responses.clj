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
(def ^:const cookie-p3p-header "policyref=\"/w3c/p3p.xml\", CP=\"NOI DSP COR NID PSA OUR IND COM NAV STA\"") ; TODO: move this into config

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
  "Sets a SnowPlow cookie with visitor `id`, lasting `duration` seconds for `domain`"
  [id duration domain] 
  {:value    id
   ; :domain   domain ; Comment this line out to test locally, because of http://stackoverflow.com/questions/1134290/cookies-on-localhost-with-explicit-domain
   :expires  (now-plus duration)})

(defn- generate-id
  "Checks `cookies` and generates a uuid for the visitor as necessary"
  [cookies]
  (get (cookies cookie-name) :value (uuid)))

(defn send-cookie-and-pixel
  "Respond with a transparent pixel and the cookie"
  [cookies duration domain]
  (let [id (generate-id cookies)
        cookie-contents (set-cookie id duration domain)]
    {:status  200
     :headers {"Content-Type"   "image/gif"
               "P3P"             cookie-p3p-header
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