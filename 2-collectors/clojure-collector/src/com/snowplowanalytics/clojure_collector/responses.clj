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

(ns com.snowplowanalytics.clojure-collector.responses
  "Holds the different HTTP responses sent by clojure-collector"
  (:import (org.apache.commons.codec.binary Base64))
  (:import (org.joda.time DateTime)))

(def pixel-bytes (Base64/decodeBase64 (.getBytes "R0lGODlhAQABAPAAAAAAAAAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw=="))) ; Can't define ^:const on this as per http://stackoverflow.com/questions/13109958/why-cant-i-use-clojures-const-with-a-java-byte-array
(def pixel (new java.io.ByteArrayInputStream pixel-bytes))
(def ^:const pixel-length (str (alength pixel-bytes)))

(def send-404
  "Respond with a 404"
  {:status  404
   :headers {"Content-Type" "text/plain"}
   :body    (str "404 Not found")})

(def send-200
  "Respond with a 200"
  {:status  200
   :headers {"Content-Type" "text/plain"}
   :body    (str "OK")})

; [cookie-id cookie-duration cookie-contents]
(def send-cookie-and-pixel
  "Responds with a transparent pixel and the cookie"
  {:status  200
   :headers {"Set-Cookie"    (str "sp=" "cookie-id" "; expires=" "[1]" ";" "cookie-contents")
             "P3P"           "policyref=\"/w3c/p3p.xml\", CP=\"NOI DSP COR NID PSA OUR IND COM NAV STA\""
             "Content-Type"  "image/gif"
             "Content-Length" pixel-length}
   :body    pixel})

(defn- set-id-cookie
  "Sets the SnowPlow ID cookie"
  [id duration domain] 
  {:value    (str "sp=" id)
   :expires  (-> (new DateTime) (.plusSeconds duration))
   :domain   domain})

;; TODO: [1] new Date(new Dat`e().getTime()+cookieDuration).toUTCString()