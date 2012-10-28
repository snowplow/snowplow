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
  (:import (org.apache.commons.codec.binary Base64)))

(def ^:const pixel-data (str "R0lGODlhAQABAPAAAAAAAAAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw=="))
(def pixel (Base64/decodeBase64 pixel-data)) ; Can't define ^:const on this as per http://stackoverflow.com/questions/13109958/why-cant-i-use-clojures-const-with-a-java-byte-array
(def ^:const pixel-length (alength pixel))

;; Respond with a 404.
(def send-404
  {:status  404
   :headers {"Content-Type" "text/plain"}
   :body    (str "404 Not found")})

;; Respond with a 200.
(def send-200
  {:status  200
   :headers {"Content-Type" "text/plain"}
   :body    (str "OK")})

;; Respond with a transparent pixel and the cookie. ; [cookie-id cookie-duration cookie-contents]
(def send-cookie-and-pixel
  {:status  200
   :headers {"Set-Cookie"    (str "sp=" "cookie-id" "; expires=" "[1]" ";" "cookie-contents")
             "P3P"           "policyref=\"/w3c/p3p.xml\", CP=\"NOI DSP COR NID PSA OUR IND COM NAV STA\""
             "Content-Type"  "image/gif"
             "Content-Length" (str pixel-length)}
   :body    (new java.io.ByteArrayInputStream pixel)})

;; TODO: [1] new Date(new Date().getTime()+cookieDuration).toUTCString()