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

(ns com.snowplowanalytics.clojure-collector
  "Main app handler"
  (:use [compojure.core :only (GET defroutes)])
  (:require (compojure handler route)
            [com.snowplowanalytics.clojure-collector.responses :as responses]))
 ; ice.png is legacy name for i
(defroutes app*
  (GET "/i"       request responses/send-cookie-and-pixel)
  (GET "/ice.png" request responses/send-cookie-and-pixel) 
  (GET "/healthcheck" request responses/send-200)
  (compojure.route/not-found  responses/send-404))

(def app (compojure.handler/api app*))

;; ; To run locally:
(use '[ring.adapter.jetty :only (run-jetty)])     
(def server (run-jetty #'app {:port 8081 :join? false}))