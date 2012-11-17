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

(ns snowplow.clojure-collector.core
  "Core app handler"
  (:use [compojure.core          :only [defroutes GET]]
        [ring.middleware.cookies :only [wrap-cookies]]
        [ring.middleware.reload  :only [wrap-reload]]
        [metrics.ring.expose     :only [expose-metrics-as-json]]
        [metrics.ring.instrument :only [instrument]])
  (:require [compojure handler route]
            [snowplow.clojure-collector.responses :as responses]
            [snowplow.clojure-collector.config    :as config]))

(defn- send-cookie-and-pixel-or-redirect'
  "Wrapper for send-cookie-and-pixel-or-redirect, pulling
   in the configuration settings"
  [cookies]
  (responses/send-cookie-and-pixel-or-redirect
    cookies
    config/duration
    config/domain
    config/p3p-header))

(defroutes routes
  "Our main routes - see also beanstalk.clj plus expose-metrics-as-json"
  (GET "/i"           {c :cookies} (send-cookie-and-pixel-or-redirect' c))
  (GET "/ice.png"     {c :cookies} (send-cookie-and-pixel-or-redirect' c)) ; legacy name for i
  (GET "/healthcheck" request responses/send-200)
  ;  + "/status"      provided by expose-metrics-as-json
  (compojure.route/not-found  responses/send-404))

(def app
  "Our routes plus selected wraps"
 (-> #'routes
   (wrap-cookies)
   (wrap-reload '(snowplow.clojure-collector.core responses)) ; TODO: disable this in production
   (#(expose-metrics-as-json % "/status")) ; Lambda because needs routes as first arg
   (instrument)))