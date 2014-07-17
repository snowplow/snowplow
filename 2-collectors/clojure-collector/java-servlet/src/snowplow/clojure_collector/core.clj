;;;; Copyright (c) 2012-2013 Snowplow Analytics Ltd. All rights reserved.
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
;;;; Copyright: Copyright (c) 2012-2013 Snowplow Analytics Ltd
;;;; License:   Apache License Version 2.0

(ns snowplow.clojure-collector.core
  "Core app handler"
  (:use [compojure.core              :only [defroutes GET]]
        [ring.middleware.cookies     :only [wrap-cookies]]
        [ring.middleware.reload      :only [wrap-reload]]
        [ring.middleware.stacktrace  :only [wrap-stacktrace]]
        [metrics.ring.expose         :only [expose-metrics-as-json]]
        [metrics.ring.instrument     :only [instrument]])
  (:require [compojure handler route]
            [snowplow.clojure-collector.responses  :as responses]
            [snowplow.clojure-collector.config     :as config]
            [snowplow.clojure-collector.middleware :as mware]))

(defn- send-cookie-etc'
  "Wrapper for send-cookie-etc, pulling
   in the configuration settings"
  [cookies]
  (responses/send-cookie-etc
    cookies
    config/duration
    config/domain
    config/p3p-header
    nil))

(defroutes routes
  "Our routes"
  (GET "/i"           {c :cookies} (send-cookie-etc' c))
  (GET "/ice.png"     {c :cookies} (send-cookie-etc' c)) ; legacy name for i
  (GET "/healthcheck" request responses/send-200)
  ;GET "/status"      from expose-metrics-as-json, only in development env
  ;HEAD "/"           from beanstalk.clj
  (compojure.route/not-found  responses/send-404))

(def app
  "Our routes plus selected wraps.
   See middleware.clj for details"
 (-> #'routes
   (wrap-cookies)
   (mware/wrap-if config/development? wrap-reload '(snowplow.clojure-collector.core snowplow.clojure-collector.middleware responses))
   (mware/wrap-if config/development? wrap-stacktrace)
   (mware/wrap-if config/production?  mware/wrap-failsafe)
   (mware/wrap-request-logging)
   (mware/wrap-exception-logging)
   (mware/wrap-if config/development? expose-metrics-as-json "/status")
   (mware/wrap-if config/development? instrument)))