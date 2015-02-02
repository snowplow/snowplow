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
  (:use [compojure.core              :only [defroutes GET POST HEAD]]
        [ring.middleware.cookies     :only [wrap-cookies]]
        [ring.middleware.reload      :only [wrap-reload]]
        [ring.middleware.stacktrace  :only [wrap-stacktrace]]
        [metrics.ring.expose         :only [expose-metrics-as-json]]
        [metrics.ring.instrument     :only [instrument]])
  (:require [compojure handler route]
            [snowplow.clojure-collector.responses  :as responses]
            [snowplow.clojure-collector.config     :as config]
            [snowplow.clojure-collector.middleware :as mware]))

(defn- send-cookie-pixel-or-200'
  "Wrapper for send-cookie-pixel-or-200,
   pulling in the configuration settings"
  [cookies pixel]
  (responses/send-cookie-pixel-or-200
    cookies
    config/duration
    config/domain
    config/p3p-header
    pixel))

(defroutes routes
  "Our routes"
  (GET  "/i"                  {c :cookies} (send-cookie-pixel-or-200' c true))
  (GET  "/ice.png"            {c :cookies} (send-cookie-pixel-or-200' c true))  ; legacy name for i
  (GET  "/:vendor/:version"   {c :cookies} (send-cookie-pixel-or-200' c true))  ; for tracker GET support
  (POST "/:vendor/:version"   {c :cookies} (send-cookie-pixel-or-200' c false)) ; for tracker POST support, no pixel
  (HEAD "/:vendor/:version"   request responses/send-200)                       ; for webhooks' own checks e.g. Mandrill
  (GET  "/healthcheck"        request responses/send-200)
  (GET  "/crossdomain.xml"    request responses/send-flash-crossdomain)         ; for Flash cross-domain posting
  ;GET "/status"              available from expose-metrics-as-json, only in development env
  ;HEAD "/"                   available from beanstalk.clj
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
