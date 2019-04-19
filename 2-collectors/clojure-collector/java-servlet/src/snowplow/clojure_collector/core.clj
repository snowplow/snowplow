;;;; Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
;;;; Copyright: Copyright (c) 2012-2019 Snowplow Analytics Ltd
;;;; License:   Apache License Version 2.0

(ns snowplow.clojure-collector.core
  "Core app handler"
  (:use [compojure.core              :only [defroutes GET POST HEAD OPTIONS]]
        [ring.middleware.cookies     :only [wrap-cookies]]
        [ring.middleware.params      :only [wrap-params]]
        [ring.middleware.reload      :only [wrap-reload]]
        [ring.middleware.stacktrace  :only [wrap-stacktrace]]
        [metrics.ring.expose         :only [expose-metrics-as-json]]
        [metrics.ring.instrument     :only [instrument]])
  (:require [compojure handler route]
            [snowplow.clojure-collector.responses  :as responses]
            [snowplow.clojure-collector.config     :as config]
            [snowplow.clojure-collector.middleware :as mware]))

(defn- send-cookie-pixel-or-200-or-redirect'
  "Wrapper for send-cookie-pixel-or-200-or-redirect,
   pulling in the configuration settings"
  [cookies headers pixel vendor params]
  (responses/send-cookie-pixel-or-200-or-redirect
    cookies
    headers
    config/duration
    config/domain
    config/path
    config/p3p-header
    pixel
    vendor
    params))

(defn- send-cookie-pixel-or-200'
  "Wrapper for send-cookie-pixel-or-200-or-redirect,
   with nil vendor and empty params map"
  [cookies headers pixel]
  (send-cookie-pixel-or-200-or-redirect' cookies headers pixel nil {}))

(def send-flash-crossdomain
  "Send back the cross domain policy if configured, 404 otherwise"
  (if (and config/cross-domain-policy-domain config/cross-domain-policy-secure)
    (responses/send-flash-crossdomain config/cross-domain-policy-domain config/cross-domain-policy-secure)
    responses/send-404))

(defroutes routes
  "Our routes"
  (GET     "/i"                {c :cookies, hs :headers} (send-cookie-pixel-or-200' c hs true))
  (GET     "/ice.png"          {c :cookies, hs :headers} (send-cookie-pixel-or-200' c hs true))  ; legacy name for i
  (GET     "/:vendor/:version" {{v :vendor} :params, p :params, c :cookies, hs :headers} (send-cookie-pixel-or-200-or-redirect' c hs true v p))  ; for tracker GET support. Need params for potential redirect
  (POST    "/:vendor/:version" {c :cookies, hs :headers} (send-cookie-pixel-or-200' c hs false)) ; for tracker POST support, no pixel
  (OPTIONS "/:vendor/:version" {hs :headers} (responses/send-preflight-response hs)) ; for CORS requests
  (HEAD    "/:vendor/:version" request responses/send-200)                       ; for webhooks' own checks e.g. Mandrill
  (GET     "/healthcheck"      request responses/send-200)
  (GET     "/crossdomain.xml"  request send-flash-crossdomain)                   ; for Flash cross-domain posting
  (compojure.route/not-found   responses/send-404))

(def app
  "Our routes plus selected wraps.
   See middleware.clj for details"
 (-> #'routes
   (wrap-cookies)
   (wrap-params)
   (mware/wrap-if config/development? wrap-reload '(snowplow.clojure-collector.core snowplow.clojure-collector.middleware responses))
   (mware/wrap-if config/development? wrap-stacktrace)
   (mware/wrap-if config/production?  mware/wrap-failsafe)
   (mware/wrap-request-logging)
   (mware/wrap-exception-logging)
   (mware/wrap-if config/development? expose-metrics-as-json "/status")
   (mware/wrap-if config/development? instrument)))
