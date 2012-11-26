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

(ns snowplow.clojure-collector.config
  "Gets environment variables, using
   sensible defaults where necessary")

;; Note Beanstalk only has 4 'slots' in the UI for environment variables
(def ^:const env-varnames ["PARAM1", "SP_ENV"])
(def ^:const p3p-varnames ["PARAM2", "SP_P3P"])
(def ^:const domain-varnames ["PARAM3", "SP_DOMAIN"])
(def ^:const duration-varnames ["PARAM4", "SP_DURATION"])
; If you add any more, they won't be settable in the Beanstalk UI.

;; Defaults
(def ^:const default-p3p-header "policyref=\"/w3c/p3p.xml\", CP=\"NOI DSP COR NID PSA OUR IND COM NAV STA\"")
(def ^:const default-duration 31556900) ; A year

(defn- get-env
  "Try both options for each
   environment varname. Supports
   optional `default` as fallback"
  ([varnames] (get-env varnames nil))
  ([varnames default]
    (let [env (System/getenv)]
      (get env (first varnames)
        (get env (second varnames)
          default)))))

(def duration
  "Get the duration (in seconds) the
   cookie should last for"
  (get-env duration-varnames default-duration))

(def p3p-header
  "Get the P3P header.
   Return a default P3P policy if not set"
  (get-env p3p-varnames default-p3p-header))

(def production?
  "Running in production?"
  (= "production" (get-env env-varnames)))

(def development?
  "Running in development environment?"
  (not production?))

(def domain
  "Get the domain the name cookies will be set on.
   Can be a wildcard e.g. '.foo.com'.
   If undefined we'll just use the FQDN of the host"
  (get-env domain-varnames))