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
(def ^:const env-varname "SP_ENV")
(def ^:const p3p-varname "SP_P3P")
(def ^:const domain-varname "SP_DOMAIN")
(def ^:const duration-varname "SP_DURATION")
(def ^:const redirect-varname "SP_REDIRECT")
; Don't add any more!

;; Defaults
(def ^:const default-p3p-header "policyref=\"/w3c/p3p.xml\", CP=\"NOI DSP COR NID PSA OUR IND COM NAV STA\"")
(def ^:const default-duration 31556900) ; A year

(def duration
  "Get the duration (in seconds) the
   cookie should last for"
  (get (System/getenv) duration-varname default-duration))

(def p3p-header
  "Get the P3P header.
   Return a default P3P policy if not set"
  (get (System/getenv) p3p-varname default-p3p-header))

(def redirect-url
  "Get the redirect URL. Can be nil"
  (get (System/getenv) redirect-varname))

(def production?
  "Running in production?"
  (= "production" (get (System/getenv) env-varname)))

(def development?
  "Running in development environment?"
  (not production?))

(def domain
  "Get the domain the name cookies will be set on.
   Can be a wildcard e.g. '.foo.com'.
   If undefined we'll just use the FQDN of the host"
  (get (System/getenv) domain-varname))