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

(ns snowplow.clojure-collector.beanstalk
  "AWS Elastic Beanstalk-specific functionality"
  (:use [compojure.core :only (HEAD defroutes)])
  (:require [snowplow.clojure-collector.core :as clojure-collector]
            [compojure.core :as compojure]))

(defroutes app
  ; This HEAD route is here because Amazon's Elastic Beanstalk determines if
  ; your application is up by whether it responds successfully to a
  ; HEAD request at /
  (HEAD "/" [] "")
  clojure-collector/app)
