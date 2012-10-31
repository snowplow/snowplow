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

(defproject snowplow/clojure-collector "0.1.0"
  :description "A SnowPlow event collector written in Clojure. AWS Elastic Beanstalk compatible."
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [ring "1.1.6"]
                 [compojure "1.1.3"]
                 [metrics-clojure "0.9.1"]
                 [metrics-clojure-ring "0.9.1"]
                 [commons-codec/commons-codec "1.7"]]
  :plugins      [[lein-ring "0.7.5"]
                 [lein-beanstalk "0.2.6"]]
  :ring {:handler snowplow.clojure-collector.beanstalk/app})