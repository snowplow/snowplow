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

;;;; Author:    Mark Butler (mailto:support@snowplowanalytics.com)
;;;; Copyright: Copyright (c) 2012-2013 Snowplow Analytics Ltd
;;;; License:   Apache License Version 2.0

(ns leiningen.aws
  (:require [leiningen.core.classpath :as classpath]
            [leiningen.ring.uberwar]
            [leiningen.ring.war]
            [robert.hooke]))

(defn skip-file? [original_func project war-path file]
  (and (not (re-find #".ebextensions.*" war-path)) (original_func project war-path file)))

(def hooked (atom 0))

(defn aws
  "Creates a valid Amazon web services WAR that
   is deployable to servlet containers."
  [project & args]
  (if (= 0 @hooked)
    (do
      (robert.hooke/add-hook #'leiningen.ring.war/skip-file? #'skip-file?)
      (swap! hooked inc)))
  (leiningen.ring.uberwar/uberwar project))
