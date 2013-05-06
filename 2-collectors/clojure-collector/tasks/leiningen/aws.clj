(ns leiningen.aws
  (:require [leiningen.core.classpath :as classpath]
            [leiningen.ring.uberwar]
            [leiningen.ring.war]
            [robert.hooke]))

(defn skip-file? [original_func project war-path file]
  (and (not (re-find #".ebextensions.*" war-path)) (original_func project war-path file)))

(def hooked (atom 0))

(defn aws
  "Creates a valid Amazon web services WAR that is deployable to servlet containers."
  [project & args]
  (if (= 0 @hooked)
    (do
      (robert.hooke/add-hook #'leiningen.ring.war/skip-file? #'skip-file?)
      (swap! hooked inc)))
  (leiningen.ring.uberwar/uberwar project))
