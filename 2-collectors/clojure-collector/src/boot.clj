(ns boot
  "A namespace only used while developing to 
   start and stop an internal server"
  (:use ring.adapter.jetty)
  (:use clojure.contrib.def)
  (:use snowplow.clojure-collector))
 
(def *port* 8081) ;; Override if needed

(defonce server (atom nil))

(defn stop 
  "Stops the web application"
  [] 
  (if-not (nil? @server)
    (do
      (.stop @server)
      (reset! server  nil))))
  
(defn start 
  "Starts or restarts the web application.
   From a repl:
    (require 'boot)
    (boot/start)"
  [] 
  (stop)
  (s (run-jetty #'app {:port *port* :join? false})
    (reset! server s)))

(defn restart
  "Restarts the web application"
  []
  (start))