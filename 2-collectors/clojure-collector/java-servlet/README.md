# java-servlet

The Java servlet component of the Clojure Collector, written in Clojure using Ring and Compojure.

To test:

```
$ cd 2-collectors/clojure-collector/java-servlet
$ lein ring server-headless
WARNING!!! version ranges found for:
...
Started server on port 3000
```

Then browse to [http://localhost:3000/i](http://localhost:3000/i) on your host.

To build:

```
$ lein aws
Retrieving ring/ring-servlet/1.1.6/ring-servlet-1.1.6.pom from clojars
...
Created 2-collectors/clojure-collector/java-servlet/target/clojure-collector-1.1.0-standalone.war
```
