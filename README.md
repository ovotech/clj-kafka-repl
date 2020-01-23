clj-kafka-repl
==============

General purpose REPL functions for interrogating Kafka.

[API](https://ovotech.github.io/clj-kafka-repl/)

Getting started
---------------

### Configuration

Before starting you'll need to create a configuration file in `~/.clj-kafka-repl/config.edn`. It should be of the form:

```clj
{:profiles
 {:prod
  {:kafka-config
   ; As defined here: https://kafka.apache.org/documentation/#configuration
   {:bootstrap.servers "host1:port1,host2:port2,...",
    ...},

   ; If you wish to use Apache Avro
   :schema-registry-config
   {:base-url "host:port",
    :username "schema-registry-username",
    :password "schema-registry-password"}},

  :nonprod
  {...}}}
``` 

### Running

Once configured, just start a REPL. As indicated at the prompt, just
run `(help)` to get a breakdown of available functions.

When you run one of the functions you will need to specify which profile to use. This is done with the `with` macro; E.g.:

```clj
(with :nonprod (kafka/get-latest-offsets "your-topic"))
```

Serialization/deserialization
-----------------------------

Several serialization/deserialization formats are supported:

* `:string`: as plain text. This is the default for both keys and values.
* `:edn`: as edn
* `:avro`: Use [Apache Avro](https://avro.apache.org/) and a [Confluent schema registry](https://docs.confluent.io/current/schema-registry/index.html) defined in the configuration.
* `:nippy`: compressed/decompressed using [nippy](https://github.com/ptaoussanis/nippy).

You can either specify these formats in calls to the functions or in the configuration using the
`:default-key-serializer`, `:default-value-serializer`, `:default-key-deserializer` and `:default-value-deserializer`
top-level configuration options. For example, to serialize/deserialize values using Avro (but continue to
serialize/deserialize as strings):

```clj
{:default-value-serializer   :avro
 :default-value-deserializer :avro

 :profiles {...}}
```
