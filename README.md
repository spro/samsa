# samsa

A microservices library with req-rep and pub-sub and streams, oh my.

## Quick look

```coffeescript
samsa = require 'samsa'
hello_methods =
    sayHello: ({name}) -> "Hello, #{name}!"
hello_service = new samsa.Member 'hello', hello_methods
```

```coffeescript
samsa = require 'samsa'
hello_client = new samsa.Member
reply = await hello_client.request 'hello', 'sayHello', {name: "Jeffrey"}
assert reply == "Hello, Jeffrey!"
```

## Architecture

Samsa builds upon Kafka producers and consumers to enable easy request-reply and pub-sub between microservices. All this is encapsulated in a single Node class, which may or may not be named.

A named Node acts as both a Service and a Client. As a Service it listens for requests and events to act on, and sends replies or errors back as necessary. An unnamed Node can act as a Client only, sending requests or publishing events to listening Services.

### What that means in Kafka

* Each Node acting as a Service (that is, it has a name) consumes a Kafka Topic of the same name.
* Requests and Events are sent to services by name, so they use a Kafka Producer to add messages to that Topic.
* Replies are sent on a Topic suffixed "-reply", i.e. the "hello" Service sends replies on the "hello-reply".

All Services act as part of the same Kafka Consumer Group, so they receive a balanced subset of requests.

## Thanks

This Node.js implementation is written around [kafka-node](https://github.com/SOHU-Co/kafka-node).


