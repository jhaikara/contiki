CoAP Publish-Subscribe Interface
================================

Implements the CoAP pubsub standard according to [draft-koster-core-coap-pubsub-05](https://www.ietf.org/archive/id/draft-koster-core-coap-pubsub-05.txt). Note that both the standard and the implementation are experimental and not for operational use.

Both the broker and client interfaces are supported, allowing for client, broker and brokerless peer nodes. The broker and client both use the Erbium REST engine and CoAP implementation.

Broker
------

The broker is self-contained, and simply stores and returns topic content as sent by the publishing client. Other processes on the broker node can create and manipulate topics on the broker through the API functions.

To set up the broker:

1. Add coap-pubsub to APPS in the makefile along with Erbium.
`APPS += er-coap`
`APPS += rest-engine`
`APPS += coap-pubsub`

2. include the the `coap-pubsub-broker.h` header.

3. Call `coap_pubsub_init_broker()` to start the broker.


Client
------

The client API functions are called synchronously, making the process yield. Any automatic variables are invalid after each call. API functions take a struct representing the topic the function works on. Each topic struct has a pointer to another struct representing the broker the topic resides on. Before a function can be called, the structs need to be filled in with the appropriate information. 

The user process is responsible for managing the memory of the structs and their members. If the user processs needs to do the READ or SUBSCRIBE functions, a buffer for the return data needs to be set up and pointed to by the topic struct.

If the topic paths or the broker function set path is unknown, the DISCOVER command can be used to find them. The discover function will fill in the broker function set path in the broker struct, and populate a list of topics if one is passed to the function.

To set up the client:

1. Add coap-pubsub to APPS in the makefile along with Erbium.
`APPS += er-coap`
`APPS += rest-engine`
`APPS += coap-pubsub`

2. include the the `coap-pubsub-client.h` header.

3. Call `coap_pubsub_init_client()` to initiate the client.


Unsupported features
--------------------

The broker does not support the Simple Flow Control defined by the standard draft. 
