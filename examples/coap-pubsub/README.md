CoAP Publish-Subscribe Examples
===============================

coap-pubsub-broker-example.c
----------------------------

A straightforward broker implementation that simply enables the broker.


coap-pubsub-client-example.c
----------------------------

Comprehensive client that demonstrates all the commands against a hardcoded broker. Make sure the broker address in the `uip_ip6addr()` is correct before use. To test subscription add a topic with the path `topic2` to the broker.

This example has been tested in the Cooja simulator using wismotes, along with coap-pubsub-broker-example.c and a rpl-border-router. 

coap-pubsub-combined-example.c
-----------------

Combines a broker and client on the same node. The client scans the RPL neighbor table for brokers by attempting the DISCOVER command on each of the neighboring nodes. When a broker is found, the client behaves like the other client example.

