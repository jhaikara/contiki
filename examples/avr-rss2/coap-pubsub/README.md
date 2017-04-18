CoAP Publish-Subscribe Examples for the avr-rss2 platform
=========================================================

broker-avr-rss2.c
-----------------

Broker for the avr-rss2 platform. Adds a topic called `/broker/temp` and updates it with the temperature sensor value.

client-avr-rss2.c
-----------------

Temperature publisher client for the avr-rss2. This client scans the RPL neighbor table for brokers by attempting the DISCOVER command on each of the neighboring nodes. When a broker is found, the node creates a topic called `/<node-mac-address>/temp`, and publishes the value of the temperature sensor every 5 seconds.

Note: The topic path is too long for the default max path length set in the Erbium CoAP observe client. Reconfigure `COAP_OBSERVER_URL_LEN` in `contiki/apps/er-coap/er-coap-observe.h` to at least 25 for the broker.
