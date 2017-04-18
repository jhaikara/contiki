CONTIKI_PROJECT = coap-pubsub-broker-example coap-pubsub-client-example client-rabbitmq coap-pubsub-combined-example

all: $(CONTIKI_PROJECT)

CFLAGS += -DPROJECT_CONF_H=\"project-conf.h\"

APPS += er-coap
APPS += rest-engine
APPS += coap-pubsub

CONTIKI_WITH_IPV6 = 1
CONTIKI=../..
include $(CONTIKI)/Makefile.include

