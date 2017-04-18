/*
 * Copyright (c) 2017, Jussi Haikara
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This file is part of the Contiki operating system.
 */

/**
 * \file
 *      CoAP Publish-Subscribe client API
 * \author
 *      Jussi Haikara <haikara@kth.se>
 */


#include "contiki.h"
#include "er-coap.h"
#include "er-coap-engine.h"
#include "er-coap-observe-client.h"
#include "rest-engine.h"

#ifndef COAP_PUBSUB_CLIENT_H_
#define COAP_PUBSUB_CLIENT_H_

#define COAP_PUBSUB_MESSAGE_TYPE COAP_TYPE_CON

#define COAP_PUBSUB_MAX_CREATE_MESSAGE_LEN 64
#define COAP_PUBSUB_MAX_CONTENT_LEN 256
#define COAP_PUBSUB_MAX_URL_LEN 63

typedef struct broker_s broker_t;

struct broker_s {
  broker_t *next;
  uip_ipaddr_t address;
  uint16_t port;
  char base_url[COAP_PUBSUB_MAX_URL_LEN + 1];
  uint8_t last_response_code;
};

typedef struct client_topic_s client_topic_t;

struct client_topic_s {
  client_topic_t *next;
  broker_t *broker;
  void (*notification_callback)();
  uint8_t flags;
  uint32_t observe_seq;
  char url[COAP_PUBSUB_MAX_URL_LEN + 1]; /* Topic path relative to function set */
  uint16_t content_type;
  uint8_t *content;
  size_t content_len;
  uint32_t max_age;
  uint8_t last_response_code;
};

typedef enum {
  SUBSCRIPTION_ENABLED = 1, /* If we want to receive notifications. */
  IS_SUBSCRIBED = 2,        /* If we are confirmed subscribed. */
  RECEIVE_OUT_OF_ORDER = 4  /* Whether to receive out of order notifications. */

} client_topic_flags_t;

typedef enum {
  NO_CONTENT_2_04 = 68,

  UNSUPPORTED_CONTENT_FORMAT_4_15 = 143,
  TOO_MANY_REQUESTS_4_29 = 157,

} coap_pubsub_code_t;

void process_response_chunk(void *response);
void process_discovery_response(void *response);

coap_packet_t *coap_pubsub_setup_discover(list_t *topic_list, broker_t *broker);
coap_packet_t *coap_pubsub_setup_create(client_topic_t *topic);
coap_packet_t *coap_pubsub_setup_remove(client_topic_t *topic);
coap_packet_t *coap_pubsub_setup_publish(client_topic_t *topic);
coap_packet_t *coap_pubsub_setup_read(client_topic_t *topic);
coap_packet_t *coap_pubsub_setup_subscribe(client_topic_t *topic);
coap_packet_t *coap_pubsub_setup_unsubscribe(client_topic_t *topic);


/*===========================
 * API
 *===========================
 */


/**
 * \brief Initializes the pubsub client
 */
void coap_pubsub_init_client();

/*---------------------------------------------------------------------------*/
/**
 * \brief             Discovers the function set path and topics of a broker
 * \param topic_list  pointer to a list_t with client_topic_t structs to popluate 
 * \param broker      pointer to the broker_t to discover.
 *
 * Pass a broker struct with the address and port filled in. The function set path
 * will be written to the .base_url member if found.
 * Prints a response code in the .last_response_code member of the broker.
 * Pass a NULL as the topic list to only discover the broker function set url.
 */
#define COAP_PUBSUB_DISCOVER(topic_list, broker)            \
{                                                           \
  COAP_BLOCKING_REQUEST(                                    \
      &(broker)->address, (broker)->port                    \
      , coap_pubsub_setup_discover((topic_list), (broker))  \
      , process_discovery_response)                         \
}

/*---------------------------------------------------------------------------*/
/**
 * \brief       Creates a topic
 * \param topic client_topic_t with topic parameters
 *
 * Requires the .url, .max_age and .content_type members to be set.
 */
#define COAP_PUBSUB_CREATE(topic)           \
{                                           \
  COAP_BLOCKING_REQUEST(                    \
      &(topic)->broker->address             \
      , (topic)->broker->port               \
      , coap_pubsub_setup_create(topic)     \
      , process_response_chunk)             \
}

/*---------------------------------------------------------------------------*/
/**
 * \brief       Removes a topic
 * \param topic client_topic_t with topic parameters
 */
#define COAP_PUBSUB_REMOVE(topic)           \
{                                           \
  COAP_BLOCKING_REQUEST(                    \
      &(topic)->broker->address             \
      , (topic)->broker->port               \
      , coap_pubsub_setup_remove(topic)     \
      , process_response_chunk)             \
}

/*---------------------------------------------------------------------------*/
/**
 * \brief       Publishes data to a topic
 * \param topic client_topic_t with topic parameters
 *
 * Requires the .content, .content_len and .max_age members to be set.
 */
#define COAP_PUBSUB_PUBLISH(topic)          \
{                                           \
  COAP_BLOCKING_REQUEST(                    \
      &(topic)->broker->address             \
      , (topic)->broker->port               \
      , coap_pubsub_setup_publish(topic)    \
      , process_response_chunk)             \
}

/*---------------------------------------------------------------------------*/
/**
 * \brief       Reads data from a topic
 * \param topic client_topic_t with topic parameters
 */
#define COAP_PUBSUB_READ(topic)             \
{                                           \
  COAP_BLOCKING_REQUEST(                    \
      &(topic)->broker->address             \
      , (topic)->broker->port               \
      , coap_pubsub_setup_read(topic)       \
      , process_response_chunk)             \
}

/*---------------------------------------------------------------------------*/
#if COAP_OBSERVE_CLIENT == 1
/**
 * \brief       Subscribes to a topic
 * \param topic client_topic_t with topic parameters
 *
 * Set the notification_callback member to a function pointer to receive
 * callbacks in response to publishes.
 */
#define COAP_PUBSUB_SUBSCRIBE(topic)        \
{                                           \
  COAP_BLOCKING_REQUEST(                    \
      &(topic)->broker->address             \
      , (topic)->broker->port               \
      , coap_pubsub_setup_subscribe(topic)  \
      , process_response_chunk)             \
}

/*---------------------------------------------------------------------------*/
/**
 * \brief       Unsubscribes from a topic
 * \param topic client_topic_t with topic parameters
 */
#define COAP_PUBSUB_UNSUBSCRIBE(topic)        \
{                                             \
  COAP_BLOCKING_REQUEST(                      \
      &(topic)->broker->address               \
      , (topic)->broker->port                 \
      , coap_pubsub_setup_unsubscribe(topic)  \
      , process_response_chunk)               \
}

#endif

#endif /* COAP_PUBSUB_CLIENT_H_ */

