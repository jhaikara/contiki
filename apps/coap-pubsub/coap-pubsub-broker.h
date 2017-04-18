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
 *      CoAP Publish-Subscribe broker headers
 * \author
 *      Jussi Haikara <haikara@kth.se>
 */

#include "contiki.h"
#include "er-coap.h"
#include "rest-engine.h"
#include "sys/clock.h"

#ifndef COAP_PUBSUB_BROKER_H_
#define COAP_PUBSUB_BROKER_H_

#define COAP_PUBSUB_BROKER_ALLOW_CLIENT_TOPIC_CREATION 1
#define COAP_PUBSUB_BROKER_ALLOW_CLIENT_TOPIC_DELETION 1

#define COAP_PUBSUB_BROKER_BASE_URI "ps"

#define COAP_PUBSUB_BROKER_MAX_TOPICS 16

#define COAP_PUBSUB_MAX_CREATE_MESSAGE_LEN 64
#define COAP_PUBSUB_MAX_CONTENT_LEN 256

#define COAP_PUBSUB_MEM_STATIC 0
#define COAP_PUBSUB_MEM_MALLOC 1
#define COAP_PUBSUB_BROKER_MEMORY_ALLOC COAP_PUBSUB_MEM_MALLOC

/* Buffer sizes for static allocation */
#define COAP_PUBSUB_BROKER_STATIC_URL_SIZE 63
#define COAP_PUBSUB_BROKER_STATIC_ATTR_SIZE 63

struct topic_s {
  struct topic_s *next;
  resource_t *resource;
  uint8_t *content;
  size_t content_len;
  uint16_t content_format;
  struct timer max_age;
#if COAP_PUBSUB_BROKER_MEMORY_ALLOC == COAP_PUBSUB_MEM_STATIC
  resource_t data_resource;
  char data_res_url[COAP_PUBSUB_BROKER_STATIC_URL_SIZE + 1];
  char data_res_attributes[COAP_PUBSUB_BROKER_STATIC_ATTR_SIZE + 1];
  uint8_t data_content[COAP_PUBSUB_MAX_CONTENT_LEN];
#endif
};

typedef struct topic_s topic_t;

/*---------------------------------------------------------------------------*/
/**
 * \brief   Initializes the broker and sets up the base resource
 */
void coap_pubsub_init_broker();

/*---------------------------------------------------------------------------*/
/**
 * \brief               Creates a new topic
 * \param url           URL path to the topic relative to the root
 * \param resource_type The CoRE Resource format of the topic
 * \return              A pointer to the created topic or null if creation failed
 */
topic_t *coap_pubsub_broker_create_topic(char *url, coap_content_format_t resource_type);

/*---------------------------------------------------------------------------*/
/**
 * \brief       Deletes a topic
 * \param topic Pointer to a valid topic to be removed
 */
void coap_pubsub_broker_remove_topic(topic_t *topic);

/*---------------------------------------------------------------------------*/
/**
 * \brief   Get the list of topics on the broker.
 * \return  Pointer to the topic list.
 *
 * This list is unsafe to modify.
 */
list_t *coap_pubsub_broker_get_topic_list();

/*---------------------------------------------------------------------------*/
/**
 * \brief             Writes new content to a topic
 * \param topic       The topic to write to
 * \param content     Content buffer to copy data from
 * \param content_len Size of data to copy
 *
 * Subscribers are notified of the new content after a successful write.
 * Any failure is not indicated.
 */
void coap_pubsub_broker_set_topic_content(topic_t *topic, uint8_t *content, size_t content_len);

/*---------------------------------------------------------------------------*/
/**
 * \brief         Provides a pointer to the topic content
 * \param topic   The topic to get the content of
 * \param content Pointer to write the content buffer pointer to
 * \return        Length of the content buffer
 *
 * The pointer is set to null and 0 is returned if no content exists.
 */
size_t coap_pubsub_broker_get_topic_content(topic_t *topic, uint8_t **content);

#endif /* COAP_PUBSUB_BROKER_H_ */

