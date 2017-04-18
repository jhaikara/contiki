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
 *      CoAP pubsub client example.
 *
 *      Demonstrates the commands in the pubsub client API.
 *
 * \author
 *      Jussi Haikara <haikara@kth.se>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "contiki.h"
#include "contiki-net.h"
#include "lib/list.h"
#include "rest-engine.h"
#include "coap-pubsub-client.h"

#define DEBUG 0
#if DEBUG
#define PRINTF(...) printf(__VA_ARGS__)
#else
#define PRINTF(...)
#endif

PROCESS(coap_client, "CoAP pubsub client example");
AUTOSTART_PROCESSES(&coap_client);

static broker_t broker;
static client_topic_t topic, topic_dir, topic_temp, topic_sub;
static uint8_t content_buffer[128];
static uint8_t sub_buffer[128];
static struct etimer publish_timer;

LIST(dyn_topic_list);
static client_topic_t dyn_topics[4];

void
subscribe_receive_callback(client_topic_t *topic)
{
  process_post(&coap_client, PROCESS_EVENT_MSG, topic);
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(coap_client, ev, data)
{
  static int i, val = 0;
  static int topics_created = 0;
  static client_topic_t *dyn_topic;

  PROCESS_BEGIN();

  topic_dir.broker = &broker;
  topic.broker = &broker;
  topic_temp.broker = &broker;
  topic_sub.broker = &broker;

  /* Broker address hardcoded to link local cooja address. */
  uip_ip6addr(&broker.address, 0xfe80, 0, 0, 0, 0x0200, 0, 0, 0x0002);
  broker.port = UIP_HTONS(COAP_DEFAULT_PORT);

  strcpy((char *)&topic_dir.url, "dir");
  topic_dir.content_type = 40;
  strcpy((char *)&topic.url, "dir/topic1");
  strcpy((char *)&topic_sub.url, "topic2");
  strcpy((char *)&topic_temp.url, "temporary");
  topic_sub.content_type = 0;
  topic.content_type = 0;
  topic.content = (uint8_t *)&content_buffer;
  topic_sub.content = (uint8_t *)&sub_buffer;
  topic_sub.notification_callback = subscribe_receive_callback;
  topic.max_age = 30;

  list_init(dyn_topic_list);

  for(i = 0; i < 4; i++) {
    list_push(dyn_topic_list, (void *)&dyn_topics[i]);
  }

  etimer_set(&publish_timer, CLOCK_SECOND * 10);

  coap_pubsub_init_client();

  PROCESS_PAUSE();

  /* Loop until we have found the broker function set and created the topics we need */
  while(!topics_created) {
    PROCESS_YIELD();
    if(!etimer_expired(&publish_timer)) {
      continue;
    }
    etimer_reset(&publish_timer);

    PRINTF("DISCOVER function set\n");
    COAP_PUBSUB_DISCOVER(&dyn_topic_list, &broker);
    PRINTF("DISCOVER finished, return code %d\n", broker.last_response_code);

    PRINTF("Broker function set at: %s\n", broker.base_url);

    /* The DISCOVER operation can return a set of topics on the broker. */
    dyn_topic = (client_topic_t *)list_head(dyn_topic_list);
    while(dyn_topic) {
      PRINTF("Topic on broker: %s\n", dyn_topic->url);
      dyn_topic = list_item_next(dyn_topic);
    }

    if(strlen(broker.base_url) > 1) {

      PRINTF("CREATE dir topic\n");
      COAP_PUBSUB_CREATE(&topic_dir);

      PRINTF("CREATE finished, return code %d\n", topic_dir.last_response_code);

      if(topic_dir.last_response_code == CREATED_2_01
         || topic_dir.last_response_code == FORBIDDEN_4_03) {
        PRINTF("Dir topic created\n");

        PRINTF("CREATE content topic\n");

        COAP_PUBSUB_CREATE(&topic);

        PRINTF("CREATE finished, return code %d\n", topic.last_response_code);

        if(topic.last_response_code == CREATED_2_01
           || topic.last_response_code == FORBIDDEN_4_03) {
          PRINTF("Content topic created\n");

          topics_created = 1;
        }
      }
    }
  }

  /* Test creating and deleting a topic. */
  PRINTF("CREATE temp topic\n");
  COAP_PUBSUB_CREATE(&topic_temp);

  PRINTF("CREATE finished, return code %d\n", topic_temp.last_response_code);

  if(topic_temp.last_response_code == CREATED_2_01
     || topic_temp.last_response_code == FORBIDDEN_4_03) {
    PRINTF("Temp topic created\n");

    PRINTF("REMOVE temp topic\n");

    COAP_PUBSUB_REMOVE(&topic_temp);

    PRINTF("REMOVE finished, return code %d\n", topic_temp.last_response_code);

    if(topic_temp.last_response_code == DELETED_2_02) {
      PRINTF("Temp topic removed\n");
    }
  }

  /* Test subscribing to a topic.
   * The broker needs to have a topic called "/topic2" preset. */
  PRINTF("SUBSCRIBE to topic2\n");
  COAP_PUBSUB_SUBSCRIBE(&topic_sub);

  PRINTF("SUBSCRIBE finished, return code %d\n", topic_sub.last_response_code);

  if(topic_sub.last_response_code == CONTENT_2_05
     || topic_sub.last_response_code == NO_CONTENT_2_04) {
    PRINTF("Subscription sent\n");
  } else {
    PRINTF("Subscription failed: %.*s\n", topic_sub.content_len, topic_sub.content);
  }

  /* Wait for updates on the subscribed topic, and perodically
   * read from and then publish to our created topic. */
  while(1) {
    PROCESS_WAIT_EVENT();

    if(ev == PROCESS_EVENT_MSG) {
      if((client_topic_t *)data == &topic_sub) {
        PRINTF("Published content received: %.*s\n", topic_sub.content_len, topic_sub.content);

        if(!(topic_sub.flags & IS_SUBSCRIBED)) {
          PRINTF("Subscription canceled by broker!");
        }
      } else {
        PRINTF("Unknown publish received\n");
      }
    }
    if(etimer_expired(&publish_timer)) {

      PRINTF("READ topic\n");

      COAP_PUBSUB_READ(&topic);

      PRINTF("read finished, return code %d\n", topic.last_response_code);

      if(topic.last_response_code == CONTENT_2_05) {
        PRINTF("Topic content: %.*s\n", topic.content_len, topic.content);
      } else if(topic.last_response_code == NO_CONTENT_2_04) {
        PRINTF("No topic content\n");
      } else {
        PRINTF("READ failed, error message: %.*s\n", topic.content_len, topic.content);
      }

      val++;

      sprintf((char *)topic.content, "%d", val);
      topic.content_len = strlen((char *)topic.content); /* Does not include the \0 of the string in the message. */

      PRINTF("PUBLISH value %s, len %u\n", topic.content, topic.content_len);

      COAP_PUBSUB_PUBLISH(&topic);

      PRINTF("PUBLISH finished, return code %d\n", topic.last_response_code);

      if(topic.last_response_code == NOT_FOUND_4_04) {
        PRINTF("No topic!\n");
      }

      etimer_reset(&publish_timer);
    }
  }

  PROCESS_END();
}
