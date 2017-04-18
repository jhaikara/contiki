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
 *      CoAP pubsub broker that publises the temperature.
 * \author
 *      Jussi Haikara <haikara@kth.se>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "contiki.h"
#include "sys/etimer.h"
#include "contiki-net.h"
#include "rest-engine.h"
#include "coap-pubsub-broker.h"
#include "dev/temp-sensor.h"


PROCESS(coap_broker, "CoAP pubsub broker example - avr-rss2");
AUTOSTART_PROCESSES(&coap_broker);

PROCESS_THREAD(coap_broker, ev, data)
{
  static char temp_str[32];
  static topic_t *temperature;
  static struct etimer update_timer;

  PROCESS_BEGIN();
  PROCESS_PAUSE();

  SENSORS_ACTIVATE(temp_sensor);
  coap_pubsub_init_broker();
  temperature = coap_pubsub_broker_create_topic("ps/broker/temp", TEXT_PLAIN); 

  etimer_set(&update_timer, CLOCK_SECOND * 10);
  while(1){
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&update_timer));

    sprintf(temp_str, "%-5.2f", ((double) temp_sensor.value(0)/100.));
    printf("Time: %lu, Updating temperature: %s\n",clock_seconds(), temp_str);
    coap_pubsub_broker_set_topic_content(temperature, (uint8_t *)temp_str, strlen(temp_str));

    etimer_restart(&update_timer);
  }


  PROCESS_END();
}
