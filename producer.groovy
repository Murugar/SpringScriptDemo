package com.rabbitmq.jms.sample

@Grab("com.rabbitmq.jms:rabbitmq-jms:1.5.0")

@Grab("commons-lang:commons-lang:2.6")

import com.rabbitmq.jms.admin.RMQConnectionFactory
import org.apache.commons.lang.math.RandomUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled

@EnableScheduling
@Log
@EnableJms
class StockQuoter {

  def stocks = ["TTT", "RRR", "SSS"] 
  def lastPrice = ["TTT": 200.0, "RRR": 222.0, "SSS": 777.0] 

  @Autowired
  JmsTemplate jmsTemplate

  @Bean
  ConnectionFactory connectionFactory() {
    new RMQConnectionFactory()
  }

  @Scheduled(fixedRate = 1000L)
  void publishQuote() {
    // Pick a random stock symble
    Collections.shuffle(stocks, new Random())
    def symbol = stocks[0]

    // Toss a coin and decide if the price goes...
    if (RandomUtils.nextBoolean()) {
      // ...up by a random 0% - 10%
      lastPrice[symbol] = Math.round(lastPrice[symbol] * (1 + RandomUtils.nextInt(10)/100.0) * 100) / 100
    } else {
      // ...or down by a similar random amount
      lastPrice[symbol] = Math.round(lastPrice[symbol] * (1 - RandomUtils.nextInt(10)/100.0) * 100) / 100
    }

    // Log new price locally
    log.info "Sending Stock Quote...${symbol} is now ${lastPrice[symbol]}"

    // Coerce a javax.jms.MessageCreator
    def messageCreator = { session ->
      session.createObjectMessage("Message...${symbol} is now ${lastPrice[symbol]}".toString())
    } as MessageCreator

    // And publish to RabbitMQ using Spring's JmsTemplate
    jmsTemplate.send("rabbit-trader-channel", messageCreator)
  }

}

