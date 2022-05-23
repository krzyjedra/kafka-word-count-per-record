import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringDeserializer, StringSerializer}

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps

object RecordCounter extends App {

  val consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092")
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer")
  consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  val consumer = new KafkaConsumer[String, String](consumerProps)

  import scala.jdk.CollectionConverters._

  consumer.subscribe(Seq("input").asJava)

  val producerProps = new Properties()
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer])
  val producer = new KafkaProducer[String, Int](producerProps)

  while (true) {
    val records = consumer.poll(1.seconds.toJava).asScala
    records.foreach { r =>
      r.value()
        .split("\\W+")
        .groupBy(identity)
        .view
        .mapValues(_.length)
        .foreach { case MyR @ (word, occs) =>
          println(MyR)
        }
    }
  }
}
