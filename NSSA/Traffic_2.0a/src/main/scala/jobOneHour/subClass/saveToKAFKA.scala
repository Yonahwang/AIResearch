package jobOneHour.subClass

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2018/6/12.
  */

trait saveToKAFKA{

  class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable{
    /* This is the key idea that allows us to work around running into
       NotSerializableExceptions. */
    lazy val producer = createProducer()

    def send(topic: String, key: K, value: V): Future[RecordMetadata] =
      producer.send(new ProducerRecord[K, V](topic, key, value))

    def send(topic: String, value: V): Future[RecordMetadata] =
      producer.send(new ProducerRecord[K, V](topic, value))
  }

  object KafkaSink {
    def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
      val createProducerFunc = () => {
        val producer = new KafkaProducer[K, V](config)
        sys.addShutdownHook {
          // Ensure that, on executor JVM shutdown, the Kafka producer sends
          // any buffered messages to Kafka before shutting down.
          producer.close()
        }
        producer
      }
      new KafkaSink(createProducerFunc)
    }
    def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
  }


  def toKafka(mysparksession: SparkSession, node_topic:(String, String), data:DataFrame) = {
    val dataset: Dataset[String] = data.toJSON
    val sc = mysparksession.sparkContext
    //  kafka主结点 "192.168.2.116:9092"
    val kafka_node = node_topic._1
    //  broken的topic
    val kafka_topic = node_topic._2
    //广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        //        p.setProperty("bootstrap.servers", "192.168.2.116:9092")
        p.setProperty("bootstrap.servers", kafka_node)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    //输出到kafka
    dataset.rdd.foreach(record=>{
      //      kafkaProducer.value.send("topic", record)
      kafkaProducer.value.send(kafka_topic, record)
    })
  }


}