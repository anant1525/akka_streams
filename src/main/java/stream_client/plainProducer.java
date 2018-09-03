package stream_client;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.serialization.StringSerializer;

import com.typesafe.config.Config;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class plainProducer {


	
	final static ActorSystem system = ActorSystem.create("example");

	  final static Materializer materializer = ActorMaterializer.create(system);
	
	
	final static Config config = system.settings().config().getConfig("akka.kafka.producer");
	final static ProducerSettings<String, String> producerSettings =
	    ProducerSettings
	        .create(config, new StringSerializer(), new StringSerializer())
	        .withBootstrapServers("localhost:9092");
	
	  final static KafkaProducer<String, String> kafkaProducer =
		      producerSettings.createKafkaProducer();
		  // #producer
	
	public static void main(String[] args) {
	demo();
	    
	  }
	
	  protected static void terminateWhenDone(CompletionStage<Done> result) {
		    result
		      .exceptionally(e -> {
		        system.log().error(e, e.getMessage());
		        return Done.getInstance();
		      })
		      .thenAccept(d -> system.terminate());
		  }
	
	
	public static void demo() {
        // #plainSinkWithProducer
        CompletionStage<Done> done =
            Source.range(1, 100)
                .map(number -> number.toString())
                .map(value -> new ProducerRecord<String, String>("test", "Anant" + value))
                .runWith(Producer.plainSink(producerSettings, kafkaProducer), materializer);
        // #plainSinkWithProducer

        terminateWhenDone(done);
    }
	
	
}
