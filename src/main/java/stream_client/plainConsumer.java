package stream_client;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.Done;
import akka.NotUsed;
import akka.actor.*;
import akka.japi.Pair;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.*;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


public class plainConsumer {
	
	
	protected final static ActorSystem system = ActorSystem.create("example");

	  protected final static Materializer materializer = ActorMaterializer.create(system);

	  protected final int maxPartitions = 100;

	  protected <T> Flow<T, T, NotUsed> business() {
	    return Flow.create();
	  }
	  
	// #settings
	  final static Config config = system.settings().config().getConfig("akka.kafka.consumer");
	  final static ConsumerSettings<String, String> consumerSettings =
	      ConsumerSettings.create(config, new StringDeserializer(), new StringDeserializer())
	          .withBootstrapServers("localhost:9092")
	          .withGroupId("group1")
	          .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	  // #settings

	  final ConsumerSettings<String, String> consumerSettingsWithAutoCommit =
	          // #settings-autocommit
	          consumerSettings
	                  .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
	                  .withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
	          // #settings-autocommit
	  
	  
	  public static void main(String[] args) {
		    demo();
		  }
	  
	  public static void demo() {
		  
	  
		    // #atMostOnce
//		    Consumer.Control control =
//		        Consumer
//		            .atMostOnceSource(consumerSettings, Subscriptions.topics("test"))
//		            .mapAsync(10, record -> business(record.key(), record.value()))
//		            .to(Sink.foreach(it -> System.out.println("Done with " + it)))
//		            .run(materializer);
//
//		    // #atMostOnce
//		  }
//
//		  // #atMostOnce
//		  static CompletionStage<String> business(String key, byte[] value) { // .... }
//		  // #atMostOnce
//		    return CompletableFuture.completedFuture("");
//		  }

		  
		    Consumer.DrainingControl<Done> control =
		            Consumer.atMostOnceSource(consumerSettings, Subscriptions.topics("test"))
		                .map(ConsumerRecord::value)
		                .toMat(Sink.foreach(m -> {System.out.println(m
		                		);}), Keep.both())
		                .mapMaterializedValue(Consumer::createDrainingControl)
		                .run(materializer);
	  }
		    
}
