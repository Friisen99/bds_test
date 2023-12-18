package bds;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.core.fs.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;

import EventManager.EventComparator;
import EventManager.EventParser;
import EventManager.JoinTagAndLikeEvents;
import EventManager.KeySelectorForJoinedEvent;
import EventManager.KeySelectorForLikeEvent;
import EventManager.KeySelectorForTagEvent;
import WatermarkManager.BDSWatermarkStrategy;

public class DataStreamJob 
{
	static boolean useTestData = false;
	static boolean useFlinkContainer = false;
	static Time windowSlide = Time.milliseconds(5000);
	static Time windowLength = Time.milliseconds(15000);
	static String kafka = useFlinkContainer ? "broker:9092" : "localhost:19092";
	static String dataPath = "C:/....../BDS-Programming-Assignment-2/Data/";
	
	// Sets up the execution environment, which is the main entry point to building Flink applications.
	final static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	
	public static void main(String[] args) throws Exception 
	{	
		if (useTestData)
		{
			ProcessStreamFromFiles(dataPath + "SampleDataSet/InOrderStream/");
			System.out.println("Finish query with SampleDataSet + InOrderStream");
			
			ProcessStreamFromFiles(dataPath + "SampleDataSet/OutOfOrderStream/");
			System.out.println("Finish query with SampleDataSet + OutOfOrderStream");
			
			ProcessStreamFromFiles(dataPath + "DataSet/InOrderStream/");
			System.out.println("Finish query with DataSet + InOrderStream");
			
			ProcessStreamFromFiles(dataPath + "DataSet/OutOfOrderStream/");
			System.out.println("Finish query with DataSet + OutOfOrderStream");
		}
		else
		{
			ProcessStreamFromKafka();
			System.out.println("Finish query with kafka stream");
		}
		
		System.out.println("All queries are done!");
	}
	
	static SingleOutputStreamOperator<String> DoQuery(
			SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> tagStream,
			SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> likeStream)
	{
		return tagStream
				.map(e -> 
				{
					//System.out.println("Receive tag event: <" + e.f0 + ", " + e.f1 + ", " + e.f2 + ">");
					return Tuple3.of(e.f0, e.f1, e.f2);
				})
				.returns(Types.TUPLE(Types.LONG, Types.INT, Types.INT))
				// Do the following steps by using the given key selectors and functions in EventManager package
				// You can also implement your own, but remember to include all codes in this file
			    // ** STEP 1: do window join
				// https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/joining/#window-join
			    // ...
			    // ** STEP 2: do filter
			    // https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/overview/#filter
		        // ...
		        // ** STEP 3: map the event to a different format, so to get prepared for the next step
		        // https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/overview/#map
		        // ...
		        // ** STEP 4: do window aggregate
		        // https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/operators/windows/#aggregatefunction
		        // ...
		        // ** STEP 5: map the event to a format, so it is easier to print
		        .map(e -> 
		        {
		        	System.out.println("output: <" + e.f0 + ", " + e.f1 + ">");
		        	return e.f0 + " " + e.f1;
		        })
		        .returns(Types.STRING);
	}
	
	static void ProcessStreamFromFiles(String dataDirectory) throws Exception
	{
		var tagStream = GetStreamFromLocalFile(dataDirectory, "tag");
		var likeStream = GetStreamFromLocalFile(dataDirectory, "like");
		
		// create the FlinkResult folder if not exists
		Files.createDirectories(Paths.get(dataDirectory + "FlinkResult/"));
		
		// clear the stale content in the FlinkResult folder
		FileUtils.cleanDirectory(new File(dataDirectory + "FlinkResult/"));
		
		// https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/filesystem/#row-encoded-formats
		var fileSink = FileSink
			    .forRowFormat(new Path(dataDirectory + "FlinkResult"), new SimpleStringEncoder<String>("UTF-8"))
			    .withOutputFileConfig(new OutputFileConfig("FlinkResult", ".txt"))
			    .withRollingPolicy
			    (
			        DefaultRollingPolicy
			            .builder()
			            .withRolloverInterval(Duration.ofSeconds(5))
			            .withInactivityInterval(Duration.ofSeconds(5))
			            .withMaxPartSize(MemorySize.ofMebiBytes(1))
			            .build()
			    )
				.build();
		
        var res = DoQuery(tagStream, likeStream);
		
		res
	        .sinkTo(fileSink)
	        // set parallelism as 1, so the results are printed to one file
	        .setParallelism(1);
	    
		// Execute program, beginning computation.
		env.execute("Process stream from files");
		
		CheckCorrectness(dataDirectory);
	}
	
	static void ProcessStreamFromKafka() throws Exception
	{
		var tagStream = GetStreamFromKafka("tag");
		var likeStream = GetStreamFromKafka("like");
		
		var kafkaSink = KafkaSink
				    .<String>builder()
				    .setBootstrapServers(kafka)
				    .setRecordSerializer
				    (
				        KafkaRecordSerializationSchema
				        	.builder()
				            .setTopic("query-result")
				            .setKeySerializationSchema(new SimpleStringSchema())
				            .setValueSerializationSchema(new SimpleStringSchema())
		                    .build()
				    )
				    .build();
		
		var res = DoQuery(tagStream, likeStream);
		
		res
	        .sinkTo(kafkaSink)
	        // set parallelism as 1, so the results are printed to one file
	        .setParallelism(1);
	     
		System.out.println("Ready to process events from kafka");
		
		// Execute program, beginning computation.
		env.execute("Process stream from kafka");
	}
	
	static SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> GetStreamFromKafka(String topic) throws Exception
	{	
		// https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/
		var source = KafkaSource
			    .<byte[]>builder()
				.setBootstrapServers(kafka)  // kafka port
				.setGroupId(topic)           // consumer ID
				.setTopics(topic)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(ByteArrayDeserializer.class))
				.build();
		System.out.println("Subscribe to kafka topic: " + topic);				
		return env
				.fromSource(source, WatermarkStrategy.noWatermarks(), topic + "-source")
		        .map((byte[] bytes) -> EventParser.ParseEvent(bytes))
	            // you need to specify which type is returned from the "map" operator
		        // https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/java_lambdas/
                .returns(Types.TUPLE(Types.LONG, Types.INT, Types.INT))
                .setParallelism(1)
                // https://stackoverflow.com/questions/70096166/parallelism-in-flink-kafka-source-causes-nothing-to-execute/70101290#70101290
                // https://stackoverflow.com/questions/69765972/migrating-from-flinkkafkaconsumer-to-kafkasource-no-windows-executed
                .assignTimestampsAndWatermarks(new BDSWatermarkStrategy())
                // filter out the "watermark" event from source
                // because from now on, Flink will generate watermarks
                .filter((Tuple3<Long, Integer, Integer> e) -> e.f1 != -1 || e.f2 != -1);
	}
	
	static SingleOutputStreamOperator<Tuple3<Long, Integer, Integer>> GetStreamFromLocalFile(String dataDirectory, String topic) throws Exception
	{
		var path = dataDirectory + topic + ".txt";
		
		var source = FileSource
				.forRecordStreamFormat(new TextLineInputFormat(), new Path(path))
				.build();
		
		return env
				.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
				// https://stackoverflow.com/questions/41265266/how-to-solve-inaccessibleobjectexception-unable-to-make-member-accessible-m
				.map((String str) -> EventParser.ParseStringEvent(str))
	            // you need to specify which type is returned from the "map" operator
		        // https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/java_lambdas/
                .returns(Types.TUPLE(Types.LONG, Types.INT, Types.INT))
                .setParallelism(1)
                // https://stackoverflow.com/questions/70096166/parallelism-in-flink-kafka-source-causes-nothing-to-execute/70101290#70101290
                // https://stackoverflow.com/questions/69765972/migrating-from-flinkkafkaconsumer-to-kafkasource-no-windows-executed
                .assignTimestampsAndWatermarks(new BDSWatermarkStrategy().withTimestampAssigner((event, timestamp) -> event.f0))
                // filter out the "watermark" event from source
                // because from now on, Flink will generate watermarks
                .filter((Tuple3<Long, Integer, Integer> e) -> e.f1 != -1 || e.f2 != -1);
	}
	
	static void CheckCorrectness(String dataDirectory) throws IOException
	{
		// STEP 1: read the expected result
		var expected = new ArrayList<Tuple2<Integer, Integer>>();
		var file = new File(dataDirectory + "../expected_result.txt");
		var reader = new BufferedReader(new FileReader(file));
		var line = reader.readLine();
		while (line != null)
		{
			var strs = line.split(" ");
			expected.add(Tuple2.of(Integer.parseInt(strs[0]), Integer.parseInt(strs[1])));
			line = reader.readLine();
		}
		expected.sort(new EventComparator());
		
		// STEP 2: read the actual result
		var actual = new ArrayList<Tuple2<Integer, Integer>>();
		var paths = Files
				// retrieve all files in the FlinkResult folder
				.walk(Paths.get(dataDirectory + "FlinkResult/"))
				// find the text file that contains the query result
				.filter(path -> path.toString().contains("FlinkResult-"));
		
		var resultPath = paths.findFirst().get().toString();
		file = new File(resultPath);
		reader = new BufferedReader(new FileReader(file));
		line = reader.readLine();
		while (line != null)
		{
			var strs = line.split(" ");
			actual.add(Tuple2.of(Integer.parseInt(strs[0]), Integer.parseInt(strs[1])));
			line = reader.readLine();
		}
		actual.sort(new EventComparator());
		
		// STEP 3: check if all records in the actual result are also in the expected result
		if (actual.size() != expected.size()) 
        {
            System.out.println("The query result is wrong, expected_num_output = " + expected.size() + ", actual_num_output = " + actual.size());
            return;
        }
        
        for (var i = 0; i < actual.size(); i++) 
        {
            var isEqual = actual.get(i).f0.equals(expected.get(i).f0) && actual.get(i).f1.equals(expected.get(i).f1);
            if (!isEqual)
            {
                System.out.println("The query result is wrong, expected: <" + expected.get(i).f0 + ", " + expected.get(i).f1 + ">, actual: <" + actual.get(i).f0 + ", " + actual.get(i).f1 + ">");
                return;
            }
        }
        
        System.out.println("The query result is corret. ");
	}
}