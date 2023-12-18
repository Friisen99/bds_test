package EventManager;

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple3;
import org.msgpack.core.MessagePack;

/**
 * Important:
 * (1) Since the input data are serialized by MessagePack serializer in .net world,
 * we must also use the MessagePack deserializer to deserialize data in java world.
 * So, if you change the data sent from Orleans, remember to make changes here correspondingly. 
 * reference: https://msgpack.org/index.html
 * reference: https://github.com/msgpack/msgpack/blob/master/spec.md
 * reference: https://github.com/msgpack/msgpack-java/blob/develop/msgpack-core/src/test/java/org/msgpack/core/example/MessagePackExample.java
 *
 * (2) Here we use flink's built-in tuples (eg. Tuple2, Tuple3, Tuple4...) as event type
   If you create your own event type (eg. MyEvent), there will be nasty bugs to fix -_-
 *  
 * (3) You can also use String as event type, it is maybe more generic
 */
public class EventParser 
{
    static enum EventType {Regular, Watermark};    // must keep it the same as in the C# program
	
	public static Tuple3<Long, Integer, Integer> ParseEvent(byte[] bytes) throws IOException
	{
    	var unpacker = MessagePack.newDefaultUnpacker(bytes);
    	unpacker.unpackArrayHeader();               // the number of elements stored in the following bytes
    	var timestamp = unpacker.unpackLong();      // 1st must be the timestamp
    	var type = unpacker.unpackInt();
    	var content = unpacker.unpackValue().asBinaryValue().asByteArray();
    	
    	var data1 = -1;
    	var data2 = -1;
    	if (type == 0)
    	{
    		var content_unpacker = MessagePack.newDefaultUnpacker(content);
        	content_unpacker.unpackArrayHeader();
    		data1 = content_unpacker.unpackInt();
    		data2 = content_unpacker.unpackInt();
    		content_unpacker.close();
    		//System.out.println("Receive event: timestamp = " + timestamp + ", event type = " + type + ", data1 = " + data1 + ", data2 = " + data2);
    	}
    	unpacker.close();
    	
    	return Tuple3.of(timestamp, data1, data2);
	}
	
	public static Tuple3<Long, Integer, Integer> ParseStringEvent(String str) throws IOException
	{
		var strs = str.split(" ");
		var timestamp = Long.parseLong(strs[0]);
		if (strs.length == 1) return Tuple3.of(timestamp, -1, -1);
		
		var data1 = Integer.parseInt(strs[1]);
		var data2 = Integer.parseInt(strs[2]);
		return Tuple3.of(timestamp, data1, data2);
	}
}
