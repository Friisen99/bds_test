package WatermarkManager;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class BDSWatermarkGenerator implements WatermarkGenerator<Tuple3<Long, Integer, Integer>> 
{
	@Override
	public void onEvent(Tuple3<Long, Integer, Integer> event, long eventTimestamp, WatermarkOutput output) 
	{
		if (event.f1 == -1 && event.f2 == -1) 
		{
			output.emitWatermark(new Watermark(eventTimestamp));
		}
	}

	@Override
	public void onPeriodicEmit(WatermarkOutput output) 
	{
		// TODO Auto-generated method stub
	}
}