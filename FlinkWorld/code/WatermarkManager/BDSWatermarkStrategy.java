package WatermarkManager;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;

public class BDSWatermarkStrategy implements WatermarkStrategy<Tuple3<Long, Integer, Integer>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public WatermarkGenerator<Tuple3<Long, Integer, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) 
	{
		return new BDSWatermarkGenerator();
	}
}