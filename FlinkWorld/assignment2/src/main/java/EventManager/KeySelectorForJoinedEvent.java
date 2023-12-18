package EventManager;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeySelectorForJoinedEvent implements KeySelector<Tuple2<Integer, Integer>, Integer>
{
	private static final long serialVersionUID = 1L;

	@Override
	public Integer getKey(Tuple2<Integer, Integer> joinedInfo) throws Exception
	{
		return joinedInfo.f0;
	}
}