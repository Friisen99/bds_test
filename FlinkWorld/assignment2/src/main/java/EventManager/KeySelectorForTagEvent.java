package EventManager;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class KeySelectorForTagEvent implements KeySelector<Tuple3<Long, Integer, Integer>, Tuple2<Integer, Integer>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Integer, Integer> getKey(Tuple3<Long, Integer, Integer> tagInfo) throws Exception
	{
		return Tuple2.of(tagInfo.f1, tagInfo.f2);
	}
}