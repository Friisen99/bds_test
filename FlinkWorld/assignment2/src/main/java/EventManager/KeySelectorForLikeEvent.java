package EventManager;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class KeySelectorForLikeEvent implements KeySelector<Tuple3<Long, Integer, Integer>, Tuple2<Integer, Integer>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Integer, Integer> getKey(Tuple3<Long, Integer, Integer> likeInfo) throws Exception
	{
		return Tuple2.of(likeInfo.f2, likeInfo.f1);
	}
}