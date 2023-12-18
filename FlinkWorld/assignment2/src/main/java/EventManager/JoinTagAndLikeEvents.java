package EventManager;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class JoinTagAndLikeEvents implements JoinFunction<Tuple3<Long, Integer, Integer>, Tuple3<Long, Integer, Integer>, Tuple4<Long, Long, Integer, Integer>>
{
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple4<Long, Long, Integer, Integer> join(Tuple3<Long, Integer, Integer> tagInfo, Tuple3<Long, Integer, Integer> likeInfo) throws Exception
	{
		return Tuple4.of(tagInfo.f0, likeInfo.f0, tagInfo.f1, tagInfo.f2);
	}
}