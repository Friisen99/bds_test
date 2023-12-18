package EventManager;

import java.util.Comparator;

import org.apache.flink.api.java.tuple.Tuple2;

public class EventComparator implements Comparator<Tuple2<Integer, Integer>>
{
	@Override
	public int compare(Tuple2<Integer, Integer> e1, Tuple2<Integer, Integer> e2) 
	{
		if (e1.f0 < e2.f0) return -1;
		else if (e1.f0 > e2.f0) return 1;
		else
		{
			if (e1.f1 < e2.f1) return -1;
			else if (e1.f1 > e2.f1) return 1;
			else return 0;
		}
	}

}