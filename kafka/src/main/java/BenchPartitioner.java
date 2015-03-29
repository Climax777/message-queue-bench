import kafka.producer.Partitioner;

/**
 * Created by climax on 2015/03/29.
 */
public class BenchPartitioner implements Partitioner {
    @Override
    public int partition(Object o, int i) {
        return 0;
    }
}
