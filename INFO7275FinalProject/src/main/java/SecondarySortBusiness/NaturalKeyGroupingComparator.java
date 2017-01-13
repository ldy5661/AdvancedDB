package SecondarySortBusiness;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by dongyueli on 12/11/16.
 */
public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
        return Double.compare(key1.getAvgStars(), key2.getAvgStars());
    }

}
