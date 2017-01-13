package SecondarySortBusiness;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by dongyueli on 12/11/16.
 */
public class CompositeSortComparator extends WritableComparator {

    protected CompositeSortComparator() {
        super(CompositeKeyWritable.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

        int cmpResult = (-1) * Double.compare(key1.getAvgStars(), key2.getAvgStars());
        if (cmpResult == 0)// same Category
        {
            return (-1) * Integer.compare(key1.getCount(), key2.getCount());
        }
        return cmpResult;
    }
}
