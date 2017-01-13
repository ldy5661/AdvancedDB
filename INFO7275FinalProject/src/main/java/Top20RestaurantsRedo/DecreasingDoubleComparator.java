package Top20RestaurantsRedo;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by dongyueli on 12/10/16.
 */
public class DecreasingDoubleComparator {

    public static class MyDecreasingDoubleComparator extends WritableComparator {
        public MyDecreasingDoubleComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            double thisValue = readDouble(b1, s1);
            double thatValue = readDouble(b2, s2);
            return (-1) * (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }
}
