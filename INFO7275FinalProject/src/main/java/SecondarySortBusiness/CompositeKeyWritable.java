package SecondarySortBusiness;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by dongyueli on 12/11/16.
 */
public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {
    private double avgStars;
    private int count;

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(double avgStars, int count) {
        this.avgStars = avgStars;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getAvgStars() {
        return avgStars;
    }

    public void setAvgStars(double avgStars) {
        this.avgStars = avgStars;
    }

    public void readFields(DataInput dataInput) throws IOException {
        avgStars = dataInput.readDouble();
        count = dataInput.readInt();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(avgStars);
        dataOutput.writeInt(count);
    }

    public int compareTo(CompositeKeyWritable objKeyPair) {
        int result = Double.compare(avgStars, objKeyPair.avgStars);
        if (0 == result) {
            result = Integer.compare(count, objKeyPair.count);
        }
        return result;
    }

    public String toString() {
        return avgStars + "\t" + count;
    }
}
