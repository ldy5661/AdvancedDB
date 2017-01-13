package SecondarySortBusiness;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by dongyueli on 12/11/16.
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKeyWritable, Text> {
    public int getPartition(CompositeKeyWritable key, Text value, int numReduceTasks) {
        double avgStars = key.getAvgStars();
        if (numReduceTasks == 0) {
            return 0;
        }
        if (avgStars < 1.5) {
            return 0;
        } else if (avgStars >=1.5 && avgStars < 2.0) {
            return 1 % numReduceTasks;
        } else if (avgStars >=2.0 && avgStars < 2.5) {
            return 2 % numReduceTasks;
        } else if (avgStars >=2.5 && avgStars < 3.0) {
            return 3 % numReduceTasks;
        } else if (avgStars >=3.0 && avgStars < 3.5) {
            return 4 % numReduceTasks;
        } else if (avgStars >=3.5 && avgStars < 4.0) {
            return 5 % numReduceTasks;
        } else if (avgStars >=4.0 && avgStars < 4.5) {
            return 6 % numReduceTasks;
        } else if (avgStars >=4.5 && avgStars < 5.0) {
            return 7 % numReduceTasks;
        } else {
            return 8 % numReduceTasks;
        }
    }
}
