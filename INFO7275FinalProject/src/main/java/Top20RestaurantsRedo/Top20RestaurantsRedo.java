package Top20RestaurantsRedo;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by dongyueli on 12/10/16.
 */
public class Top20RestaurantsRedo {
    public static class Top20MapperRedo extends Mapper<Object, Text, DoubleWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("%%%");
            double instar = Double.parseDouble(splits[3]);
            context.write(new DoubleWritable(instar), value);
        }
    }

    public static class Top20ReducerRedo extends TableReducer<DoubleWritable, Text, ImmutableBytesWritable> {
        static int counter = 0;

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                if (counter < 20) {
                    String[] out = val.toString().split("%%%");
                    String outKey = out[0];
                    String outName = out[1];
                    String outAddress = out[2];
                    String outRating = out[3];
                    Put put = new Put(Bytes.toBytes(outKey));
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("Stars"), Bytes.toBytes(outRating));
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("Business Name"), Bytes.toBytes(outName));
                    put.add(Bytes.toBytes("cf"), Bytes.toBytes("Full Address"), Bytes.toBytes(outAddress));
                    context.write(null, put);
                    counter++;
                }
            }
        }
    }
}
