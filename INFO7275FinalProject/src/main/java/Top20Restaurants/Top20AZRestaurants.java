package Top20Restaurants;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by dongyueli on 12/10/16.
 */
public class Top20AZRestaurants {
    public static class Top20Mapper extends Mapper<Object, Text, NullWritable, Text> {
        public static TreeMap<Stars, Text> starsToBusinessNameMap = new TreeMap<Stars, Text>(new MyStarsComp1());

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("%%%");
            Double instar = Double.parseDouble(splits[3]);

            starsToBusinessNameMap.put(new Stars(instar), new Text(value));
            Iterator<Map.Entry<Stars, Text> > iter = starsToBusinessNameMap.entrySet().iterator();
            Map.Entry<Stars, Text> entry = null;
            while (starsToBusinessNameMap.size() > 20) {
                entry = iter.next();
                iter.remove();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text t : starsToBusinessNameMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class Top20Reducer extends TableReducer<NullWritable, Text, ImmutableBytesWritable> {
        public static TreeMap<Stars, Text> starsToBusinessNameMap = new TreeMap<Stars, Text>(new MyStarsComp1());
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                String[] splits = value.toString().split("%%%");
                Double instar = Double.parseDouble(splits[3]);
                starsToBusinessNameMap.put(new Stars(instar), new Text(value));
                if (starsToBusinessNameMap.size() > 20) {
                    starsToBusinessNameMap.remove(starsToBusinessNameMap.firstKey());
                }
            }

            Iterator<Map.Entry<Stars, Text>> iter = starsToBusinessNameMap.entrySet().iterator();
            Map.Entry<Stars, Text> entry = null;
            while(starsToBusinessNameMap.size() > 20){
                entry = iter.next();
                iter.remove();

            }

            for (Text t : starsToBusinessNameMap.descendingMap().values()) {
                String[] out = t.toString().split("%%%");
                String outKey = out[0];
                String outName = out[1];
                String outAddress = out[2];
                String outRating = out[3];
                Put put = new Put(Bytes.toBytes(outKey));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("Stars"), Bytes.toBytes(outRating));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("Business Name"), Bytes.toBytes(outName));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("Full Address"), Bytes.toBytes(outAddress));
                context.write(null, put);
            }
        }
    }
}
