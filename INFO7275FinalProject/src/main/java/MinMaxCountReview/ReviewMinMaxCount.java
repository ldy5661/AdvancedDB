package MinMaxCountReview;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by dongyueli on 12/4/16.
 */
public class ReviewMinMaxCount {

    private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");

    public static class ReviewMinMaxCountMapper extends Mapper<Object, Text, Text, ReviewMinMaxCountTuple> {
        private Text outUserId = new Text();
        private ReviewMinMaxCountTuple outTuple = new ReviewMinMaxCountTuple();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if(jsonObj.getString("type").equals("review")) {
                    String userId = jsonObj.getString("user_id");
                    String strDate = jsonObj.getString("date");
                    Date creationDate = frmt.parse(strDate);

                    outUserId.set(userId);
                    outTuple.setMin(creationDate);
                    outTuple.setMax(creationDate);
                    outTuple.setCount(1);

                    context.write(outUserId, outTuple);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }

    public static class ReviewMinMaxCountReducer extends TableReducer <Text, ReviewMinMaxCountTuple, ImmutableBytesWritable> {
//    public static class ReviewMinMaxCountReducer extends Reducer<Text, ReviewMinMaxCountTuple, Text, ReviewMinMaxCountTuple> {
    private ReviewMinMaxCountTuple result = new ReviewMinMaxCountTuple();

        public void reduce(Text key, Iterable<ReviewMinMaxCountTuple> values, Context context)
            throws IOException, InterruptedException{
            result.setMin(null);
            result.setMax(null);
            result.setCount(0);
            int sum = 0;

            for(ReviewMinMaxCountTuple val : values) {
                if(result.getMin() == null || val.getMin().compareTo(result.getMin()) < 0) {
                    result.setMin(val.getMin());
                }

                if(result.getMax() == null || val.getMax().compareTo(result.getMax()) > 0) {
                    result.setMax(val.getMax());
                }

                sum += val.getCount();
            }

            result.setCount(sum);

            Put put = new Put(Bytes.toBytes(String.valueOf(key)));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("first time reviewed"), Bytes.toBytes(frmt.format(result.getMin())));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("last time reviewed"), Bytes.toBytes(frmt.format(result.getMax())));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("total number of reviews"), Bytes.toBytes(String.valueOf(result.getCount())));

            context.write(null, put);
//            context.write(key, result);
        }
    }

    public static class ReviewMinMaxCountTuple implements Writable {
        private Date min = new Date();
        private Date max = new Date();
        private long count = 0;

        public Date getMin() {
            return min;
        }

        public void setMin(Date min) {
            this.min = min;
        }

        public Date getMax() {
            return max;
        }

        public void setMax(Date max) {
            this.max = max;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public void readFields(DataInput in) throws IOException {
            min = new Date(in.readLong());
            max = new Date(in.readLong());
            count = in.readLong();
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(min.getTime());
            out.writeLong(max.getTime());
            out.writeLong(count);
        }

        public String toString(){
            return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
        }
    }

}
