package JoinBusinessTip;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by dongyueli on 12/4/16.
 */
public class BusinessTipReduceSideJoin {

    public static class BusinessMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if (jsonObj.getString("type").equals("business")) {
                    outKey.set(jsonObj.getString("business_id"));
                    outValue.set("A" + line);
                    context.write(outKey, outValue);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    public static class TipMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if (jsonObj.getString("type").equals("tip")) {
                    outKey.set(jsonObj.getString("business_id"));
                    outValue.set("B" + line);
                    context.write(outKey, outValue);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }


    public static class BusinessTipReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        private String joinType = null;

        public void setup(Context context) throws IOException, InterruptedException {
            joinType = context.getConfiguration().get("join.type");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            listA.clear();
            listB.clear();

            for (Text tmp : values) {
                if (tmp.charAt(0) == 'A') {
                    listA.add(new Text(tmp.toString().substring(1)));
                } else if (tmp.charAt(0) == 'B') {
                    listB.add(new Text(tmp.toString().substring(1)));
                }
            }

            try {
                executeJoinLogic(context);
            } catch (JSONException e) {
                e.printStackTrace();
            }

        }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException, JSONException {
            if (joinType.equalsIgnoreCase("inner")) {
                if (!listA.isEmpty() && !listB.isEmpty()) {
                    for (Text A : listA) {
                        for (Text B : listB) {
                            JSONObject jsonObjA = new JSONObject(A.toString());
                            JSONObject jsonObjB = new JSONObject(B.toString());
                            Put put = new Put(Bytes.toBytes(jsonObjA.getString("business_id")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("name"), Bytes.toBytes(jsonObjA.getString("name")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("city"), Bytes.toBytes(jsonObjA.getString("city")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("state"), Bytes.toBytes(jsonObjA.getString("state")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("stars"), Bytes.toBytes("" + jsonObjA.getDouble("stars")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("review_count"), Bytes.toBytes("" + jsonObjA.getInt("review_count")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("text"), Bytes.toBytes(jsonObjB.getString("text")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("date"), Bytes.toBytes(jsonObjB.getString("date")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("likes"), Bytes.toBytes("" + jsonObjB.getInt("likes")));
                            context.write(new ImmutableBytesWritable(Bytes.toBytes(jsonObjA.getString("business_id"))), put);
                        }
                    }
                }
            } else if (joinType.equalsIgnoreCase("leftouter")) {
                for (Text A : listA) {
                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            JSONObject jsonObjA = new JSONObject(A.toString());
                            JSONObject jsonObjB = new JSONObject(B.toString());
                            Put put = new Put(Bytes.toBytes(jsonObjA.getString("business_id")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("name"), Bytes.toBytes(jsonObjA.getString("name")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("city"), Bytes.toBytes(jsonObjA.getString("city")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("state"), Bytes.toBytes(jsonObjA.getString("state")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("stars"), Bytes.toBytes("" + jsonObjA.getDouble("stars")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("review_count"), Bytes.toBytes("" + jsonObjA.getInt("review_count")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("text"), Bytes.toBytes(jsonObjB.getString("text")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("date"), Bytes.toBytes(jsonObjB.getString("date")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("likes"), Bytes.toBytes("" + jsonObjB.getInt("likes")));
                            context.write(new ImmutableBytesWritable(Bytes.toBytes(jsonObjA.getString("business_id"))), put);
                        }
                    } else {
                        JSONObject jsonObjA = new JSONObject(A.toString());
                        Put put = new Put(Bytes.toBytes(jsonObjA.getString("business_id")));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("name"), Bytes.toBytes(jsonObjA.getString("name")));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("city"), Bytes.toBytes(jsonObjA.getString("city")));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("state"), Bytes.toBytes(jsonObjA.getString("state")));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("stars"), Bytes.toBytes("" + jsonObjA.getDouble("stars")));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("review_count"), Bytes.toBytes("" + jsonObjA.getInt("review_count")));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("text"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("date"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("likes"), Bytes.toBytes(""));
                        context.write(new ImmutableBytesWritable(Bytes.toBytes(jsonObjA.getString("business_id"))), put);

                    }
                }
            } else if (joinType.equalsIgnoreCase("rightouter")) {
                for (Text B : listB) {
                    if (!listA.isEmpty()) {
                        for (Text A : listA) {
                            JSONObject jsonObjA = new JSONObject(A.toString());
                            JSONObject jsonObjB = new JSONObject(B.toString());
                            Put put = new Put(Bytes.toBytes(jsonObjA.getString("business_id")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("name"), Bytes.toBytes(jsonObjA.getString("name")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("city"), Bytes.toBytes(jsonObjA.getString("city")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("state"), Bytes.toBytes(jsonObjA.getString("state")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("stars"), Bytes.toBytes("" + jsonObjA.getDouble("stars")));
                            put.add(Bytes.toBytes("Business"), Bytes.toBytes("review_count"), Bytes.toBytes("" + jsonObjA.getInt("review_count")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("text"), Bytes.toBytes(jsonObjB.getString("text")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("date"), Bytes.toBytes(jsonObjB.getString("date")));
                            put.add(Bytes.toBytes("Tip"), Bytes.toBytes("likes"), Bytes.toBytes("" + jsonObjB.getInt("likes")));
                            context.write(new ImmutableBytesWritable(Bytes.toBytes(jsonObjA.getString("business_id"))), put);
                        }
                    } else {
                        JSONObject jsonObjB = new JSONObject(B.toString());
                        Put put = new Put(Bytes.toBytes(jsonObjB.getString("business_id")));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("name"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("city"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("state"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("stars"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("review_count"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("text"), Bytes.toBytes(jsonObjB.getString("text")));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("date"), Bytes.toBytes(jsonObjB.getString("date")));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("likes"), Bytes.toBytes("" + jsonObjB.getInt("likes")));
                        context.write(new ImmutableBytesWritable(Bytes.toBytes(jsonObjB.getString("business_id"))), put);
                    }
                }
            } else if (joinType.equalsIgnoreCase("fullouter")) {
                if (!listA.isEmpty()) {
                    for (Text A : listA) {
                        if (!listB.isEmpty()) {
                            for (Text B : listB) {
                                JSONObject jsonObjA = new JSONObject(A.toString());
                                JSONObject jsonObjB = new JSONObject(B.toString());
                                Put put = new Put(Bytes.toBytes(jsonObjA.getString("business_id")));
                                put.add(Bytes.toBytes("Business"), Bytes.toBytes("name"), Bytes.toBytes(jsonObjA.getString("name")));
                                put.add(Bytes.toBytes("Business"), Bytes.toBytes("city"), Bytes.toBytes(jsonObjA.getString("city")));
                                put.add(Bytes.toBytes("Business"), Bytes.toBytes("state"), Bytes.toBytes(jsonObjA.getString("state")));
                                put.add(Bytes.toBytes("Business"), Bytes.toBytes("stars"), Bytes.toBytes("" + jsonObjA.getDouble("stars")));
                                put.add(Bytes.toBytes("Business"), Bytes.toBytes("review_count"), Bytes.toBytes("" + jsonObjA.getInt("review_count")));
                                put.add(Bytes.toBytes("Tip"), Bytes.toBytes("text"), Bytes.toBytes(jsonObjB.getString("text")));
                                put.add(Bytes.toBytes("Tip"), Bytes.toBytes("date"), Bytes.toBytes(jsonObjB.getString("date")));
                                put.add(Bytes.toBytes("Tip"), Bytes.toBytes("likes"), Bytes.toBytes("" + jsonObjB.getInt("likes")));
                                context.write(new ImmutableBytesWritable(Bytes.toBytes(jsonObjA.getString("business_id"))), put);
                            }
                        }
                    }
                } else {
                    for (Text B : listB) {
                        JSONObject jsonObjB = new JSONObject(B.toString());
                        Put put = new Put(Bytes.toBytes(jsonObjB.getString("business_id")));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("name"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("city"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("state"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("stars"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Business"), Bytes.toBytes("review_count"), Bytes.toBytes(""));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("text"), Bytes.toBytes(jsonObjB.getString("text")));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("date"), Bytes.toBytes(jsonObjB.getString("date")));
                        put.add(Bytes.toBytes("Tip"), Bytes.toBytes("likes"), Bytes.toBytes("" + jsonObjB.getInt("likes")));
                        context.write(new ImmutableBytesWritable(Bytes.toBytes(jsonObjB.getString("business_id"))), put);
                    }
                }
            }

        }

    }
}