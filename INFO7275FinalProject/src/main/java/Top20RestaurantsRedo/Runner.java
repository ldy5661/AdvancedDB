//package Top20RestaurantsRedo;
//
//import Top20Restaurants.Top20AZRestaurantsPrep;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import static org.apache.hadoop.hbase.util.Bytes.toBytes;
//
///**
// * Created by dongyueli on 11/21/16.
// */
//public class Runner {
//    public static void main(String[] args) throws Exception {
//
//        Configuration conf = HBaseConfiguration.create();
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        //creating table descriptor
//        HTableDescriptor table = new HTableDescriptor(toBytes("Top20AZRestaurantsTableRedo"));
//        //creating column family descriptor
//        HColumnDescriptor family = new HColumnDescriptor(toBytes("cf"));
//        //adding coloumn family to HTable
//        table.addFamily(family);
//        admin.createTable(table);
//
//        Configuration conf1 = new Configuration();
//        Job job1 = Job.getInstance(conf1, "Top20AZRestaurantsPrep");
//        job1.setJarByClass(Runner.class);
//        job1.setMapperClass(Top20AZRestaurantsPrep.Top20Mapper.class);
//        job1.setOutputKeyClass(NullWritable.class);
//        job1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
//
//        job1.waitForCompletion(true);
//
//        JobConf conf2 = new JobConf(Runner.class);
//        conf2.setOutputKeyComparatorClass(DecreasingDoubleComparator.MyDecreasingDoubleComparator.class);
//        Job job2 = Job.getInstance(conf2, "Top20AZRestaurantsRedo");
//        job2.setJarByClass(Runner.class);
//        job2.setMapperClass(Top20RestaurantsRedo.Top20MapperRedo.class);
//        job2.setReducerClass(Top20RestaurantsRedo.Top20ReducerRedo.class);
//        job2.setMapOutputKeyClass(DoubleWritable.class);
//        job2.setMapOutputValueClass(Text.class);
//
//        FileInputFormat.addInputPath(job2, new Path(args[1]));
//
//        TableMapReduceUtil.initTableReducerJob("Top20AZRestaurantsTableRedo", Top20RestaurantsRedo.Top20ReducerRedo.class, job2);
//
//        System.exit(job2.waitForCompletion(true) ? 0 : 1);
//    }
//}
