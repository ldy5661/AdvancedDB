import BinningBusinessCategories.BusinessCategoriesBinning;
import DistinctUser.CountUsersMapper;
import DistinctUser.CountUsersReducer;
import JoinBusinessTip.BusinessTipReduceSideJoin;
import MinMaxCountReview.ReviewMinMaxCount;
import SecondarySortBusiness.*;
import Top20Restaurants.Top20AZRestaurants;
import Top20Restaurants.Top20AZRestaurantsPrep;
import Top20RestaurantsRedo.DecreasingDoubleComparator;
import Top20RestaurantsRedo.Top20RestaurantsRedo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 * Created by dongyueli on 12/11/16.
 */
public class Driver {

    private static final String alldata = "/INFO7275FinalProject/input";
    private static final String business = "/INFO7275FinalProject/input/yelp_academic_dataset_business.json";
    private static final String checkin = "/INFO7275FinalProject/input/yelp_academic_dataset_checkin.json";
    private static final String review = "/INFO7275FinalProject/input/yelp_academic_dataset_review.json";
    private static final String tip = "/INFO7275FinalProject/input/yelp_academic_dataset_tip.json";
    private static final String user = "/INFO7275FinalProject/input/yelp_academic_dataset_user.json";
    private static final String topOutput = "/INFO7275FinalProject/topOutput";
    private static final String topRedoOutput = "/INFO7275FinalProject/topRedoOutput";
    private static final String leftOuterJoin = "leftouter";
    private static final String binningOutput = "/INFO7275FinalProject/binningOutput";
    private static final String categoryCountAvgStarsOutput = "/INFO7275FinalProject/categoryCountAvgStarsOutput";
    private static final String secondarySortCategoryCountAvgStarsOutput = "/INFO7275FinalProject/secondarySortCategoryCountAvgStarsOutput";
    private static final String distinctUserOutput = "/INFO7275FinalProject/distinctUserOutput";


    public static void main(String[] args) throws Exception {

        ////####################1.ReviewMinMaxCount##############################
        Configuration conf1 = HBaseConfiguration.create();
        HBaseAdmin admin1 = new HBaseAdmin(conf1);
        HTableDescriptor table1 = new HTableDescriptor(toBytes("ReviewMinMaxCountTable"));
        HColumnDescriptor family1 = new HColumnDescriptor(toBytes("cf"));
        table1.addFamily(family1);
        admin1.createTable(table1);

        Job job1 = Job.getInstance(conf1, "ReviewMinMaxCount");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(ReviewMinMaxCount.ReviewMinMaxCountMapper.class);
        job1.setReducerClass(ReviewMinMaxCount.ReviewMinMaxCountReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(ReviewMinMaxCount.ReviewMinMaxCountTuple.class);
        FileInputFormat.addInputPath(job1, new Path(review));
        TableMapReduceUtil.initTableReducerJob("ReviewMinMaxCountTable", ReviewMinMaxCount.ReviewMinMaxCountReducer.class, job1);
        job1.waitForCompletion(true);


        ////####################2.Top20Restaurants#################################
        final long startTime2 = System.currentTimeMillis();

        Configuration conf22 = HBaseConfiguration.create();
        HBaseAdmin admin2 = new HBaseAdmin(conf22);
        HTableDescriptor table2 = new HTableDescriptor(toBytes("Top20AZRestaurantsTable"));
        HColumnDescriptor family2 = new HColumnDescriptor(toBytes("cf"));
        table2.addFamily(family2);
        admin2.createTable(table2);

        Configuration conf21 = new Configuration();
        Job job21 = Job.getInstance(conf21, "Top20AZRestaurantsPrep");
        job21.setJarByClass(Driver.class);
        job21.setMapperClass(Top20AZRestaurantsPrep.Top20Mapper.class);
        job21.setOutputKeyClass(NullWritable.class);
        job21.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job21, new Path(business));
        FileOutputFormat.setOutputPath(job21, new Path(topOutput));

        job21.waitForCompletion(true);

        Job job22 = Job.getInstance(conf22, "Top20Restaurants");
        job22.setJarByClass(Driver.class);
        job22.setMapperClass(Top20AZRestaurants.Top20Mapper.class);
        job22.setReducerClass(Top20AZRestaurants.Top20Reducer.class);
        job22.setNumReduceTasks(1);
        job22.setMapOutputKeyClass(NullWritable.class);
        job22.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job22, new Path(topOutput));

        TableMapReduceUtil.initTableReducerJob("Top20AZRestaurantsTable", Top20AZRestaurants.Top20Reducer.class, job22);

        job22.waitForCompletion(true);

        long totalTime2 = System.currentTimeMillis() - startTime2;
        System.out.println("Top20Restaurants Running time: " + totalTime2);

        ////####################3.Top20RestaurantsRedo#################################
        final long startTime3 = System.currentTimeMillis();

        Configuration conf33 = HBaseConfiguration.create();
        HBaseAdmin admin3 = new HBaseAdmin(conf33);
        HTableDescriptor table3 = new HTableDescriptor(toBytes("Top20AZRestaurantsTableRedo"));
        HColumnDescriptor family3 = new HColumnDescriptor(toBytes("cf"));
        table3.addFamily(family3);
        admin3.createTable(table3);

        Configuration conf31 = new Configuration();
        Job job31 = Job.getInstance(conf31, "Top20AZRestaurantsPrep");
        job31.setJarByClass(Driver.class);
        job31.setMapperClass(Top20AZRestaurantsPrep.Top20Mapper.class);
        job31.setOutputKeyClass(NullWritable.class);
        job31.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job31, new Path(business));
        FileOutputFormat.setOutputPath(job31, new Path(topRedoOutput));

        job31.waitForCompletion(true);

        JobConf conf32 = new JobConf(Driver.class);
        conf32.setOutputKeyComparatorClass(DecreasingDoubleComparator.MyDecreasingDoubleComparator.class);
        Job job32 = Job.getInstance(conf32, "Top20AZRestaurantsRedo");
        job32.setJarByClass(Driver.class);
        job32.setMapperClass(Top20RestaurantsRedo.Top20MapperRedo.class);
        job32.setReducerClass(Top20RestaurantsRedo.Top20ReducerRedo.class);
        job32.setMapOutputKeyClass(DoubleWritable.class);
        job32.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job32, new Path(topRedoOutput));
        TableMapReduceUtil.initTableReducerJob("Top20AZRestaurantsTableRedo", Top20RestaurantsRedo.Top20ReducerRedo.class, job32);

        job32.waitForCompletion(true);

        long totalTime3 = System.currentTimeMillis() - startTime3;
        System.out.println("Top20RestaurantsRedo Running time: " + totalTime3);

        ////####################4.JoinBusinessTip#################################

        Configuration conf4 = HBaseConfiguration.create();
        HBaseAdmin admin4 = new HBaseAdmin(conf4);
        HTableDescriptor table4 = new HTableDescriptor(toBytes("BusinessTipReduceSideJoinTable"));
        HColumnDescriptor family41 = new HColumnDescriptor(toBytes("Business"));
        HColumnDescriptor family42 = new HColumnDescriptor(toBytes("Tip"));
        table4.addFamily(family41);
        table4.addFamily(family42);
        admin4.createTable(table4);

        Job job4 = Job.getInstance(conf4, "BusinessTipReduceSideJoin");
        job4.setJarByClass(Driver.class);
        job4.setReducerClass(BusinessTipReduceSideJoin.BusinessTipReducer.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job4, new Path(business), TextInputFormat.class, BusinessTipReduceSideJoin.BusinessMapper.class);
        MultipleInputs.addInputPath(job4, new Path(tip), TextInputFormat.class, BusinessTipReduceSideJoin.TipMapper.class);

        TableMapReduceUtil.initTableReducerJob("BusinessTipReduceSideJoinTable", BusinessTipReduceSideJoin.BusinessTipReducer.class, job4);
        job4.getConfiguration().set("join.type", leftOuterJoin);

        job4.waitForCompletion(true);


        ////####################5.BinningBusinessCategories#################################

        Configuration conf5 = new Configuration();
        Job job5 = Job.getInstance(conf5, "Binning Bussiness Categories");
        job5.setJarByClass(Driver.class);
        job5.setMapperClass(BusinessCategoriesBinning.BinningMapper.class);
        job5.setNumReduceTasks(0);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(NullWritable.class);

        MultipleOutputs.addNamedOutput(job5, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job5, true);
        FileInputFormat.addInputPath(job5, new Path(business));
        FileOutputFormat.setOutputPath(job5, new Path(binningOutput));

        job5.waitForCompletion(true);


        ////####################6.SecondarySort#################################

        Configuration conf6 = new Configuration();
        Job job6 = Job.getInstance(conf6, "Categories Count Star Average");
        job6.setJarByClass(Driver.class);
        job6.setMapperClass(CategoriesCountAndStarAvg.CategoriesMapper.class);
        job6.setReducerClass(CategoriesCountAndStarAvg.CategoriesReducer.class);
        job6.setCombinerClass(CategoriesCountAndStarAvg.CategoriesReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(CategoriesCountAndStarAvg.CountAvgStarsTuple.class);

        FileInputFormat.addInputPath(job6, new Path(business));
        FileOutputFormat.setOutputPath(job6, new Path(categoryCountAvgStarsOutput));

        job6.waitForCompletion(true);

        Configuration conf62 = new Configuration();
        Job job62 = Job.getInstance(conf62, "SS Categories Count Star Average");
        job62.setJarByClass(Driver.class);
        job62.setMapperClass(SecondarySort.SsMapper.class);
        job62.setReducerClass(SecondarySort.SsReducer.class);
        job62.setPartitionerClass(NaturalKeyPartitioner.class);
        job62.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job62.setSortComparatorClass(CompositeSortComparator.class);
        job62.setMapOutputKeyClass(CompositeKeyWritable.class);
        job62.setMapOutputValueClass(Text.class);
        job62.setNumReduceTasks(9);

        FileInputFormat.addInputPath(job62, new Path(categoryCountAvgStarsOutput));
        FileOutputFormat.setOutputPath(job62, new Path(secondarySortCategoryCountAvgStarsOutput));

        job62.waitForCompletion(true);

        ////####################7.DistinctUser#################################

        Configuration conf7 = new Configuration();
        Job job7 = Job.getInstance(conf7, "Count Users");
        job7.setJarByClass(Driver.class);
        job7.setMapperClass(CountUsersMapper.class);
        job7.setReducerClass(CountUsersReducer.class);
        job7.setOutputKeyClass(NullWritable.class);
        job7.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job7, new Path(user));
        FileOutputFormat.setOutputPath(job7, new Path(distinctUserOutput));

        System.exit(job7.waitForCompletion(true) ? 0 : 1);

    }

}
