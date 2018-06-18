package com;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Step1 {


    /***
     * input:
     * userID,itemID,score
     *  A,1,1
     A,3,5
     A,4,3
     B,2,3
     B,5,3
     C,1,5
     C,6,10
     D,1,10
     D,5,5
     E,3,5
     E,4,1
     F,2,5
     F,3,3
     F,6,1
     * output:
     * (userID,itemID__score)
     *  ("A","1_1")
     ("A","3_5")
     ("A","4_3")
     ("B","2_3")
     ("B","5_3")
     ("C","1_5")
     ("C","6_10")
     ("D","1_10")
     ("D","5_5")
     ("E","3_5")
     ("E","4_1")
     ("F","2_5")
     ("F","3_3")
     ("F","6_1")
     *
     * 即map操作是将（用户ID,物品ID,行为分值)转为（用户ID,物品ID_行为分值)
     *
     */
    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //System.out.println("map,key=" + key + ",value=" + value.toString());
            String values[] = value.toString().split(",");
            String userID = values[0];
            String itemID = values[1];
            String score = values[2];
            outKey.set(userID);
            outValue.set(itemID + "_" + score);
            context.write(outKey, outValue);
            System.out.println("(\"" + userID + "\",\"" + itemID + "_" + score + "\")");
        }
    }

    /***
     * input:
     * itemID [userID_socre...]
     *  ("A",["1_1","3_5","4_3"])
     ("B",["2_3","5_3"])
     ("C",["6_10","1_5"])
     ("D",["5_5","1_10"])
     ("E",["3_5","4_1"])
     ("F",["6_1","2_5","3_3"])

     output:
     userID [itemID_sumScore...]
     ("A",["1_1","3_5","4_3"])
     ("B",["2_3","5_3"])
     ("C",["6_10","1_5"])
     ("D",["5_5","1_10"])
     ("E",["3_5","4_1"])
     ("F",["6_1","2_5","3_3"])
     *
     */
    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String itemID = key.toString();
            StringBuilder log = new StringBuilder();
            log.append("(\"" + itemID + "\",[");
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text value : values) {
                log.append("\"" + value + "\",");
                String userID = value.toString().split("_")[0];
                String score = value.toString().split("_")[1];
                if (map.get(userID) == null) {
                    map.put(userID, Integer.valueOf(score));
                } else {
                    Integer preScore = map.get(userID);
                    map.put(userID, preScore + Integer.valueOf(score));
                }
            }
            if (log.toString().endsWith(","))
                log.deleteCharAt(log.length() - 1);
            log.append("])");
            System.out.println(log);
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String userID = entry.getKey();
                String score = String.valueOf(entry.getValue());
                sb.append(userID + "_" + score + ",");
            }
            String line = null;
            if (sb.toString().endsWith(",")) {
                line = sb.substring(0, sb.length() - 1);
            }
            outKey.set(itemID);
            outValue.set(line);
            context.write(outKey, outValue);
        }

    }

    private static final String INPATH = "/input/data.txt";//输入文件路径
    private static final String OUTPATH = "/output/tuijian2_1";//输出文件路径

    public int run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = {INPATH, OUTPATH};
        //这里需要配置参数即输入和输出的文件路径
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        //conf.set("fs.defaultFS",HDFS);
        // JobConf conf1 = new JobConf(WordCount.class);
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "step1");//Job(Configuration conf, String jobName) 设置job名称和
        job.setJarByClass(Step1.class);
        job.setMapperClass(Mapper1.class); //为job设置Mapper类
        //job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类
        job.setReducerClass(Reducer1.class); //为job设置Reduce类

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);        //设置输出key的类型
        job.setOutputValueClass(Text.class);//  设置输出value的类型

        //TODO
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //为map-reduce任务设置InputFormat实现类   设置输入路径

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//为map-reduce任务设置OutputFormat实现类  设置输出路径

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(OUTPATH);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true) ? 1 : -1;

    }

    public static void main(String[] args) {
        try {
            new Step1().run();
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}