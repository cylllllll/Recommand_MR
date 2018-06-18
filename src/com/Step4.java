package com;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import java.lang.Object;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

/**
 * 矩阵相乘:
 * 1、转置评分矩阵
 * 2、相似度矩阵 与 (转置评分矩阵)
 * 这里进行2：相似度矩阵 与 (转置评分矩阵)相乘
 * input:
 * A   A_1.00,C_0.08,D_0.15,E_0.93,F_0.43
 * B   B_1.00,D_0.32,F_0.60
 * C   F_0.15,D_0.40,C_1.00,A_0.08
 * D   D_1.00,C_0.40,B_0.32,A_0.15
 * E   A_0.93,E_1.00,F_0.50
 * F   C_0.15,E_0.50,F_1.00,A_0.43,B_0.60
 * <p>
 * cache:
 * A   1_1,3_5,4_3
 * B   2_3,5_3
 * C   1_5,6_10
 * D   1_10,5_5
 * E   3_5,4_1
 * F   2_5,3_3,6_1
 * <p>
 * output:
 * A   1_2.90,2_2.15,3_10.94,4_3.93,5_0.75,6_1.23
 * B   1_3.20,2_6.00,3_1.80,5_4.60,6_0.60
 * C   6_10.15,5_2.00,4_0.24,3_0.85,2_0.75,1_9.08
 * D   1_12.15,6_4.00,5_5.96,4_0.45,3_0.75,2_0.96
 * E   1_0.93,2_2.50,3_11.15,4_3.79,6_0.50
 * F   1_1.18,2_6.80,3_7.65,4_1.79,5_1.80,6_2.50
 */
public class Step4 {
    public static class Mapper4 extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private List<String> cacheList = new ArrayList<String>();

        private DecimalFormat df = new DecimalFormat("0.00");

        /***
         *  将保存右侧矩阵的文件缓存到内存中，每一行为一个字符串，是所有行构成list
         */
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            FileReader fr = new FileReader("myfile");
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while ((line = br.readLine()) != null) {
                cacheList.add(line);
                System.out.println("----------------------cache line :" + line);
            }
            fr.close();
            br.close();
        }


        /*  左侧矩阵逻辑形式
         * 1    2   -2  0
         * 3    3   4   -3
         * -2   0   2   3
         * 5    3   -1  2
         * -4   2   0   2
         * 左侧矩阵物理形式
         * 1    1_1,2_2,3_-2,4_0
         * 2    1_3,2_3,3_4,4_-3
         * 3    1_-2,2_0,3_2,4_3
         * 4    1_5,2_3,3_-1,4_2
         * 5    1_-4,2_2,3_0,4_2
         *
         * 右侧矩阵（已转置）物理形式
         *  1   3_0,1_0,4_-2,2_1
            2   3_1,4_2,2_3,1_3
            3   4_-1,1_-1,3_4,2_5
            4   1_2,3_-1,4_1,2_-2
            5   4_2,3_2,1_-3,2_-1

            key: "1"
            value: "1   1_1,2_2,3_-2,4_0"
         * */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("-------------------map,key=" + key + "value=" + value);
            String[] rowAndline = value.toString().split("\t");
            //获得行号
            //rowAndline : {"1","1_1,2_2,3_-2,4_0"}
            String row_matrix1 = rowAndline[0];
            //row_matrix1 ："1"
            String[] column_value_array_matrix1 = rowAndline[1].split(",");
            //获得各列
            //rowAndline[1] ： "1_1,2_2,3_-2,4_0"
            //column_value_array_matrix1 : {"1_1","2_2","3_-2","4_0"}
            for (String line : cacheList)// 以line:"3     4_-1,1_-1,3_4,2_5"为例
            {
                String[] rowAndline2 = line.toString().split("\t");
                //rowAndline2 : {"3","4_-1,1_-1,3_4,2_5"}
                String row_matrix2 = rowAndline2[0];
                //获得转置矩阵line行的行号（原右矩阵的列号）
                String[] column_value_array_matrix2 = rowAndline2[1].split(",");
                //rowAndline2[1] : "4_-1,1_-1,3_4,2_5"
                //column_value_array_matrix2 : {"4_-1","1,-1","3_4","2_5"}
                double result = 0;
                //保存成绩累加结果
                for (String column_value_matrix1 : column_value_array_matrix1)//对于左侧矩阵line行的每一列(分量) "1_1","2_2","3_-2","4_0"
                {
                    String column_maxtrix1 = column_value_matrix1.split("_")[0];
                    //获得列号
                    String value_matrix1 = column_value_matrix1.split("_")[1];
                    //获得该列的值

                    for (String column_value_matrix2 : column_value_array_matrix2)//对于右侧矩阵的line行的每一列(分量) "4_-1","1,-1","3_4","2_5"
                    {
                        String column_maxtrix2 = column_value_matrix2.split("_")[0];
                        //获得列号
                        String value_matrix2 = column_value_matrix2.split("_")[1];
                        //获得该列的值

                        if (column_maxtrix2.equals(column_maxtrix1))//这里也体现了为什么要标明列号，只有列号明确且相等，才证明是同一个位置的分量
                        {
                            result += Double.valueOf(value_matrix1) * Double.valueOf(value_matrix2);
                            //result += 1 * (-1)
                            //result += 2 * 5
                            //result += -2 * 4
                            //result += 0 * (-1)
                        }
                    }
                }
                if (result == 0)
                    continue;

                outKey.set(row_matrix1);//输出的key值设置为左侧矩阵的行号
                outValue.set(row_matrix2 + "_" + df.format(result));//输出的value值设置为右侧转置矩阵的行号(实际矩阵的列号)_该位置的值
                context.write(outKey, outValue);
                //("1","3_1")
            }
            //("1","2_7")("1,"3_1")("1","2_4")("1","4_0")("1","5_9")
            //("2","1_9")...
            //....
        }
    }


    public static class Reducer4 extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        /**
         * 将map产生的key-value对进行组合，拼接成结果矩阵的物理形式
         * ("1","2_7")("1,"3_1")("1","2_4")("1","4_0")("1","5_9")
         * ("2","1_9")...
         * ...
         * 对于key值相同的元素("1","2_7")("1,"3_1")("1","2_4")("1","4_0")("1","5_9")
         * 会将其组合
         * key : "1"
         * values : {"2_7","3_1","2_4","4_0","5_9"}
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            for (Text text : values) {
                sb.append(text + ",");
            }
            // sb : "2_7,3_1,2_4,4_0,5_9,"
            String line = "";
            if (sb.toString().endsWith(",")) {
                line = sb.substring(0, sb.length() - 1);
            }
            //line :"2_7,3_1,2_4,4_0,5_9"
            outKey.set(key);
            outValue.set(line);
            context.write(outKey, outValue);
            // ("1","2_7,3_1,2_4,4_0,5_9")
        }

    }


    private static final String INPATH = "/output/tuijian2_2/part-r-00000";
    private static final String OUTPATH = "/output/tuijian2_4";
    private static final String CACHE = "/output/tuijian2_3/part-r-00000";


    public int run() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        String[] otherArgs = {INPATH, OUTPATH};
        //这里需要配置参数即输入和输出的文件路径
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        //conf.set("fs.defaultFS",HDFS);
        JobConf conf1 = new JobConf(Step4.class);
        Job job = new Job(conf, "Step4");//Job(Configuration conf, String jobName) 设置job名称和
        job.setJarByClass(Step4.class);
        job.setMapperClass(Mapper4.class); //为job设置Mapper类
        //job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类
        job.setReducerClass(Reducer4.class); //为job设置Reduce类

        job.addCacheArchive(new URI(CACHE + "#myfile"));
        //DistributedCache.addCacheArchive(new URI(CACHE + "#myfile"));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);        //设置输出key的类型
        job.setOutputValueClass(Text.class);//  设置输出value的类型

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
            new Step4().run();
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}