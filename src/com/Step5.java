package com;

        import java.io.BufferedReader;
        import java.io.FileReader;
        import java.io.IOException;
        import java.net.URI;
        import java.net.URISyntaxException;
        import java.text.DecimalFormat;
        import java.util.ArrayList;
        import java.util.List;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.FileSystem;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.Reducer.Context;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/***
 * 去掉推荐列表中，用户已经操作过的商品，例如用户A已经购买过iphone7，则将iphone7从推荐列表中删除
 * input：相似度矩阵
 *  1   A_9.87,B_2.38,C_23.90
 2   A_16.59,B_8.27,C_4.25
 3   C_23.95,A_4.44
 4   B_3.27,C_22.85,A_11.68
 5   A_6.45,B_7.42
 6   C_3.10,A_15.40,B_9.77
 * cache:操作记录
 *  1   A_2,C_5
 2   A_10,B_3
 3   C_15
 4   A_3,C_5
 5   B_3
 6   A_5,B_5

 map:
 例如
 1商品的推荐列表：1      A_9.87,B_2.38,C_23.90
 1商品的操作记录：1      A_2,C_5
 则对于1商品，由于A已经有2分，C已经右5分
 应该把A和C从1的推荐列表中删除，
 只保留B
 而最终是要根据用户来推荐商品，于是将用户作为key,物品和推荐度作为value返回
 (B,1_2.38)

 reduce：
 将同一用户推荐的商品合并输出

 output:
 A   2_2.15,5_0.75,6_1.23
 B   1_3.20,3_1.80,6_0.60
 C   2_0.75,3_0.85,4_0.24,5_2.00
 D   2_0.96,3_0.75,4_0.45,6_4.00
 E   1_0.93,2_2.50,6_0.50
 F   1_1.18,4_1.79,5_1.80
 *
 */
public class Step5 {

    public static class Mapper5 extends Mapper<LongWritable, Text, Text, Text> {
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
            FileReader fr = new FileReader("itemUserScore4");
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while ((line = br.readLine()) != null) {
                cacheList.add(line);
                System.out.println("----------------------cache line :" + line);
            }
            fr.close();
            br.close();
        }


        /**
         * 以
         * 1商品的推荐列表：1      A_9.87,B_2.38,C_23.90
         * 1商品的操作记录：1      A_2,C_5
         * 为例
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("-------------------map,key=" + key + "value=" + value);
            String item_matrix1 = value.toString().split("\t")[0];
            //推荐列表商品号 1
            String[] user_score_array_matrix1 = value.toString().split("\t")[1].split(",");
            //推荐列表 A_9.87,B_2.38,C_23.90

            for (String line : cacheList)//商品的操作记录列表
            {
                String item_matrix2 = line.toString().split("\t")[0];
                //操作记录商品号 1

                String[] user_score_array_matrix2 = line.toString().split("\t")[1].split(",");
                //操作记录  A_2,C_5

                if (item_matrix1.equals(item_matrix2))//如果推荐列表商品号==操作记录商品号，证明是同一商品，才能操作
                {
                    for (String user_score : user_score_array_matrix1)//对于推荐列表中每一个用户  A_9.87,B_2.38,C_23.90
                    {
                        boolean flag = false;//默认操作过标志位
                        String user_matrix1 = user_score.split("_")[0];
                        //用户ID
                        String score_matrix1 = user_score.split("_")[1];
                        //推荐度

                        for (String user_score2 : user_score_array_matrix2)//对于操作记录中的每一条记录  A_2,C_5
                        {
                            String user_matrix2 = user_score2.split("_")[0];
                            //用户ID
                            if (user_matrix1.equals(user_matrix2))//如果两个ID相等 如A_9.87 和A_2 则证明用户A操作过该商品
                            {
                                flag = true;
                            }
                        }
                        if (flag == false)//如果用户A没有操作过该物品
                        {
                            outKey.set(item_matrix1);//将用户ID作为Key
                            outValue.set(user_matrix1 + "_" + score_matrix1);//将商品ID_推荐度作为value
                            context.write(outKey, outValue);//写入结果集
                        }
                    }
                }
            }
        }
    }


    public static class Reducer5 extends Reducer<Text, Text, Text, Text> {
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


    private static final String INPATH = "/output/tuijian2_4/part-r-00000";
    private static final String OUTPATH = "/output/tuijian2_5";
    private static final String CACHE = "/output/tuijian2_1/part-r-00000";

    public int run() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        String[] otherArgs = {INPATH, OUTPATH};
        //这里需要配置参数即输入和输出的文件路径
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        //conf.set("fs.defaultFS",HDFS);
        // JobConf conf1 = new JobConf(WordCount.class);
        Job job = new Job(conf, "step4");//Job(Configuration conf, String jobName) 设置job名称和
        job.setJarByClass(Step5.class);
        job.setMapperClass(Mapper5.class); //为job设置Mapper类
        //job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类
        job.setReducerClass(Reducer5.class); //为job设置Reduce类

        job.addCacheArchive(new URI(CACHE + "#itemUserScore4"));

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
            new Step5().run();
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}