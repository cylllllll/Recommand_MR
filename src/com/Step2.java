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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/***
 *
 *计算每两行的相似度，最终形成一个相似度矩阵
 *
 * input
 *  A   1_1,3_5,4_3
 B   2_3,5_3
 C   1_5,6_10
 D   1_10,5_5
 E   3_5,4_1
 F   2_5,3_3,6_1
 同时将input拷贝一份到缓存cache中，然后对input和cache的每一行就行求值
 output:
 A   A_1.00,C_0.08,D_0.15,E_0.93,F_0.43
 B   B_1.00,D_0.32,F_0.60
 C   F_0.15,D_0.40,C_1.00,A_0.08
 D   D_1.00,C_0.40,B_0.32,A_0.15
 E   A_0.93,E_1.00,F_0.50
 F   C_0.15,E_0.50,F_1.00,A_0.43,B_0.60
 *
 */
public class Step2 {

    /***
     * input:
     * itemID [userID_sumScore...]
     A   1_1,3_5,4_3
     B   2_3,5_3
     C   1_5,6_10
     D   1_10,5_5
     E   3_5,4_1
     F   2_5,3_3,6_1
     cache : = input
     output:
     map,key=0,value=A   1_1,3_5,4_3
     A   A_1.00
     A   C_0.08
     A   D_0.15
     A   E_0.93
     A   F_0.43
     map,key=14,value=B  2_3,5_3
     B   B_1.00
     B   D_0.32
     B   F_0.60
     map,key=24,value=C  1_5,6_10
     C   A_0.08
     C   C_1.00
     C   D_0.40
     C   F_0.15
     map,key=35,value=D  1_10,5_5
     D   A_0.15
     D   B_0.32
     D   C_0.40
     D   D_1.00
     map,key=46,value=E  3_5,4_1
     E   A_0.93
     E   E_1.00
     E   F_0.50
     map,key=56,value=F  2_5,3_3,6_1
     F   A_0.43
     F   B_0.60
     F   C_0.15
     F   E_0.50
     F   F_1.00
     *
     *
     */
    public static class Mapper2 extends Mapper<LongWritable,Text,Text,Text>
    {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private List<String> cacheList = new ArrayList<String>();

        private DecimalFormat df = new DecimalFormat("0.00");

        /***
         *  将文件缓存到内存中，每一行为一个字符串，是所有行构成list
         */
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            FileReader fr = new FileReader("itemUserScore2");
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while((line = br.readLine()) != null)
            {
                cacheList.add(line);
            }
            fr.close();
            br.close();
        }

        /***
         *  以
         *  value ：1    A_2,C_5
         cacheList : 2   A_10,B_3
         为例
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("map,key=" + key + ",value=" + value.toString());
            String[] rowAndline = value.toString().split("\t");
            //获得行号
            //rowAndline : 1    A_2,C_5
            String row_matrix1 = rowAndline[0];
            //row_matrix1 ：1
            String[] column_value_array_matrix1 = rowAndline[1].split(",");
            //获得各列
            //rowAndline[1] ： A_2,C_5
            //column_value_array_matrix1 : [A_2,C_5]

            //|x|=sqrt(x1^2+x2^2+...)
            double denominator1 = 0;
            //定义向量1的模
            for(String colunm : column_value_array_matrix1)//对于向量1的每一个分量
            {
                String score = colunm.split("_")[1];
                denominator1 +=  Double.valueOf(score) * Double.valueOf(score);
                //计算分量的平方并累加到模
            }
            denominator1 = Math.sqrt(denominator1);//开跟号得到模


            for(String line : cacheList)// 以line 2  A_10,B_3  为例
            {
                String[] rowAndline2 = line.toString().split("\t");
                //rowAndline2 : 2   A_10,B_3
                String row_matrix2 = rowAndline2[0];
                //row_matrix2 :2
                String[] column_value_array_matrix2 = rowAndline2[1].split(",");
                //column_value_array_matrix2 : A_10,B_3

                double denominator2 = 0;//求向量2的模
                for(String colunm : column_value_array_matrix2)
                {
                    String score = colunm.split("_")[1];
                    denominator2 +=  Double.valueOf(score) * Double.valueOf(score);
                }
                denominator2 = Math.sqrt(denominator2);


                int numerator = 0;
                //保存成绩累加结果
                for(String column_value_matrix1 : column_value_array_matrix1)//对于向量1的每一列(分量) A_2,C_5
                {
                    String column_maxtrix1 = column_value_matrix1.split("_")[0];
                    //获得用户ID
                    String value_matrix1 = column_value_matrix1.split("_")[1];
                    //获得分数

                    for(String column_value_matrix2 : column_value_array_matrix2)//对于向量2的每一列(分量) A_10,B_3
                    {
                        String column_maxtrix2 = column_value_matrix2.split("_")[0];
                        //获得用户ID
                        String value_matrix2 = column_value_matrix2.split("_")[1];
                        //获得分数

                        //如果是同一个分量
                        if(column_maxtrix2.equals(column_maxtrix1))//这里也体现了为什么要标明列号，只有列号明确且相等，才证明是同一个位置的分量
                        {
                            numerator += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);
                            //numerator += 2×10
                        }
                    }
                }

                double cos = numerator / (denominator1 * denominator2);
                //求余弦
                if(cos == 0)
                    continue;
                outKey.set(row_matrix1);//输出的key值设置为左侧矩阵的行号
                outValue.set(row_matrix2 + "_" + df.format(cos));//输出的value值设置为右侧转置矩阵的行号(实际矩阵的列号)_该位置的值
                context.write(outKey, outValue);
                System.out.println(outKey + "\t" + outValue);
            }
        }
    }

    /***
     * input:
     *  ("1",["1_1.00","2_0.36","3_0.93","4_0.99","6_0.26"])
     ("2",["1_0.36","2_1.00","4_0.49","5_0.29","6_0.88"])
     ("3",["4_0.86","3_1.00","1_0.93"])
     ("4",["1_0.99","4_1.00","6_0.36","3_0.86","2_0.49"])
     ("5",["2_0.29","5_1.00","6_0.71"])
     ("6",["1_0.26","5_0.71","6_1.00","2_0.88","4_0.36"])

     output:
     1   1_1.00,2_0.36,3_0.93,4_0.99,6_0.26
     2   1_0.36,2_1.00,4_0.49,5_0.29,6_0.88
     3   4_0.86,3_1.00,1_0.93
     4   1_0.99,4_1.00,6_0.36,3_0.86,2_0.49
     5   2_0.29,5_1.00,6_0.71
     6   1_0.26,5_0.71,6_1.00,2_0.88,4_0.36

     即将分量连起来
     得到最终的相似度矩阵
     *
     *
     *
     */
    public static class Reducer2 extends Reducer<Text,Text,Text,Text>
    {
        private Text outKey = new Text();
        private Text outValue = new Text();


        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //System.out.println(ReduceUtils.getReduceInpt(key, values));
            //只能遍历一次？
            StringBuilder sb = new StringBuilder();
            for(Text text : values)
            {
                sb.append(text + ",");
            }
            String line = "";
            if(sb.toString().endsWith(","))
            {
                line = sb.substring(0,sb.length()-1);
            }
            outKey.set(key);
            outValue.set(line);
            context.write(outKey, outValue);
        }

    }


    private static final String INPATH = "/output/tuijian2_1/part-r-00000";
    private static final String OUTPATH = "/output/tuijian2_2";

    private static final String CACHE = "/output/tuijian2_1/part-r-00000";

    public int run() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS",HDFS);
        String[] otherArgs = {INPATH,OUTPATH};
        //这里需要配置参数即输入和输出的HDFS的文件路径
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "step2");//Job(Configuration conf, String jobName) 设置job名称和
        job.setJarByClass(Step2.class);
        job.setMapperClass(Mapper2.class); //为job设置Mapper类
        //job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类
        job.setReducerClass(Reducer2.class); //为job设置Reduce类

        job.addCacheArchive(new URI(CACHE + "#itemUserScore2"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);        //设置输出key的类型
        job.setOutputValueClass(Text.class);//  设置输出value的类型

        //TODO
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //为map-reduce任务设置InputFormat实现类   设置输入路径

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//为map-reduce任务设置OutputFormat实现类  设置输出路径

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(OUTPATH);
        if(fs.exists(outPath))
        {
            fs.delete(outPath, true);
        }

        return job.waitForCompletion(true) ? 1 : -1;


    }

    public static void main(String[] args)
    {
        try {
            new Step2().run();
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}