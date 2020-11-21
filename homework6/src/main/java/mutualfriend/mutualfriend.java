package mutualfriend;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class mutualfriend {

    /*
     * 第一阶段map，输出<朋友，人>
     */
    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] person_friends = line.split(",");
            String person = person_friends[0];
            person_friends[1] = person_friends[1].trim();// 去掉首尾空格
            String[] friends = person_friends[1].split(" ");
            for (String friend : friends) {
                context.write(new Text(friend), new Text(person));
            }
        }
    }

    /**
     * 第一阶段 Reduce 对所有传过来的<朋友，list(人)>进行拼接，输出<朋友,拥有这名朋友的所有人>
     **/
    public static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuffer bf = new StringBuffer();// 可变的字符串
            for (Text friend : values) {
                bf.append(friend.toString()).append(",");
            }
            bf = bf.deleteCharAt(bf.length() - 1);// 把最后边的","删掉
            context.write(key, new Text(String.valueOf(bf)));
        }
    }

    /**
     * 第二阶段Mapper 将第一阶段产生的数据作为原数据
     * 1.将上一阶段reduce输出的<朋友,拥有这名朋友的所有人>信息中的“拥有这名朋友的所有人”进行排序 ，以防出现B-C C-B这样的重复
     * 2.将“拥有这名朋友的所有人”进行两两配对，并将配对后的字符串当做键，“朋友”当做值输出，即输出<人-人，共同朋友>
     **/
    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] friend_persons = line.split("\t");
            String friend = friend_persons[0];
            String[] persons = friend_persons[1].split(",");
            Arrays.sort(persons); // 排序
            // 两两配对
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    context.write(new Text("[" + persons[i] + "," + persons[j] + "]"), new Text(friend));
                }
            }

        }
    }

    /**
     * 第二阶段的reduce <人-人，list(共同朋友)> 中的“共同好友”进行拼接 最后输出<人-人，两人的所有共同好友>
     **/
    public static class MyReduce2 extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuffer bf = new StringBuffer();
            Set<String> set = new HashSet<>();
            for (Text s : values) {
                if (!set.contains(s.toString())) {
                    set.add(s.toString());
                }
            }
            bf.append("[");
            for (String s : set) {
                bf.append(s).append(",");
            }
            bf = bf.deleteCharAt(bf.length() - 1);
            bf.append("]");
            Text rekey = new Text("(" + key.toString() + "," + bf.toString() + ")");
            context.write(rekey, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: mutualfriend  input output");
            System.exit(2);
        }
        // 第一阶段
        Job job1 = Job.getInstance(conf, "step1");
        job1.setJarByClass(mutualfriend.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReduce1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        Path tempDir = new Path("mutualfriend-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));// 定义一个临时目录
        FileInputFormat.addInputPath(job1, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job1, tempDir);
        //job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setInputFormatClass(TextInputFormat.class);
        if(job1.waitForCompletion(true)){
        // 第二阶段
        Job job2 = Job.getInstance(conf, "step2");
        job2.setJarByClass(mutualfriend.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReduce2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, tempDir);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job2, new Path(remainingArgs[1]));
        System.out.println(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}