package test1;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    static enum CountersEnum { INPUT_WORDS }

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private boolean caseSensitive;
    private Set<String> patternsToSkip = new HashSet<String>();
    private Set<String> wordsToSkip = new HashSet<String>(); 
    private Configuration conf;
    private BufferedReader fis;
    /**
    * 整个setup就做了两件事： 1.读取配置文件中的wordcount.case.sensitive，赋值给caseSensitive变量
    * 2.读取配置文件中的wordcount.skip.patterns，如果为true，将CacheFiles的文件都加入过滤范围
    */
    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      // getBoolean(String name, boolean defaultValue)
      // 获取name指定属性的值，如果属性没有指定，或者指定的值无效，就用defaultValue返回。
      // 属性可以在命令行中通过-Dpropretyname指定，例如 -Dwordcount.case.sensitive=true
      // 属性也可以在main函数中通过job.getConfiguration().setBoolean("wordcount.case.sensitive",
      // true)指定      
      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
      if (conf.getBoolean("wordcount.skip.patterns", false)) {
        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
        // for (URI patternsURI : patternsURIs) {// 每一个patternsURI都代表一个文件
        //   Path patternsPath = new Path(patternsURI.getPath());
        //   String patternsFileName = patternsPath.getName().toString();
        //   parseSkipFile(patternsFileName);// 将文件加入过滤范围
        // }
        // 认为是三个文件
        Path patternsPath = new Path(patternsURIs[0].getPath());
        String patternsFileName = patternsPath.getName().toString();
        parseSkipFile(patternsFileName);// 将文件加入过滤范围

        patternsPath = new Path(patternsURIs[1].getPath());
        patternsFileName = patternsPath.getName().toString();
        wordSkipFile(patternsFileName);// 将文件加入过滤范围

        patternsPath = new Path(patternsURIs[2].getPath());
        patternsFileName = patternsPath.getName().toString();
        wordSkipFile(patternsFileName);// 将文件加入过滤范围
      }
    }

    /**
    * 将指定文件的内容加入过滤范围
    * 
    * @param fileName
    */
    private void parseSkipFile(String fileName) {
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String pattern = null;
        while ((pattern = fis.readLine()) != null) {// SkipFile的每一行都是一个需要过滤的pattern，例如\!
          patternsToSkip.add(pattern);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file '"
            + StringUtils.stringifyException(ioe));
      }
    }
    private void wordSkipFile(String fileName){
      try {
        fis = new BufferedReader(new FileReader(fileName));
        String word = null;
        while ((word = fis.readLine()) != null) {// SkipFile的每一行都是一个需要过滤的word，例如one
          wordsToSkip.add(word);
        }
      } catch (IOException ioe) {
        System.err.println("Caught exception while parsing the cached file(words) '"
            + StringUtils.stringifyException(ioe));
      }

    }

    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = (caseSensitive) ?// 如果设置了大小写敏感，就保留原样，否则全转换成小写
          value.toString() : value.toString().toLowerCase();
      for (String pattern : patternsToSkip) {// 将数据中所有满足patternsToSkip的pattern都过滤掉
        line = line.replaceAll(pattern, "");
      }
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        if(word.getLength()>=3 && !wordsToSkip.contains(word.toString())){//长度大于等于3才统计
        context.write(word, one);
        // getCounter(String groupName, String counterName)计数器
        // 枚举类型的名称即为组的名称，枚举类型的字段就是计数器名称
        Counter counter = context.getCounter(CountersEnum.class.getName(),
            CountersEnum.INPUT_WORDS.toString());
        counter.increment(1);
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if ((remainingArgs.length != 2) && (remainingArgs.length != 6)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    //排序的main部分处理
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        for(;i < remainingArgs.length;++i){//多个patterns文件
          job.addCacheFile(new Path(remainingArgs[i]).toUri());
        }
        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }


    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}