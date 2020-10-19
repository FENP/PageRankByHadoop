import java.io.IOException;
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

public class OutLink {
  /*
    使用map函数得到<URL, 外链接>
  */
  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{
    private IntWritable word1 = new IntWritable();  // key
    private Text word2 = new Text();                // value
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");  // 按行切割字符串
      while (itr.hasMoreTokens()) {
        String line = itr.nextToken();
        /* 忽略注释行 */
        if(line.charAt(0) == '#')
          continue;
        /* 行内切割字符串 */
        String[] words = line.split("\t");
        word1.set(Integer.parseInt(words[0]));  // 获得URL
        word2.set(words[1]);                    // 获得外链接
        context.write(word1, word2);            
      }
    }
  }

  /*
    合并外链接，并将初始PR值置为1.0
  */
  public static class OutLinkJoinReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text result = new Text();
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String line = "1.0";
      for (Text val : values) {
        line += " ";
        line += val.toString();
      }
      result.set(line);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "oulink join");
    job.setJarByClass(OutLink.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(OutLinkJoinReducer.class);
    job.setReducerClass(OutLinkJoinReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
