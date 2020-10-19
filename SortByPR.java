import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortByPR {
  /*
    获得每一行包含的<URL, PR>信息，输出<PR, URL>
  */
  public static class TokenizerMapper extends Mapper<Object, Text, FloatWritable, Text>{
    private Text Id = new Text();
    private FloatWritable PR = new FloatWritable();
    private float v;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");  // 按行切割，获得URL与其PR值
      while (itr.hasMoreTokens()) {
        String line = itr.nextToken();
        /* 行内切割字符串 */
        String[] words = line.split(" |\t");
        Id.set(words[0]);
        v = Float.parseFloat(words[1]);
        PR.set(v);
        context.write(PR, Id);         
      }
    }
  }

  /*
    根据PR值进行排序，相同PR值的URL合并
  */
  public static class SortReducer extends Reducer<FloatWritable,Text,FloatWritable,Text> {
    private Text result = new Text();
    private String line = "";
    public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text val : values) {
        line += val.toString();
        line += " ";
      }
      result.set(line);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Sort by PR");
    job.setJarByClass(SortByPR.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(OutLinkJoinReducer.class);
    job.setReducerClass(SortReducer.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
