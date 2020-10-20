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

public class ComputePR {
  private static final float q = 0.85f;  
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    private Text outId = new Text(), PRInfo = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      int count = 0;
      String line = "";

      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");  // 按行切割，获取每一行的一个URL与其PR值、外链接
      while (itr.hasMoreTokens()) {
        String linkLine = itr.nextToken();
        /* 行内切割字符串 */
        String[] words = linkLine.split(" |\t");
        /* 获得链接+PR值+外链接数目字符串 */
        line = "";
        line += (words[0] + " " + words[1]);
        count = words.length - 2;
        line += (" " + String.valueOf(count));
        PRInfo.set(line);
        /* 添加<外链接， 来源链接PR信息> */
        for(int i = 2; i < words.length; i++){
          String id = words[i];
          outId.set(id);
          context.write(outId, PRInfo);
        }   
      }
    }
  }

  public static class PageRankReducer extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int N = 0;
      float Npr = 0.0f;
      String line = "";
      /* 重新计算PR值 */
      for (Text val : values) {
        String[] params = val.toString().split(" ");
        line += (" " + params[0]);
        float pr = Float.parseFloat(params[1]);
        int n = Integer.parseInt(params[2]);
        Npr += (pr / n);
        N++;
      }
      Npr *= q;
      Npr += (1 - q) / N;
      result.set(String.valueOf(Npr) + line);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "compute PR");
    job.setJarByClass(ComputePR.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(OutLinkJoinReducer.class);
    job.setReducerClass(PageRankReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
