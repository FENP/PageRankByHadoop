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

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text word1 = new Text(), word2 = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        String line = itr.nextToken();
        if(line.charAt(0) == '#')
          continue;
        String[] words = line.split(" |\t");
        word1.set(words[0]);
        word2.set(words[1]);
        context.write(word1, word2);
      }
    }
  }

  public static class OutLinkJoinReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
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
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
