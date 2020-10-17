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
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    private Text outId = new Text(), PRInfo = new Text();
    private int count;
    private String line;
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      if(itr.hasMoreTokens())
        line += itr.nextToken();
      line += (" " + itr.nextToken());
      count = itr.countTokens();
      line += (" " + String.valueOf(count)); 
      while (itr.hasMoreTokens()) {
        String id = itr.nextToken();
        outId.set(id);
        PRInfo.set(line);
        context.write(outId, PRInfo);
      }
    }
  }

  public static class PageRankReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    private int N = 0;
    private float Npr = 0;
    private String line;
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      if(key.toString().equals("0")){
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
