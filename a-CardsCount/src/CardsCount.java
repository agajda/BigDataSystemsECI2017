
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CardsCount {
	
  public static final String inputFilename = "cards.txt";
  public static final String outputDir = "output";	

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "CardsCount");  
    job.setJarByClass(CardsCount.class);    	
 	job.setMapperClass(CardMapper.class);
 	job.setMapOutputKeyClass(Text.class);
 	job.setMapOutputValueClass(IntWritable.class); 	 
 	job.setReducerClass(CardTotalReducer.class);          
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputFilename));
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
