package my.books;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.*;


public class TrendsTweeter {

      public static class Map1 extends Mapper<Object, Text, Text, IntWritable>{
    	  
    	  private final static IntWritable one = new IntWritable(1);

 
    	  public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        	
            String line = value.toString();
            String[] tuple = line.split("\\n");
            try{
                for(int i=0;i<tuple.length; i++){
                    JSONObject obj = new JSONObject(tuple[i]);
                    JSONObject user = obj.getJSONObject("user");
                    String userid = user.getString("id_str").trim();
                    
                    if (userid != null && userid.length()> 0){
                      context.write(new Text(userid), one);
                    }
                }
            }catch(JSONException e){
                e.printStackTrace();
            }           
        }
    }

      
      
   public static class Reduce1 extends Reducer<Text,IntWritable,Text, IntWritable>{
    	  
   	  private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        	
        	int sum = 0;
        	for (IntWritable val: values){
        		sum += val.get();      		
        	}
        	result.set(sum);
        	context.write(key, result);
        
        }
    }

   
   
   public static class Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {
	   
       @Override
       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

           String line = value.toString(); // userid occurences
           StringTokenizer tokenizer = new StringTokenizer(line);
           while (tokenizer.hasMoreTokens()) {
               String token = tokenizer.nextToken();
               // Context here is like a multi set which allocates value "one" for key "word".
               //                                             occurences                             userid
               context.write(new LongWritable(Long.parseLong(tokenizer.nextToken().toString())), new Text(token));
           }
       }
   }

   public static class Reducer2 extends Reducer<LongWritable, Text, Text, Text> {

       @Override
       protected void reduce(LongWritable key, Iterable<Text> trends, Context context) throws IOException, InterruptedException {

           for (Text val : trends) {
               context.write(new Text(val.toString()), new Text(key.toString()));
           }
       }
   }
   

   
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: CombineBooks <in> <out>");
            System.exit(2);
        }

        final String tempPath = "/user/cloudera/project1/temp";
        final String outputPath = args[1];

        
        Job job1 = new Job(conf, "FirstJob");
        job1.setJarByClass(TrendsTweeter.class);
        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(tempPath));

//        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        boolean succ = job1.waitForCompletion(true);
        if (!succ) {
            System.out.println("Job1 failed, exiting");
            System.exit(1);
        }
        
        
        
        Job job2 = new Job(conf, "SecondPart");
        job2.setJarByClass(TrendsTweeter.class);

        FileInputFormat.setInputPaths(job2, new Path(tempPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reducer2.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        
        
        
    }
}