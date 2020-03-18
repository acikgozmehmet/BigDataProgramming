import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MutualFriends {

    public static class MapperMutualFriends extends Mapper<LongWritable, Text, Text, Text> {

        // word is storing the pair of usernames
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // each line in the dataset is loaded as line
            String[] line = value.toString().split("->");
            // to make sure that each line is split into 2 parts
            // First part is the name of the user
            // and the second part is the friends
            if(line.length == 2){

                // first word is the  user
                String user = line[0].trim();
                line[1] = line[1].trim();
                // split each of its friends by whitespaces and store them into a list
                List<String> friends = Arrays.asList(line[1].split("\\s+"));
                // for each of the friend from the the list (friends)
                for(String friend: friends) {

                    friend = friend.trim();
                    if (user.compareToIgnoreCase(friend) < 0)
                        word.set("(" + user + " " + friend + ")" );
                    else
                        word.set("("+ friend + " " + user +")" );

                    // creating a map of two  users and whom their commmon friends
                    context.write(word, new Text(line[1]));
                }
            }
        }
    }

    public static class ReducerMutualFriends extends Reducer<Text, Text, Text, Text> {

        // to store the final reduced key-value pair
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // creating a new hash set so that there will be no duplicate key value
            // and creating stringbuilder to report the common friends

            Set<String> set = new HashSet<String>();
            StringBuilder stringBuilder = new StringBuilder();

            // for each friend in the values from key-value pair
            // reduce or group all the key value pairs by the key. For example (A, B)->C and (A, B)->D into (A,B)->(C,D)
            for (Text friends : values) {
                List<String> temp = Arrays.asList(friends.toString().trim().split("\\s+"));
                for(String friend: temp) {
                    if(set.contains(friend)) {
                        stringBuilder.append(friend + " "); // if it is in the set, report him/her as mutual friend
                    } else {
                        set.add(friend); // if it is not in the set, add him/her into the set
                    }
                }
            }

            // writing the reduced key value pair as the results
            // There has to be at least one common friend in order to report, otherwise skip without reporting
//            if (stringBuilder.toString().trim().length() != 0) {
                String output = "-> (" + stringBuilder.toString().trim() + ")";
                result.set(new Text(output));
                context.write(key, result);
//          }

        }
    }

    public static void main(String[] args) throws Exception {

        // number of arguments has to be exactly 2
        if(args.length != 2){
            System.err.println("Usage: MutualFriends <in_dir> <out_dir>");
        }

        // configuration setup
        Configuration conf = new Configuration();

        // set job instance
        Job job = Job.getInstance(conf, "MutualFriends");
        // class name
        job.setJarByClass(MutualFriends.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set mapper class and reducer class
        job.setMapperClass(MapperMutualFriends.class);
        job.setReducerClass(ReducerMutualFriends.class);

        // set input format and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        // first argument will be the input path and second will be output path
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}