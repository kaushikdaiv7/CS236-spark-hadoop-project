package edu.ucr.cs.cs236;
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


public class App {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private Text groupKey = new Text();
        private IntWritable outputValue = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String currentLine = value.toString();

            String[] tokens = currentLine.split(",");
            
            //Setting group to the trimmed State value 
            String group = tokens[1].trim(); 

            //Filter rows with variable code
            String variable_to_sum = "2010_Census_Population";

            if (tokens[3].equals(variable_to_sum)){
                try {
                    
                    //Type cast the population value to integer
                    int colValue = Integer.parseInt(tokens[4]);

                    //Set key as State
                    groupKey.set(group);

                    //Set Value as the population of County
                    outputValue.set(colValue);
                    
                    context.write(groupKey, outputValue);
                } catch (NumberFormatException e) {
                    // leave this empty
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                          Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                //Sum the population of counties for each state
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();
        System.out.println("Start time: " + startTime);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "get_state_by_pop_2010");
        job.setJarByClass(App.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        long endTime = System.nanoTime();
        System.out.println("Time required: " + endTime);
        double timeRequired = (endTime - startTime) / 1e9;
        System.out.printf("Time required: %.4f seconds\n", timeRequired);
        System.exit(success ? 0 : 1);
    }
}