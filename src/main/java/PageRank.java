import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

    public class LinkMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        protected void map(Object key, Text value, Context context) {
            // TODO: write link mapping function
        }

    }

    public class TitleMapper extends Mapper<Object, Text, IntWritable, Text> {

        @Override
        protected void map(Object key, Text value, Context context) {
            // TODO: write title mapping function
        }

    }

    public class RankReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) {
            // TODO: write reducer function
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PageRank(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
