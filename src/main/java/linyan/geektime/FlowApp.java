package linyan.geektime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class FlowApp
{
    public static void main( String[] args )
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration=new Configuration();

        Job job=Job.getInstance(configuration);

        job.setJarByClass(FlowApp.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, "hdfs://jikehadoop01:8020/user/student/linyan/HTTP_20130313143750.dat");
        // FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job完成后的结果文件所在目录
        //FileOutputFormat.setOutputPath(job, new Path("hdfs://jikehadoop01:8020/user/student/linyan/output/"));
        FileOutputFormat.setOutputPath(job, new Path(args[0])); //Path(args[1])
        //向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
