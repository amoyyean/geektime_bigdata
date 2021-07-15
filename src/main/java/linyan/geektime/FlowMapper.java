package linyan.geektime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();
    private Text keyText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line=value.toString();
        String []fields=StringUtils.split(line,'\t');
        //获取电话号码
        String phoneNum=fields[1];
        //获取上传流量数据
        long upflow=Long.parseLong(fields[fields.length-3]);
        //获取下载流量数据
        long downflow=Long.parseLong(fields[fields.length-2]);
        flowBean.setFlowData(upflow, downflow);
        keyText.set(phoneNum);

        context.write(keyText, flowBean);

    }

}