package linyan.geektime;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
            throws IOException, InterruptedException {

        long sumDownFlow=0;
        long sumUpFlow=0;

        //统计每一个手机号耗费的总上行和下行流量
        for (FlowBean value : values) {

            sumUpFlow+=value.getUpFlow();
            sumDownFlow+=value.getDownFlow();
        }

        flowBean.setFlowData(sumUpFlow, sumDownFlow);

        context.write(key, flowBean);

    }
}
