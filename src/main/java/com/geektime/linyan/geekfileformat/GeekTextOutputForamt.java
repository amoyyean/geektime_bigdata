package com.geektime.linyan.geekfileformat;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

public class GeekTextOutputForamt <K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {
    public RecordWriter getHiveRecordWriter(JobConf job, Path outPath,
                                            Class<? extends Writable> valueClass, boolean isCompressed,
                                            Properties tableProperties, Progressable progress)
            throws IOException {
        GeekRecordWriter writer = new GeekRecordWriter(super
                .getHiveRecordWriter(job, outPath, BytesWritable.class,
                        isCompressed, tableProperties, progress));
        return writer;
        }

    public class GeekRecordWriter implements RecordWriter {
        RecordWriter writer;
        BytesWritable bytesWritable;

        public GeekRecordWriter(RecordWriter writer) {
        this.writer = writer;
        bytesWritable = new BytesWritable();
        }

        @Override
        public void close(boolean abort) throws IOException {
            writer.close(abort);
        }

        @Override
        public void write(Writable w) throws IOException {
            String strReplace = ((Text)w).toString().replaceAll ("Ge{2,256}k", "");
            Text txtReplace = new Text();
            txtReplace.set(strReplace);
            byte [] output = txtReplace.getBytes();
            bytesWritable.set(output, 0, output.length);
            writer.write(bytesWritable);
        }
    }
}
