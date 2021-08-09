package com.geektime.linyan.geekfileformat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class GeekTextInputFormat implements InputFormat<LongWritable, Text>, JobConfigurable {

    /**
     * GeekRecordReader.
     *
     */
    public static class GeekRecordReader implements RecordReader<LongWritable, Text> {
        LineRecordReader reader;
        Text text;

        public GeekRecordReader(LineRecordReader reader) throws IOException {
            this.reader = reader;
            text = reader.createValue();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public LongWritable createKey() {
            return reader.createKey();
        }

        @Override
        public Text createValue() {
            return new Text("");
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            while (reader.next(key, text)) {
                String StrDecoded = text.toString().replaceAll("ge{2,256}k", "");
                Text txtDecoded = new Text();
                txtDecoded.set(StrDecoded);
                value.set(txtDecoded.getBytes(), 0, txtDecoded.getLength());
//                value.set(StrDecoded);
                return true;
            }
            // no more data
            return false;
        }
    }

    TextInputFormat format;
    JobConf job;

    public GeekTextInputFormat() {
        format = new TextInputFormat();
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        format.configure(job);
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        GeekRecordReader reader = new GeekRecordReader(
                new LineRecordReader(job, (FileSplit) genericSplit));
        return reader;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return format.getSplits(job, numSplits);
    }

}