package com.geektime.linyan.geekfileformat;

import java.io.IOException;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class GeekTextInputFormat  extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter)
            throws IOException {
        return new GeekRecordReader((FileSplit) inputSplit, job);
    }

    public static class GeekRecordReader implements RecordReader<LongWritable, Text> {
        LineRecordReader reader;
        Text text;
        LongWritable key;
        Random rand = new Random();

        public GeekRecordReader(FileSplit inputSplit, JobConf job) throws IOException {
            reader = new LineRecordReader(job, inputSplit);
            text = reader.createValue();
            key = reader.createKey();
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
                int randomNum = rand.nextInt(254) + 2;
                String replacedText = text.toString().replaceAll ("(\\w+\\s+){256}(\\w+\\s+)", "$1$2Ge(randomNum)k");;
                value.set(replacedText);
                return true;
            }
            // no more data
            return false;
        }
    }
}