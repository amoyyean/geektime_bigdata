package com.geektime.linyan.geekfileformat;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Random;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

public class GeekTextOutputForamt <K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {

    /**
     * GeekRecordWriter.
     *
     */
    public static class GeekRecordWriter implements RecordWriter {
        RecordWriter writer;
        Text text;

        public GeekRecordWriter(RecordWriter writer) {
        this.writer = writer;
        text = new Text();
        }

        @Override
        public void close(boolean abort) throws IOException {
            writer.close(abort);
        }

        public static int createRandNum(int min, int max) {
            Random rand = new Random();
            int randBound = max - min;
            int randomNum = rand.nextInt(randBound) + min;
            return randomNum;
        }

        public static String encodeGeek (int charCount) {
            String beginChar = "g" ;
            String repeatChar = "e" ;
            String endChar = "k" ;
            StringBuilder sb = new StringBuilder(beginChar);

            for (int i = 0; i < charCount; i++) {
                sb.append(repeatChar) ;
            }
            return sb.append(endChar).toString();
        }

        public static boolean validateWord(String word) {
            boolean res;
            // if word contains geek, return false
            Pattern pattern = Pattern.compile("ge{2,256}k");
            Matcher matcher = pattern.matcher(word) ;
            res = !matcher.matches();
            return res;
        }

        @Override
        public void write(Writable w) throws IOException {
            int min = 2;
            int max = 256;
            int randomNum = createRandNum(min, max);
            int validWordNum = 0;
            boolean firstWord = true;

            StringBuilder sb = new StringBuilder();
            // Split
            String[] words = ((Text) w).toString().split("\\s+");
            for (String word: words) {
                // Count valid words
                if (validateWord(word)) {
                    validWordNum++;
                }
                randomNum--;
                if (firstWord) {
                    sb.append(word);
                    firstWord = false;
                }
                else {
                    sb.append(" " + word);
                }
                if (randomNum == 0) {
                    String encodedWord = encodeGeek(validWordNum);
                    sb.append(" " + encodedWord);
                    // valid words count reset to 0 and create next random number
                    validWordNum = 0;
                    randomNum = createRandNum(min, max);
                }
            }
            String strEncoded = sb.toString();
            Text txtEncoded = new Text();
            txtEncoded.set(strEncoded);
            text.set(txtEncoded.getBytes(), 0, txtEncoded.getLength());
            writer.write(text);
        }
    }

    @Override
    public RecordWriter getHiveRecordWriter(JobConf job, Path outPath,
                                            Class<? extends Writable> valueClass, boolean isCompressed,
                                            Properties tableProperties, Progressable progress)
            throws IOException {
        GeekRecordWriter writer = new GeekRecordWriter(super
                .getHiveRecordWriter(job, outPath, BytesWritable.class,
                        isCompressed, tableProperties, progress));
        return writer;
    }
}
