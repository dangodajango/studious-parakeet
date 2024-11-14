package com.example.demo.hadoop;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class RawDataOutputFormat<K, V> extends TextOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException {
        Path file = getDefaultWorkFile(job, ".txt");
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fsDataOutputStream = fs.create(file, false);
        return new RawDataWriter<>(fsDataOutputStream);
    }

    @RequiredArgsConstructor
    public static class RawDataWriter<K, V> extends RecordWriter<K, V> {

        private final DataOutputStream dataOutputStream;

        @Override
        public void write(K key, V value) throws IOException {
            dataOutputStream.writeBytes(value.toString() + "\n");
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            dataOutputStream.close();
        }
    }
}
