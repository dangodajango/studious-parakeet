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
    public RecordWriter<K, V> getRecordWriter(final TaskAttemptContext job) throws IOException {
        final Path file = getDefaultWorkFile(job, ".txt");
        final FileSystem fs = file.getFileSystem(job.getConfiguration());
        final FSDataOutputStream fsDataOutputStream = fs.create(file, false);
        return new RawDataWriter<>(fsDataOutputStream);
    }

    @RequiredArgsConstructor
    public static class RawDataWriter<K, V> extends RecordWriter<K, V> {

        private final DataOutputStream dataOutputStream;

        @Override
        public void write(final K key, final V value) throws IOException {
            dataOutputStream.writeBytes(value.toString() + "\n");
        }

        @Override
        public void close(final TaskAttemptContext context) throws IOException {
            dataOutputStream.close();
        }
    }
}
