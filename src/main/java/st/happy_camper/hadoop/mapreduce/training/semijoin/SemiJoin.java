/*
 * Copyright 2011 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hadoop.mapreduce.training.semijoin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import st.happy_camper.hadoop.mapreduce.training.semijoin.io.SemiJoinOutputKeyWritable;
import st.happy_camper.hadoop.mapreduce.training.semijoin.mapreduce.SemiJoinMapper;
import st.happy_camper.hadoop.mapreduce.training.semijoin.mapreduce.SemiJoinReducer;

/**
 * @author ueshin
 */
public class SemiJoin extends Configured implements Tool {

    /**
     * 
     */
    public SemiJoin() {
    }

    /**
     * @param conf
     */
    public SemiJoin(Configuration conf) {
        super(conf);
    }

    /**
     * @param idsFileUri
     * @param joinFileUri
     * @param inputPaths
     * @param outputDir
     * @return
     * @throws IOException
     */
    public Job createJob(URI idsFileUri, URI joinFileUri, Path[] inputPaths, Path outputDir) throws IOException {
        assert idsFileUri.getFragment() != null;

        Job job = new Job(getConf(), "SemiJoin");
        job.setJarByClass(getClass());

        DistributedCache.addCacheFile(idsFileUri, job.getConfiguration());
        job.getConfiguration().set(SemiJoin.class.getName() + ".idsFile", idsFileUri.getFragment());

        DistributedCache.addCacheFile(joinFileUri, job.getConfiguration());
        job.getConfiguration().set(SemiJoin.class.getName() + ".joinFile", joinFileUri.getFragment());

        DistributedCache.createSymlink(job.getConfiguration());

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, inputPaths);

        job.setMapperClass(SemiJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(SemiJoinReducer.class);

        job.setOutputKeyClass(SemiJoinOutputKeyWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputDir);

        return job;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        URI idsFileUri = URI.create(args[0]);
        URI joinFileUri = URI.create(args[1]);
        Path[] inputPaths = new Path[args.length - 3];
        for(int i = 2; i < args.length - 1; i++) {
            inputPaths[i - 2] = new Path(args[i]);
        }
        Path outputDir = new Path(args[args.length - 1]);

        return createJob(idsFileUri, joinFileUri, inputPaths, outputDir).waitForCompletion(true) ? 0 : 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new SemiJoin(), args));
    }

}
