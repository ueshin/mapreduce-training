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
package st.happy_camper.hadoop.mapreduce.training.combined;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedMapReduceGroupingComparator;
import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedMapReduceMapOutputKeyWritable;
import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedSemiJoinOutputKeyWritable;
import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedWordCountV2OutputValueWritable;
import st.happy_camper.hadoop.mapreduce.training.combined.mapreduce.CombinedMapReducePartitioner;
import st.happy_camper.hadoop.mapreduce.training.combined.mapreduce.CombinedMapReduceReducer;
import st.happy_camper.hadoop.mapreduce.training.combined.mapreduce.CombinedSemiJoinMapper;
import st.happy_camper.hadoop.mapreduce.training.combined.mapreduce.CombinedWordCountV1Mapper;
import st.happy_camper.hadoop.mapreduce.training.combined.mapreduce.CombinedWordCountV2Mapper;

/**
 * @author ueshin
 */
public class CombinedMapReduce extends Configured implements Tool {

    /**
     * 
     */
    public CombinedMapReduce() {
    }

    /**
     * @param conf
     */
    public CombinedMapReduce(Configuration conf) {
        super(conf);
    }

    /**
     * @param wordCountV1InputPaths
     * @param wordCountV2InputPaths
     * @param idsFileUri
     * @param joinFileUri
     * @param semiJoinInputPaths
     * @param outputPath
     * @return
     * @throws IOException
     */
    public Job createJob(Path[] wordCountV1InputPaths, Path[] wordCountV2InputPaths, URI idsFileUri, URI joinFileUri,
            Path[] semiJoinInputPaths, Path outputPath) throws IOException {
        assert idsFileUri.getFragment() != null;
        assert joinFileUri.getFragment() != null;

        Job job = new Job(getConf(), "CombinedMapReduce");
        job.setJarByClass(getClass());

        for(Path inputPath : wordCountV1InputPaths) {
            MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, CombinedWordCountV1Mapper.class);
        }

        for(Path inputPath : wordCountV2InputPaths) {
            MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, CombinedWordCountV2Mapper.class);
        }

        DistributedCache.addCacheFile(idsFileUri, job.getConfiguration());
        job.getConfiguration().set(CombinedMapReduce.class.getName() + ".idsFile", idsFileUri.getFragment());

        DistributedCache.addCacheFile(joinFileUri, job.getConfiguration());
        job.getConfiguration().set(CombinedMapReduce.class.getName() + ".joinFile", joinFileUri.getFragment());

        DistributedCache.createSymlink(job.getConfiguration());

        for(Path inputPath : semiJoinInputPaths) {
            MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, CombinedSemiJoinMapper.class);
        }

        job.setMapOutputKeyClass(CombinedMapReduceMapOutputKeyWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setCombinerClass(LongSumReducer.class);
        job.setPartitionerClass(CombinedMapReducePartitioner.class);
        job.setGroupingComparatorClass(CombinedMapReduceGroupingComparator.class);

        job.setNumReduceTasks(2);
        job.setReducerClass(CombinedMapReduceReducer.class);

        MultipleOutputs.setCountersEnabled(job, true);
        MultipleOutputs.addNamedOutput(job, "v1", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(job, "v2", TextOutputFormat.class, Text.class,
                CombinedWordCountV2OutputValueWritable.class);
        MultipleOutputs.addNamedOutput(job, "semijoin", TextOutputFormat.class,
                CombinedSemiJoinOutputKeyWritable.class, LongWritable.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        List<Path> wordCountV1InputPaths = new ArrayList<Path>();
        for(String path : args[0].split(":")) {
            wordCountV1InputPaths.add(new Path(path));
        }
        List<Path> wordCountV2InputPaths = new ArrayList<Path>();
        for(String path : args[1].split(":")) {
            wordCountV2InputPaths.add(new Path(path));
        }
        URI idsFileUri = new URI(args[2]);
        URI joinFileUri = new URI(args[3]);
        List<Path> semiJoinInputPaths = new ArrayList<Path>();
        for(String path : args[4].split(":")) {
            semiJoinInputPaths.add(new Path(path));
        }
        Path outputPath = new Path(args[5]);

        return createJob(wordCountV1InputPaths.toArray(new Path[wordCountV1InputPaths.size()]),
                wordCountV2InputPaths.toArray(new Path[wordCountV2InputPaths.size()]), idsFileUri, joinFileUri,
                semiJoinInputPaths.toArray(new Path[semiJoinInputPaths.size()]), outputPath).waitForCompletion(true) ? 0
                : 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new CombinedMapReduce(), args));
    }

}
