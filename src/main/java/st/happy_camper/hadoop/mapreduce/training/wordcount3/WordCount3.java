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
package st.happy_camper.hadoop.mapreduce.training.wordcount3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import st.happy_camper.hadoop.mapreduce.training.wordcount3.io.WordCount3GroupingComparator;
import st.happy_camper.hadoop.mapreduce.training.wordcount3.io.WordCount3MapOutputKeyWritable;
import st.happy_camper.hadoop.mapreduce.training.wordcount3.io.WordCount3OutputValueWritable;
import st.happy_camper.hadoop.mapreduce.training.wordcount3.mapreduce.WordCount3Mapper;
import st.happy_camper.hadoop.mapreduce.training.wordcount3.mapreduce.WordCount3Partitioner;
import st.happy_camper.hadoop.mapreduce.training.wordcount3.mapreduce.WordCount3Reducer;

/**
 * @author ueshin
 */
public class WordCount3 extends Configured implements Tool {

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        Path[] inputPaths = new Path[args.length - 1];
        for(int i = 0; i < args.length - 1; i++) {
            inputPaths[i] = new Path(args[i]);
        }
        Path outputDir = new Path(args[args.length - 1]);

        Job job = new Job(getConf(), "WordCount3");
        job.setJarByClass(getClass());

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, inputPaths);

        job.setMapperClass(WordCount3Mapper.class);
        job.setMapOutputKeyClass(WordCount3MapOutputKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(WordCount3Partitioner.class);
        job.setGroupingComparatorClass(WordCount3GroupingComparator.class);

        job.setReducerClass(WordCount3Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordCount3OutputValueWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputDir);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WordCount3(), args));
    }

}
