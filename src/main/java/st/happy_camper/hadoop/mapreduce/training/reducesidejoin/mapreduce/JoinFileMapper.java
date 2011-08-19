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
package st.happy_camper.hadoop.mapreduce.training.reducesidejoin.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import st.happy_camper.hadoop.mapreduce.training.reducesidejoin.io.ReduceSideJoinOutputKeyWritable;

/**
 * @author ueshin
 */
public class JoinFileMapper extends Mapper<Text, Text, ReduceSideJoinOutputKeyWritable, LongWritable> {

    private final ReduceSideJoinOutputKeyWritable keyout = new ReduceSideJoinOutputKeyWritable();

    private final LongWritable valueout = new LongWritable(0L);

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        keyout.word = key.toString();
        keyout.url = value.toString();
        context.write(keyout, valueout);
    }

}
