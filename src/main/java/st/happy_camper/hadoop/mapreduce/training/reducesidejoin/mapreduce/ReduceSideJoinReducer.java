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
import org.apache.hadoop.mapreduce.Reducer;

import st.happy_camper.hadoop.mapreduce.training.reducesidejoin.io.ReduceSideJoinOutputKeyWritable;

/**
 * @author ueshin
 */
public class ReduceSideJoinReducer extends
        Reducer<ReduceSideJoinOutputKeyWritable, LongWritable, ReduceSideJoinOutputKeyWritable, LongWritable> {

    private final ReduceSideJoinOutputKeyWritable keyout = new ReduceSideJoinOutputKeyWritable();

    private final LongWritable valueout = new LongWritable();

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(ReduceSideJoinOutputKeyWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        if(key.url != null) {
            keyout.word = key.word;
            keyout.url = key.url;

            long cnt = 0L;
            for(LongWritable value : values) {
                cnt += value.get();
            }
            valueout.set(cnt);

            if(cnt > 0L) {
                context.write(keyout, valueout);
            }
        }
    }

}
