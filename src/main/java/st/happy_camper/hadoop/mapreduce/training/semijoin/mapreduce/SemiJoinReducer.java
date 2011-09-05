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
package st.happy_camper.hadoop.mapreduce.training.semijoin.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import st.happy_camper.hadoop.mapreduce.training.semijoin.SemiJoin;
import st.happy_camper.hadoop.mapreduce.training.semijoin.io.SemiJoinOutputKeyWritable;

/**
 * @author ueshin
 */
public class SemiJoinReducer extends Reducer<Text, LongWritable, SemiJoinOutputKeyWritable, LongWritable> {

    private final SemiJoinOutputKeyWritable keyout = new SemiJoinOutputKeyWritable();

    private final LongWritable valueout = new LongWritable();

    private BufferedReader reader;

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce
     * .Reducer.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        String joinFile = context.getConfiguration().get(SemiJoin.class.getName() + ".joinFile");
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(joinFile), "utf-8"));
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {
        String word = key.toString();
        String line;
        while((line = reader.readLine()) != null) {
            String[] kv = line.split("\t");
            if(word.equals(kv[0])) {
                keyout.word = kv[0];
                keyout.url = kv[1];
                break;
            }
        }
        if(line == null) {
            throw new RuntimeException("JoinFile unexpectedly reached EOF.");
        }

        long cnt = 0L;
        for(LongWritable value : values) {
            cnt += value.get();
        }
        valueout.set(cnt);

        context.write(keyout, valueout);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce
     * .Reducer.Context)
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        reader.close();
        super.cleanup(context);
    }

}
