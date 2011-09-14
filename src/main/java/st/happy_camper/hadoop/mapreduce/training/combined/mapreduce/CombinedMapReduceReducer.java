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
package st.happy_camper.hadoop.mapreduce.training.combined.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import st.happy_camper.hadoop.mapreduce.training.combined.CombinedMapReduce;
import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedMapReduceMapOutputKeyWritable;
import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedSemiJoinOutputKeyWritable;
import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedWordCountV2OutputValueWritable;

/**
 * @author ueshin
 */
public class CombinedMapReduceReducer extends
        Reducer<CombinedMapReduceMapOutputKeyWritable, LongWritable, Text, LongWritable> {

    private MultipleOutputs<Text, LongWritable> mos;

    private final Text keyout1 = new Text();

    private final LongWritable valueout1 = new LongWritable();

    private final Text keyout2 = new Text();

    private final CombinedWordCountV2OutputValueWritable valueout2 = new CombinedWordCountV2OutputValueWritable();

    private final CombinedSemiJoinOutputKeyWritable keyout3 = new CombinedSemiJoinOutputKeyWritable();

    private final LongWritable valueout3 = new LongWritable();

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
        mos = new MultipleOutputs<Text, LongWritable>(context);

        String joinFile = context.getConfiguration().get(CombinedMapReduce.class.getName() + ".joinFile");
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(joinFile), "utf-8"));
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(CombinedMapReduceMapOutputKeyWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        switch(key.keyType) {
            case WORD_COUNT_V1: {
                keyout1.set(key.wordCountV1Key);

                long count = 0L;
                for(LongWritable value : values) {
                    count += value.get();
                }
                valueout1.set(count);

                mos.write("v1", keyout1, valueout1);

                break;
            }
            case WORD_COUNT_V2: {
                keyout2.set(new String(new int[] { key.wordCountV2Key.charAt(0) }, 0, 1));

                long words = 0L;
                long count = 0L;
                String current = null;
                for(LongWritable value : values) {
                    count += value.get();

                    String word = key.wordCountV2Key.toString();
                    if(!word.equals(current)) {
                        words++;
                        current = word;
                    }
                }
                valueout2.words = words;
                valueout2.count = count;

                mos.write("v2", keyout2, valueout2);

                break;
            }
            case SEMI_JOIN: {
                String word = key.semiJoinKey.toString();
                String line;
                while((line = reader.readLine()) != null) {
                    String[] kv = line.split("\t");
                    if(word.equals(kv[0])) {
                        keyout3.word = kv[0];
                        keyout3.url = kv[1];
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
                valueout3.set(cnt);

                mos.write("semijoin", keyout3, valueout3);

                break;
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce
     * .Reducer.Context)
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        reader.close();
        super.cleanup(context);
    }

}
