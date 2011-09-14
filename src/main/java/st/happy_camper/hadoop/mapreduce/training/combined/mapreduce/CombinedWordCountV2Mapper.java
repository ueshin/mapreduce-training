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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedMapReduceMapOutputKeyWritable;
import st.happy_camper.hadoop.mapreduce.training.combined.io.CombinedMapReduceMapOutputKeyWritable.KeyType;

/**
 * @author ueshin
 */
public class CombinedWordCountV2Mapper extends
        Mapper<LongWritable, Text, CombinedMapReduceMapOutputKeyWritable, LongWritable> {

    private final CombinedMapReduceMapOutputKeyWritable keyout = new CombinedMapReduceMapOutputKeyWritable();
    {
        keyout.keyType = KeyType.WORD_COUNT_V2;
    }

    private final LongWritable valueout = new LongWritable(1L);

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for(String word : value.toString().toLowerCase().split("\\W+")) {
            if(!word.isEmpty()) {
                keyout.wordCountV2Key.set(word);
                context.write(keyout, valueout);
            }
        }
    }

}
