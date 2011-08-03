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
package st.happy_camper.hadoop.mapreduce.training.wordcount3.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import st.happy_camper.hadoop.mapreduce.training.wordcount3.io.WordCount3MapOutputKeyWritable;
import st.happy_camper.hadoop.mapreduce.training.wordcount3.io.WordCount3OutputValueWritable;

/**
 * @author ueshin
 */
public class WordCount3Reducer extends
        Reducer<WordCount3MapOutputKeyWritable, Text, Text, WordCount3OutputValueWritable> {

    private final Text keyout = new Text();

    private final WordCount3OutputValueWritable valueout = new WordCount3OutputValueWritable();

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(WordCount3MapOutputKeyWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        keyout.set(new String(new int[] { key.codePoint }, 0, 1));

        long words = 0L;
        long count = 0L;
        String current = null;
        for(Text text : values) {
            count++;

            String word = text.toString();
            if(!word.equals(current)) {
                words++;
                current = word;
            }
        }
        valueout.words = words;
        valueout.count = count;

        context.write(keyout, valueout);
    }

}
