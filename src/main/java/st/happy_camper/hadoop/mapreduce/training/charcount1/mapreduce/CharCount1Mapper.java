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
package st.happy_camper.hadoop.mapreduce.training.charcount1.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import st.happy_camper.hadoop.mapreduce.training.charcount1.io.CharCount1OutputKeyWritable;

/**
 * @author ueshin
 */
public class CharCount1Mapper extends Mapper<LongWritable, Text, CharCount1OutputKeyWritable, LongWritable> {

    private final CharCount1OutputKeyWritable keyout = new CharCount1OutputKeyWritable();

    private final LongWritable valueout = new LongWritable(1L);

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
     * Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        keyout.taskAttemptId = context.getTaskAttemptID().toString();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        keyout.offset = key.get();

        String txt = value.toString().replaceAll("\\s", "");
        for(int i = 0; i < txt.length(); i++) {
            keyout.codePoint = txt.codePointAt(Character.isHighSurrogate(txt.charAt(i)) ? i++ : i);
            context.write(keyout, valueout);
        }
    }

}
