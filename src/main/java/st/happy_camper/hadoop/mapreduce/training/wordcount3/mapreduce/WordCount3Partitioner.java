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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import st.happy_camper.hadoop.mapreduce.training.wordcount3.io.WordCount3MapOutputKeyWritable;

/**
 * @author ueshin
 */
public class WordCount3Partitioner extends Partitioner<WordCount3MapOutputKeyWritable, Text> {

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object,
     * java.lang.Object, int)
     */
    @Override
    public int getPartition(WordCount3MapOutputKeyWritable key, Text value, int numPartitions) {
        return key.codePoint % numPartitions;
    }

}
