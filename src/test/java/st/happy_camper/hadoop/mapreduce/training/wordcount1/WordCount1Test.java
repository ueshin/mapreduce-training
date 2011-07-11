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
package st.happy_camper.hadoop.mapreduce.training.wordcount1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import st.happy_camper.hadoop.mapreduce.training.wordcount1.mapreduce.WordCount1Mapper;
import st.happy_camper.hadoop.mapreduce.training.wordcount1.mapreduce.WordCount1Reducer;

/**
 * @author ueshin
 */
public class WordCount1Test {

    private MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> driver;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        driver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>(
                new WordCount1Mapper(), new WordCount1Reducer());
    }

    @Test
    public void test() {
        driver.withInput(new LongWritable(0), new Text("Hello World!")).withInput(new LongWritable(10L), new Text(""))
                .withInput(new LongWritable(11L), new Text("World Count."))
                .withOutput(new Text("Count"), new LongWritable(1)).withOutput(new Text("Hello"), new LongWritable(1))
                .withOutput(new Text("World"), new LongWritable(2)).runTest();
    }

}
