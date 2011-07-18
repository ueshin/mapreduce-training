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
package st.happy_camper.hadoop.mapreduce.training.wordcount2.mapreduce;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * @author ueshin
 */
public class WordCount2ReducerTest {

    private ReduceDriver<Text, LongWritable, Text, LongWritable> driver;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        driver = new ReduceDriver<Text, LongWritable, Text, LongWritable>(new WordCount2Reducer());
    }

    /**
     * Test method for
     * {@link st.happy_camper.hadoop.mapreduce.training.wordcount2.mapreduce.WordCount2Reducer#reduce(org.apache.hadoop.io.Text, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)}
     * .
     */
    @Test
    public void testReduceTextIterableOfLongWritableContext() {
        driver.withInput(new Text("Word"), Arrays.asList(new LongWritable(1L), new LongWritable(1L)))
                .withOutput(new Text("Word"), new LongWritable(2L)).runTest();
    }

}
