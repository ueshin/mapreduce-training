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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * @author ueshin
 */
public class WordCount2MapperTest {

    private MapDriver<LongWritable, Text, Text, LongWritable> driver;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        driver = new MapDriver<LongWritable, Text, Text, LongWritable>(new WordCount2Mapper());
    }

    /**
     * Test method for
     * {@link st.happy_camper.hadoop.mapreduce.training.wordcount1.mapreduce.WordCount2Mapper#map(org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context)}
     * .
     */
    @Test
    public void testMapLongWritableTextContext() {
        driver.withInput(new LongWritable(0L), new Text("Hello World"))
                .withOutput(new Text("Hello"), new LongWritable(1L))
                .withOutput(new Text("World"), new LongWritable(1L)).runTest();
    }

}
