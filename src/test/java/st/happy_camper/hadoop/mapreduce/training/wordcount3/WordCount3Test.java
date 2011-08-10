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
package st.happy_camper.hadoop.mapreduce.training.wordcount3;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

/**
 * @author ueshin
 */
public class WordCount3Test extends ClusterMapReduceTestCase {

    static {
        System.setProperty("hadoop.log.dir", "target/log");
    }

    /**
     * @throws Exception
     */
    @Test
    public void testWordCount3() throws Exception {
        FileSystem fs = getFileSystem();
        fs.copyFromLocalFile(new Path("src/test/resources/hadoopwiki.txt"), getInputDir());

        Job job = new WordCount3(createJobConf()).createJob(new Path[] { getInputDir() }, getOutputDir());
        job.waitForCompletion(true);
        assertThat(job.isSuccessful(), is(true));

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(fs.open(new Path(getOutputDir(), "part-r-00000")),
                    Charset.forName("utf-8")));
            assertThat(reader.readLine(), is("1\t1\t1"));
            assertThat(reader.readLine(), is("2\t1\t1"));
            assertThat(reader.readLine(), is("3\t1\t1"));
            assertThat(reader.readLine(), is("4\t1\t1"));
            assertThat(reader.readLine(), is("5\t1\t1"));
            assertThat(reader.readLine(), is("6\t1\t1"));
            assertThat(reader.readLine(), is("7\t1\t1"));
            assertThat(reader.readLine(), is("a\t6\t14"));
            assertThat(reader.readLine(), is("b\t5\t7"));
            assertThat(reader.readLine(), is("c\t5\t5"));
            assertThat(reader.readLine(), is("d\t5\t6"));
            assertThat(reader.readLine(), is("e\t4\t4"));
            assertThat(reader.readLine(), is("f\t4\t4"));
            assertThat(reader.readLine(), is("g\t3\t4"));
            assertThat(reader.readLine(), is("h\t3\t7"));
            assertThat(reader.readLine(), is("i\t5\t8"));
            assertThat(reader.readLine(), is("j\t1\t1"));
            assertThat(reader.readLine(), is("l\t4\t4"));
            assertThat(reader.readLine(), is("m\t1\t1"));
            assertThat(reader.readLine(), is("n\t3\t3"));
            assertThat(reader.readLine(), is("o\t2\t4"));
            assertThat(reader.readLine(), is("p\t4\t6"));
            assertThat(reader.readLine(), is("s\t7\t8"));
            assertThat(reader.readLine(), is("t\t6\t11"));
            assertThat(reader.readLine(), is("u\t4\t4"));
            assertThat(reader.readLine(), is("w\t4\t6"));
            assertThat(reader.readLine(), is("y\t1\t1"));
            assertThat(reader.readLine(), nullValue());
        }
        finally {
            if(reader != null) {
                reader.close();
            }
        }
    }

}
