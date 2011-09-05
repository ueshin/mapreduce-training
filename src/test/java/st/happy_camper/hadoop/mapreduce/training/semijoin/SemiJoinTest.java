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
package st.happy_camper.hadoop.mapreduce.training.semijoin;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

/**
 * @author ueshin
 */
public class SemiJoinTest extends ClusterMapReduceTestCase {

    static {
        System.setProperty("hadoop.log.dir", "target/log");
    }

    /**
     * @throws Exception
     */
    @Test
    public void testSemiJoin() throws Exception {
        FileSystem fs = getFileSystem();
        URI idsFileUri = new URI("ids.txt#ids");
        URI joinFileUri = new URI("links.txt#links");
        fs.copyFromLocalFile(new Path("src/test/resources/ids.txt"), new Path(idsFileUri));
        fs.copyFromLocalFile(new Path("src/test/resources/links.txt"), new Path(joinFileUri));
        fs.copyFromLocalFile(new Path("src/test/resources/hadoopwiki.txt"), getInputDir());

        Job job = new SemiJoin(createJobConf()).createJob(idsFileUri, joinFileUri, new Path[] { getInputDir() },
                getOutputDir());
        job.waitForCompletion(true);
        assertThat(job.isSuccessful(), is(true));

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(fs.open(new Path(getOutputDir(), "part-r-00000")),
                    Charset.forName("utf-8")));
            assertThat(reader.readLine(), is("Apache\thttp://www.apache.org/\t2"));
            assertThat(reader.readLine(), is("GFS\thttp://dev.ariel-networks.com/column/tech/google_file_system/\t1"));
            assertThat(reader.readLine(), is("Google\thttp://www.google.co.jp/\t2"));
            assertThat(reader.readLine(), is("Hadoop\thttp://hadoop.apache.org/\t5"));
            assertThat(reader.readLine(), is("Java\thttp://www.oracle.com/technetwork/java/index.html\t1"));
            assertThat(reader.readLine(), is("MapReduce\thttp://ja.wikipedia.org/wiki/MapReduce\t1"));
            assertThat(reader.readLine(), is("Nutch\thttp://nutch.apache.org/\t1"));
            assertThat(reader.readLine(), is("Yahoo\thttp://www.yahoo.co.jp/\t1"));
            assertThat(reader.readLine(), is("applications\thttp://en.wikipedia.org/wiki/Application_software\t2"));
            assertThat(reader.readLine(), is("community\thttp://en.wikipedia.org/wiki/Community\t1"));
            assertThat(reader.readLine(), is("contributor\thttp://ejje.weblio.jp/content/contributor\t1"));
            assertThat(reader.readLine(), is("data-intensive\thttp://gihyo.jp/dev/clip/01/orangenews/vol56/0004\t1"));
            assertThat(reader.readLine(), is("distributed\thttp://e-words.jp/w/E58886E695A3E587A6E79086.html\t1"));
            assertThat(reader.readLine(), is("elephant\thttp://www.squidoo.com/apache-hadoop\t1"));
            assertThat(reader.readLine(),
                    is("framework\thttp://e-words.jp/w/E38395E383ACE383BCE383A0E383AFE383BCE382AF.html\t1"));
            assertThat(reader.readLine(), is("license\thttp://e-words.jp/w/E383A9E382A4E382BBE383B3E382B9.html\t1"));
            assertThat(reader.readLine(), is("programming\thttp://www.hyuki.com/\t1"));
            assertThat(
                    reader.readLine(),
                    is("project\thttp://ja.wikipedia.org/wiki/%E3%83%97%E3%83%AD%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88\t3"));
            assertThat(reader.readLine(), is("software\thttp://en.wikipedia.org/wiki/Computer_software\t1"));
            assertThat(reader.readLine(),
                    is("top-level\thttp://lucene.472066.n3.nabble.com/ANNOUNCEMENT-Hadoop-is-a-TLP-td673806.html\t1"));
            assertThat(reader.readLine(), nullValue());
        }
        finally {
            if(reader != null) {
                reader.close();
            }
        }
    }

}
