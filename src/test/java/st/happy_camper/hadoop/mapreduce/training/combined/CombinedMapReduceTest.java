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
package st.happy_camper.hadoop.mapreduce.training.combined;

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
public class CombinedMapReduceTest extends ClusterMapReduceTestCase {

    static {
        System.setProperty("hadoop.log.dir", "target/log");
    }

    /**
     * @throws Exception
     */
    @Test
    public void testCombinedMapReduce() throws Exception {
        FileSystem fs = getFileSystem();
        fs.mkdirs(getInputDir());

        Path v1Input = new Path(getInputDir(), "wordcountV1.txt");
        fs.copyFromLocalFile(new Path("src/test/resources/hadoopwiki.txt"), v1Input);

        Path v2Input = new Path(getInputDir(), "wordcountV2.txt");
        fs.copyFromLocalFile(new Path("src/test/resources/hadoopwiki.txt"), v2Input);

        URI idsFileUri = new URI("ids.txt#ids");
        URI joinFileUri = new URI("links.txt#links");
        Path semiJoinInput = new Path(getInputDir(), "wordcountSemiJoin.txt");
        fs.copyFromLocalFile(new Path("src/test/resources/ids.txt"), new Path(idsFileUri));
        fs.copyFromLocalFile(new Path("src/test/resources/links.txt"), new Path(joinFileUri));
        fs.copyFromLocalFile(new Path("src/test/resources/hadoopwiki.txt"), semiJoinInput);

        Job job = new CombinedMapReduce(createJobConf()).createJob(new Path[] { v1Input }, new Path[] { v2Input },
                idsFileUri, joinFileUri, new Path[] { semiJoinInput }, getOutputDir());
        job.waitForCompletion(true);
        assertThat(job.isSuccessful(), is(true));

        {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(getOutputDir(), "part-r-00000")),
                        Charset.forName("utf-8")));
                assertThat(reader.readLine(), nullValue());
            }
            finally {
                if(reader != null) {
                    reader.close();
                }
            }
        }
        {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(getOutputDir(), "part-r-00001")),
                        Charset.forName("utf-8")));
                assertThat(reader.readLine(), nullValue());
            }
            finally {
                if(reader != null) {
                    reader.close();
                }
            }
        }

        {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(getOutputDir(), "v1-r-00000")),
                        Charset.forName("utf-8")));
                assertThat(reader.readLine(), is("1\t1"));
                assertThat(reader.readLine(), is("3\t1"));
                assertThat(reader.readLine(), is("5\t1"));
                assertThat(reader.readLine(), is("7\t1"));
                assertThat(reader.readLine(), is("Doug\t1"));
                assertThat(reader.readLine(), is("Google\t2"));
                assertThat(reader.readLine(), is("Hadoop\t5"));
                assertThat(reader.readLine(), is("It\t2"));
                assertThat(reader.readLine(), is("System\t1"));
                assertThat(reader.readLine(), is("a\t4"));
                assertThat(reader.readLine(), is("across\t1"));
                assertThat(reader.readLine(), is("and\t4"));
                assertThat(reader.readLine(), is("applications\t2"));
                assertThat(reader.readLine(), is("being\t1"));
                assertThat(reader.readLine(), is("by\t3"));
                assertThat(reader.readLine(), is("community\t1"));
                assertThat(reader.readLine(), is("contributor\t1"));
                assertThat(reader.readLine(), is("distributed\t1"));
                assertThat(reader.readLine(), is("elephant\t1"));
                assertThat(reader.readLine(), is("for\t1"));
                assertThat(reader.readLine(), is("global\t1"));
                assertThat(reader.readLine(), is("intensive\t1"));
                assertThat(reader.readLine(), is("it\t1"));
                assertThat(reader.readLine(), is("license\t1"));
                assertThat(reader.readLine(), is("named\t1"));
                assertThat(reader.readLine(), is("nodes\t1"));
                assertThat(reader.readLine(), is("of\t3"));
                assertThat(reader.readLine(), is("papers\t1"));
                assertThat(reader.readLine(), is("petabytes\t1"));
                assertThat(reader.readLine(), is("programming\t1"));
                assertThat(reader.readLine(), is("project\t3"));
                assertThat(reader.readLine(), is("s\t2"));
                assertThat(reader.readLine(), is("software\t1"));
                assertThat(reader.readLine(), is("support\t1"));
                assertThat(reader.readLine(), is("that\t1"));
                assertThat(reader.readLine(), is("the\t4"));
                assertThat(reader.readLine(), is("thousands\t1"));
                assertThat(reader.readLine(), is("to\t3"));
                assertThat(reader.readLine(), is("top\t1"));
                assertThat(reader.readLine(), is("used\t1"));
                assertThat(reader.readLine(), is("was\t3"));
                assertThat(reader.readLine(), is("work\t1"));
                assertThat(reader.readLine(), nullValue());
            }
            finally {
                if(reader != null) {
                    reader.close();
                }
            }
        }
        {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(getOutputDir(), "v1-r-00001")),
                        Charset.forName("utf-8")));
                assertThat(reader.readLine(), is("2\t1"));
                assertThat(reader.readLine(), is("4\t1"));
                assertThat(reader.readLine(), is("6\t1"));
                assertThat(reader.readLine(), is("Apache\t2"));
                assertThat(reader.readLine(), is("Cutting\t1"));
                assertThat(reader.readLine(), is("File\t1"));
                assertThat(reader.readLine(), is("GFS\t1"));
                assertThat(reader.readLine(), is("Java\t1"));
                assertThat(reader.readLine(), is("MapReduce\t1"));
                assertThat(reader.readLine(), is("Nutch\t1"));
                assertThat(reader.readLine(), is("Yahoo\t1"));
                assertThat(reader.readLine(), is("after\t1"));
                assertThat(reader.readLine(), is("been\t1"));
                assertThat(reader.readLine(), is("built\t1"));
                assertThat(reader.readLine(), is("businesses\t1"));
                assertThat(reader.readLine(), is("contributors\t1"));
                assertThat(reader.readLine(), is("created\t1"));
                assertThat(reader.readLine(), is("data\t2"));
                assertThat(reader.readLine(), is("developed\t1"));
                assertThat(reader.readLine(), is("distribution\t1"));
                assertThat(reader.readLine(), is("enables\t1"));
                assertThat(reader.readLine(), is("engine\t1"));
                assertThat(reader.readLine(), is("extensively\t1"));
                assertThat(reader.readLine(), is("framework\t1"));
                assertThat(reader.readLine(), is("free\t1"));
                assertThat(reader.readLine(), is("has\t1"));
                assertThat(reader.readLine(), is("his\t1"));
                assertThat(reader.readLine(), is("inspired\t1"));
                assertThat(reader.readLine(), is("is\t2"));
                assertThat(reader.readLine(), is("its\t1"));
                assertThat(reader.readLine(), is("language\t1"));
                assertThat(reader.readLine(), is("largest\t1"));
                assertThat(reader.readLine(), is("level\t1"));
                assertThat(reader.readLine(), is("originally\t1"));
                assertThat(reader.readLine(), is("search\t1"));
                assertThat(reader.readLine(), is("son\t1"));
                assertThat(reader.readLine(), is("supports\t1"));
                assertThat(reader.readLine(), is("toy\t1"));
                assertThat(reader.readLine(), is("under\t1"));
                assertThat(reader.readLine(), is("uses\t1"));
                assertThat(reader.readLine(), is("using\t1"));
                assertThat(reader.readLine(), is("who\t1"));
                assertThat(reader.readLine(), is("with\t1"));
                assertThat(reader.readLine(), nullValue());
            }
            finally {
                if(reader != null) {
                    reader.close();
                }
            }
        }

        {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(getOutputDir(), "v2-r-00000")),
                        Charset.forName("utf-8")));
                assertThat(reader.readLine(), is("2\t1\t1"));
                assertThat(reader.readLine(), is("4\t1\t1"));
                assertThat(reader.readLine(), is("6\t1\t1"));
                assertThat(reader.readLine(), is("b\t5\t7"));
                assertThat(reader.readLine(), is("d\t5\t6"));
                assertThat(reader.readLine(), is("f\t4\t4"));
                assertThat(reader.readLine(), is("h\t3\t7"));
                assertThat(reader.readLine(), is("j\t1\t1"));
                assertThat(reader.readLine(), is("l\t4\t4"));
                assertThat(reader.readLine(), is("n\t3\t3"));
                assertThat(reader.readLine(), is("p\t4\t6"));
                assertThat(reader.readLine(), is("t\t6\t11"));
                assertThat(reader.readLine(), nullValue());
            }
            finally {
                if(reader != null) {
                    reader.close();
                }
            }
        }
        {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(fs.open(new Path(getOutputDir(), "v2-r-00001")),
                        Charset.forName("utf-8")));
                assertThat(reader.readLine(), is("1\t1\t1"));
                assertThat(reader.readLine(), is("3\t1\t1"));
                assertThat(reader.readLine(), is("5\t1\t1"));
                assertThat(reader.readLine(), is("7\t1\t1"));
                assertThat(reader.readLine(), is("a\t6\t14"));
                assertThat(reader.readLine(), is("c\t5\t5"));
                assertThat(reader.readLine(), is("e\t4\t4"));
                assertThat(reader.readLine(), is("g\t3\t4"));
                assertThat(reader.readLine(), is("i\t5\t8"));
                assertThat(reader.readLine(), is("m\t1\t1"));
                assertThat(reader.readLine(), is("o\t2\t4"));
                assertThat(reader.readLine(), is("s\t7\t8"));
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

        {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(
                        fs.open(new Path(getOutputDir(), "semijoin-r-00000")), Charset.forName("utf-8")));
                assertThat(reader.readLine(), is("Google\thttp://www.google.co.jp/\t2"));
                assertThat(reader.readLine(), is("Hadoop\thttp://hadoop.apache.org/\t5"));
                assertThat(reader.readLine(), is("applications\thttp://en.wikipedia.org/wiki/Application_software\t2"));
                assertThat(reader.readLine(), is("community\thttp://en.wikipedia.org/wiki/Community\t1"));
                assertThat(reader.readLine(), is("contributor\thttp://ejje.weblio.jp/content/contributor\t1"));
                assertThat(reader.readLine(), is("distributed\thttp://e-words.jp/w/E58886E695A3E587A6E79086.html\t1"));
                assertThat(reader.readLine(), is("elephant\thttp://www.squidoo.com/apache-hadoop\t1"));
                assertThat(reader.readLine(), is("license\thttp://e-words.jp/w/E383A9E382A4E382BBE383B3E382B9.html\t1"));
                assertThat(reader.readLine(), is("programming\thttp://www.hyuki.com/\t1"));
                assertThat(
                        reader.readLine(),
                        is("project\thttp://ja.wikipedia.org/wiki/%E3%83%97%E3%83%AD%E3%82%B8%E3%82%A7%E3%82%AF%E3%83%88\t3"));
                assertThat(reader.readLine(), is("software\thttp://en.wikipedia.org/wiki/Computer_software\t1"));
                assertThat(reader.readLine(), nullValue());
            }
            finally {
                if(reader != null) {
                    reader.close();
                }
            }
        }
        {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(
                        fs.open(new Path(getOutputDir(), "semijoin-r-00001")), Charset.forName("utf-8")));
                assertThat(reader.readLine(), is("Apache\thttp://www.apache.org/\t2"));
                assertThat(reader.readLine(),
                        is("GFS\thttp://dev.ariel-networks.com/column/tech/google_file_system/\t1"));
                assertThat(reader.readLine(), is("Java\thttp://www.oracle.com/technetwork/java/index.html\t1"));
                assertThat(reader.readLine(), is("MapReduce\thttp://ja.wikipedia.org/wiki/MapReduce\t1"));
                assertThat(reader.readLine(), is("Nutch\thttp://nutch.apache.org/\t1"));
                assertThat(reader.readLine(), is("Yahoo\thttp://www.yahoo.co.jp/\t1"));
                assertThat(reader.readLine(),
                        is("data-intensive\thttp://gihyo.jp/dev/clip/01/orangenews/vol56/0004\t1"));
                assertThat(reader.readLine(),
                        is("framework\thttp://e-words.jp/w/E38395E383ACE383BCE383A0E383AFE383BCE382AF.html\t1"));
                assertThat(
                        reader.readLine(),
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

}
