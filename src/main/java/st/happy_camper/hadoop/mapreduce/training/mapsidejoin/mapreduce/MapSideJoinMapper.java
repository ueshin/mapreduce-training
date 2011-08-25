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
package st.happy_camper.hadoop.mapreduce.training.mapsidejoin.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import st.happy_camper.hadoop.mapreduce.training.mapsidejoin.MapSideJoin;

/**
 * @author ueshin
 */
public class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text keyout = new Text();

    private final LongWritable valueout = new LongWritable(1L);

    private Map<String, String> joinMap;

    /*
     * (non-Javadoc)
     * @see
     * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
     * Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String joinFile = context.getConfiguration().get(MapSideJoin.class.getName() + ".joinFile");

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(joinFile), "utf-8"));

            Map<String, String> joinMap = new HashMap<String, String>();
            String line;
            while((line = reader.readLine()) != null) {
                String[] split = line.split("\t");
                joinMap.put(split[0], split[1]);
            }
            this.joinMap = Collections.unmodifiableMap(joinMap);
        }
        finally {
            if(reader != null) {
                reader.close();
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for(String word : value.toString().split("[^\\w-]+")) {
            if(joinMap.get(word) != null) {
                keyout.set(word + "\t" + joinMap.get(word));
                context.write(keyout, valueout);
            }
        }
    }

}
