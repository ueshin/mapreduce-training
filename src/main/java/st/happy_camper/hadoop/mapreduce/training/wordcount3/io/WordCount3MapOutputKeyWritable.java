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
package st.happy_camper.hadoop.mapreduce.training.wordcount3.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author ueshin
 */
public class WordCount3MapOutputKeyWritable implements WritableComparable<WordCount3MapOutputKeyWritable> {

    public int codePoint;

    public String word;

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, codePoint);
        Text.writeString(out, word);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        codePoint = WritableUtils.readVInt(in);
        word = Text.readString(in);
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(WordCount3MapOutputKeyWritable o) {
        int cmp = Integer.valueOf(codePoint).compareTo(o.codePoint);
        if(cmp == 0) {
            cmp = word.compareTo(o.word);
        }
        return cmp;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + codePoint;
        result = prime * result + ((word == null) ? 0 : word.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        WordCount3MapOutputKeyWritable other = (WordCount3MapOutputKeyWritable) obj;
        if(codePoint != other.codePoint)
            return false;
        if(word == null) {
            if(other.word != null)
                return false;
        }
        else if(!word.equals(other.word))
            return false;
        return true;
    }

}
