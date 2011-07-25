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
package st.happy_camper.hadoop.mapreduce.training.charcount1.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author ueshin
 */
public class CharCount1OutputKeyWritable implements WritableComparable<CharCount1OutputKeyWritable> {

    public String taskAttemptId;

    public long offset;

    public int codePoint;

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, taskAttemptId);
        WritableUtils.writeVLong(out, offset);
        WritableUtils.writeVInt(out, codePoint);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        taskAttemptId = Text.readString(in);
        offset = WritableUtils.readVLong(in);
        codePoint = WritableUtils.readVInt(in);
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(CharCount1OutputKeyWritable o) {
        int cmp = taskAttemptId.compareTo(o.taskAttemptId);
        if(cmp == 0) {
            cmp = Long.valueOf(offset).compareTo(o.offset);
        }
        if(cmp == 0) {
            cmp = Integer.valueOf(codePoint).compareTo(o.codePoint);
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
        result = prime * result + (int) (offset ^ (offset >>> 32));
        result = prime * result + ((taskAttemptId == null) ? 0 : taskAttemptId.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return taskAttemptId + ":" + offset + "\t" + String.valueOf(Character.toChars(codePoint));
    }

}
