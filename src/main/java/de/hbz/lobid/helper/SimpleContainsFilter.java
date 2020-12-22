/*
 * Copyright 2013, 2014 Deutsche Nationalbibliothek
 *
 * Licensed under the Apache License, Version 2.0 the "License";
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.hbz.lobid.helper;

import java.util.Collection;

import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;
import org.metafacture.flowcontrol.StreamBuffer;
import org.metafacture.framework.FluxCommand;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.annotations.In;
import org.metafacture.framework.annotations.Out;
import org.metafacture.framework.helpers.DefaultStreamPipe;

/**
 * Simpler than {@link org.metafacture.metamorph.Filter} it's tested if a String is contained in a stream based on a morph definition. A record is accepted if the
 * morph returns at least one non empty value.
 *
 * @author Markus Michael Geipel
 *
 */
@Description("Filters a stream based on a morph definition. A record is accepted if the morph returns at least one non empty value.")
@In(StreamReceiver.class)
@Out(StreamReceiver.class)
@FluxCommand("filter")
public final class SimpleContainsFilter extends DefaultStreamPipe<StreamReceiver> {

    private final StreamBuffer buffer = new StreamBuffer();
private String stringContainedInInput;
private boolean exists;

Trie trie;


    public SimpleContainsFilter(final String containing) {
  trie = Trie.builder().onlyWholeWords().addKeywords(containing.split(" ")).build();
 
  this.stringContainedInInput=containing;
    }

    @Override
    protected void onSetReceiver() {
        buffer.setReceiver(getReceiver());
    }


    private void dispatch(){
        
        if(exists){
            buffer.replay();
            exists = false;
        }
        buffer.clear();
    }

    @Override
    public void startRecord(final String identifier) {
        buffer.startRecord(identifier);
    }

    @Override
    public void endRecord() {
        buffer.endRecord();
        dispatch();
    }

    @Override
    public void startEntity(final String name) {
        buffer.startEntity(name);
    }

    @Override
    public void endEntity() {
        buffer.endEntity();
    }

    @Override
    public void literal(final String name, final String value) {
      // if (value.contains(stringContainedInInput)){
      //   exists=true;
      // }

      Collection<Emit> emits = trie.parseText(value);
      if (!emits.isEmpty()) {
      exists=true;
      }
     // Arrays.toString(emits.toArray()).contains(word);
        buffer.literal(name, value);

    }

    @Override
    protected void onResetStream() {
        buffer.clear();
        getReceiver().resetStream();
    }

    @Override
    protected void onCloseStream() {
        buffer.clear();
        getReceiver().closeStream();
    }
}
