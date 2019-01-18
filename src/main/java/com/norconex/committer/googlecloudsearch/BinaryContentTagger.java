/*
 * Copyright © 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

package com.norconex.committer.googlecloudsearch;

import com.google.common.io.ByteStreams;
import com.norconex.importer.doc.ImporterMetadata;
import com.norconex.importer.handler.ImporterHandlerException;
import com.norconex.importer.handler.tagger.IDocumentTagger;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Base64;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Provides access to the unparsed document content for the
 * {@code GoogleCloudSearchCommitter} when using the {@code raw} upload format.
 *
 * <h3>XML configuration usage</h3>
 *
 * <pre>
 *  &lt;tagger class="com.norconex.committer.googlecloudsearch.BinaryContentTagger"/&gt;
 * </pre>
 */
public class BinaryContentTagger implements IDocumentTagger {
  private static final Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void tagDocument(
      String reference, InputStream document, ImporterMetadata metadata, boolean parsed)
      throws ImporterHandlerException {
    if (parsed) {
      String errMsg =
          "Document is already parsed. Please make sure the <tagger ... /> entry is inside the"
              + " <preParseHandlers> list!";
      throw new ImporterHandlerException(errMsg);
    }
    addContentInBase64(document, metadata);
  }

  private void addContentInBase64(InputStream document, ImporterMetadata metadata)
      throws ImporterHandlerException {
    try {
      byte[] content = ByteStreams.toByteArray(document);
      metadata.addString(
          GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT,
          Base64.getEncoder().encodeToString(content));
    } catch (IOException e) {
      throw new ImporterHandlerException(e);
    }
  }
}
