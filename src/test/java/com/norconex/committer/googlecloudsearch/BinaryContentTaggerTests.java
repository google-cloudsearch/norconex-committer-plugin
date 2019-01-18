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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.norconex.importer.doc.ImporterMetadata;
import com.norconex.importer.handler.ImporterHandlerException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class BinaryContentTaggerTests {

  private static final String URL = "http://x.yz/abc";
  private static final String CONTENT = "Test1234567890";
  private static final String CONTENT_BASE64 = "VGVzdDEyMzQ1Njc4OTA=";
  private static final boolean PARSED = true;

  @Rule public ExpectedException thrown = ExpectedException.none();

  private BinaryContentTagger subject;

  @Before
  public void setUp() {
    subject = new BinaryContentTagger();
  }

  @Test
  public void tagDocumentShouldFailIfDocumentIsAlreadyParsed() throws ImporterHandlerException {
    thrown.expect(ImporterHandlerException.class);
    thrown.expectMessage(
        "Document is already parsed. Please make sure the <tagger ... /> entry is inside the"
            + " <preParseHandlers> list!");
    subject.tagDocument(
        URL, new ByteArrayInputStream(CONTENT.getBytes(StandardCharsets.UTF_8)), null, PARSED);
  }

  @Test
  public void tagDocumentShouldFailOnContentReadException() throws Exception {
    InputStream contentStream = mock(InputStream.class);
    when(contentStream.read(any()))
        .thenThrow(new IOException("Error when reading content stream!"));
    thrown.expect(ImporterHandlerException.class);
    thrown.expectMessage("Error when reading content stream!");
    subject.tagDocument(URL, contentStream, null, !PARSED);
  }

  @Test
  public void tagDocumentSuccessful() throws Exception {
    ImporterMetadata metadata = new ImporterMetadata();
    subject.tagDocument(
        URL, new ByteArrayInputStream(CONTENT.getBytes(StandardCharsets.UTF_8)), metadata, !PARSED);
    assertEquals(
        CONTENT_BASE64, metadata.getString(GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT));
  }
}
