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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.atLeastOnce;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.util.DateTime;
import com.google.api.services.cloudsearch.v1.model.BooleanPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.NamedProperty;
import com.google.api.services.cloudsearch.v1.model.ObjectDefinition;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PropertyDefinition;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.StructuredDataObject;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.committer.googlecloudsearch.GoogleCloudSearchCommitter.Helper;
import com.norconex.committer.googlecloudsearch.GoogleCloudSearchCommitter.UploadFormat;
import com.norconex.commons.lang.map.Properties;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.output.XmlStreamWriter;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;

/** Tests for GoogleCloudSearchCommitter. */
@RunWith(MockitoJUnitRunner.class)
public class GoogleCloudSearchCommitterTests {
  private static final String CONTENT = "Test1234567890";
  private static final String CONTENT_BASE64 = "VGVzdDEyMzQ1Njc4OTA=";
  private static final String URL = "http://x.yz/abc";
  private static final String MIME_TEXT = "text/plain";
  private static final String MIME_PDF = "text/pdf";
  private static final long CURRENT_MILLIS = 123456789;
  private static final boolean APPLY_DOMAIN_ACLS = true;
  private static final boolean CONTENT_ERROR = true;

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();

  @Mock private Helper mockHelper;
  @Mock private XMLConfiguration mockConfig;
  @Mock private XMLStreamWriter mockConfigWriter;
  @Mock private DefaultAcl mockDefaultAcl;
  @Mock private Appender appender;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private IndexingService mockIndexingService;

  @Captor private ArgumentCaptor<AbstractInputStreamContent> itemContentCaptor;
  @Captor private ArgumentCaptor<Item> itemCaptor;
  @Captor private ArgumentCaptor<LoggingEvent> loggingCaptor;

  private GoogleCloudSearchCommitter subject;

  @Before
  public void setUp() throws Exception {
    LogManager.getRootLogger().addAppender(appender);
    when(mockHelper.createIndexingService()).thenReturn(mockIndexingService);
    when(mockHelper.initDefaultAclFromConfig(mockIndexingService)).thenReturn(mockDefaultAcl);
    when(mockHelper.getCurrentTimeMillis()).thenReturn(CURRENT_MILLIS);
    when(mockHelper.isConfigInitialized()).thenReturn(true);
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_CONFIG_FILE), any()))
        .thenReturn("/path/to/config");
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("RAW");
    when(mockDefaultAcl.applyToIfEnabled(any())).thenReturn(true);
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    subject = new GoogleCloudSearchCommitter(mockHelper);
  }

  @After
  public void teardown() {
    LogManager.getRootLogger().removeAppender(appender);
  }

  @Test
  public void verifyConstantValues() {
    assertEquals(GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT, "binaryContent");
    assertEquals(GoogleCloudSearchCommitter.FIELD_CONTENT_TYPE, "document.contentType");
    assertEquals(GoogleCloudSearchCommitter.CONFIG_KEY_CONFIG_FILE, "configFilePath");
    assertEquals(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT, "uploadFormat");
  }

  @Test
  public void fullLifecycle() throws IOException, GeneralSecurityException, XMLStreamException {
    setupConfig.initConfig(new java.util.Properties());
    when(mockHelper.isConfigInitialized()).thenReturn(false);
    when(mockIndexingService.isRunning()).thenReturn(true);
    subject.loadFromXml(mockConfig);
    subject.commitBatch(
        Arrays.asList(
            addOperation(URL, MIME_PDF, CONTENT_BASE64, !CONTENT_ERROR),
            deleteOperation(URL),
            addOperation(URL, MIME_TEXT, CONTENT_BASE64, !CONTENT_ERROR)));
    subject.commitComplete();
    subject.saveToXML(mockConfigWriter);
    InOrder inOrder = Mockito.inOrder(mockHelper, mockIndexingService);
    inOrder.verify(mockHelper).isConfigInitialized();
    inOrder.verify(mockHelper).initConfig(any());
    inOrder.verify(mockHelper).createIndexingService();
    inOrder.verify(mockIndexingService).startAsync();
    inOrder.verify(mockHelper).initDefaultAclFromConfig(mockIndexingService);
    inOrder
        .verify(mockIndexingService, times(1))
        .indexItemAndContent(any(), any(), any(), any(), any());
    inOrder.verify(mockIndexingService).deleteItem(any(), any(), any());
    inOrder
        .verify(mockIndexingService, times(1))
        .indexItemAndContent(any(), any(), any(), any(), any());
    inOrder.verify(mockIndexingService).isRunning();
    inOrder.verify(mockIndexingService).stopAsync();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void initShouldInitializeConfig() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    when(mockHelper.isConfigInitialized()).thenReturn(false);
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
    verify(mockHelper, times(1)).isConfigInitialized();
    verify(mockHelper).initConfig(new String[] {"-Dconfig=/path/to/config"});
  }

  @Test
  public void initShouldNotReinitializeConfigWhenAlreadyInitialized() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
    verify(mockHelper, times(1)).isConfigInitialized();
    verify(mockHelper, times(0)).initConfig(any());
  }

  @Test
  public void initShouldFailWhenSDKConfigInitializationFailsWithIOException() throws Exception {
    when(mockHelper.isConfigInitialized()).thenReturn(false);
    doThrow(new IOException()).when(mockHelper).initConfig(any());
    thrown.expect(CommitterException.class);
    thrown.expectMessage("Initialization of SDK configuration failed.");
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
  }

  @Test
  public void loadFromXmlShouldFailWithmissingConfigPath() {
    when(mockConfig.getString(GoogleCloudSearchCommitter.CONFIG_KEY_CONFIG_FILE, null))
        .thenReturn(null);
    thrown.expect(CommitterException.class);
    thrown.expectMessage(
        "Missing required plugin configuration entry: "
            + GoogleCloudSearchCommitter.CONFIG_KEY_CONFIG_FILE);
    subject.loadFromXml(mockConfig);
  }

  @Test
  public void initShouldCreateIndexingService() throws Exception {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
    verify(mockHelper).createIndexingService();
  }

  @Test
  public void initShouldFailWhenCreatingIndexingServiceFailsWithSecurityException()
      throws Exception {
    when(mockHelper.createIndexingService()).thenThrow(new GeneralSecurityException());
    thrown.expect(CommitterException.class);
    thrown.expectMessage("failed to create IndexingService");
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
  }

  @Test
  public void initShouldFailWhenCreatingIndexingServiceFailsWithIOException() throws Exception {
    when(mockHelper.createIndexingService()).thenThrow(new IOException());
    thrown.expect(CommitterException.class);
    thrown.expectMessage("failed to create IndexingService");
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
  }

  @Test
  public void loadFromXmlShouldFailWhenUploadFormatHasInvalidValue() {
    when(mockConfig.getString(
            GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT, UploadFormat.RAW.name()))
        .thenReturn("Invalid_Value");
    thrown.expect(CommitterException.class);
    thrown.expectMessage(
        "Unknown value for '" + GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT + "'");
    subject.loadFromXml(mockConfig);
  }

  @Test
  public void initShouldStartIndexingService() {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
    verify(mockIndexingService.startAsync()).awaitRunning();
  }

  @Test
  public void addShouldFailWhenRawUploadModeIsSelectedAndBinaryContentIsNotInValidBase64() {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, "Inv@lid", !CONTENT_ERROR)));
    verify(appender, atLeastOnce()).doAppend(loggingCaptor.capture());
    List<LoggingEvent> loggingEvents = loggingCaptor.getAllValues();
    assertEquals("Exception caught while committing: " + URL, loggingEvents.get(4).getRenderedMessage());
    assertEquals(
      "Binary content field is missing or invalid. Please configure BinaryContentTagger"
        + " and make sure the '"
        + GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT
        + "' field is left untouched (e.g. watch out for KeepOnlyTagger)", loggingEvents.get(5).getRenderedMessage());
  }

  @Test
  public void addShouldFailWhenRawUploadModeIsSelectedAndBinaryContentFieldIsMissing() {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, null, !CONTENT_ERROR)));
    verify(appender, atLeastOnce()).doAppend(loggingCaptor.capture());
    List<LoggingEvent> loggingEvents = loggingCaptor.getAllValues();
    assertEquals("Exception caught while committing: " + URL, loggingEvents.get(4).getRenderedMessage());
    assertEquals(
      "Binary content field is missing or invalid. Please configure BinaryContentTagger"
        + " and make sure the '"
        + GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT
        + "' field is left untouched (e.g. watch out for KeepOnlyTagger)", loggingEvents.get(5).getRenderedMessage());
  }

  @Test
  public void addShouldFailWhenTextUploadModeIsSelectedAndTextContentStreamThrowsException() {
    setupConfig.initConfig(new java.util.Properties());
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("Text");
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(new IAddOperation() {
      @Override
      public void delete() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getReference() {
        return URL;
      }

      @Override
      public Properties getMetadata() {
        Properties props = new Properties();
        props.addString(GoogleCloudSearchCommitter.FIELD_CONTENT_TYPE, MIME_PDF);
        return props;
      }

      @Override
      public InputStream getContentStream() throws IOException {
        throw new IOException();
      }
    }));
    verify(appender, atLeastOnce()).doAppend(loggingCaptor.capture());
    List<LoggingEvent> loggingEvents = loggingCaptor.getAllValues();
    assertEquals("Exception caught while committing: " + URL, loggingEvents.get(4).getRenderedMessage());
    assertEquals(
      "Text content ('content') field is missing, please enable the index-basic plugin!", loggingEvents.get(5).getRenderedMessage());
  }

  @Test
  public void addShouldFailWhenTextUploadModeIsSelectedAndTextContentStreamReadThrowsException()
      throws Exception {
    setupConfig.initConfig(new java.util.Properties());
    when(mockDefaultAcl.applyToIfEnabled(any())).thenReturn(false);
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("Text");
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, null, CONTENT_ERROR)));
    verify(mockIndexingService)
        .indexItemAndContent(
            eq(goldenItem(APPLY_DOMAIN_ACLS, MIME_PDF)),
            itemContentCaptor.capture(),
            eq(null),
            eq(ContentFormat.TEXT),
            eq(RequestMode.ASYNCHRONOUS));
    thrown.expect(IOException.class);
    itemContentCaptor.getValue().getInputStream().read(new byte[1024]);
  }

  @Test
  public void addShouldFailWhenContentTypeFieldIsMissing() {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    thrown.expect(CommitterException.class);
    thrown.expectMessage(
        "Content type field ('" + GoogleCloudSearchCommitter.FIELD_CONTENT_TYPE + "') is missing!");
    subject.commitBatch(Arrays.asList(addOperation(URL, null, CONTENT_BASE64, CONTENT_ERROR)));
  }

  @Test
  public void addShouldFailWhenBinaryContentIsEmpty() {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, null, !CONTENT_ERROR)));
    verify(appender, times(8)).doAppend(loggingCaptor.capture());
    List<LoggingEvent> loggingEvents = loggingCaptor.getAllValues();
    assertEquals("Exception caught while committing: " + URL, loggingEvents.get(4).getRenderedMessage());
    assertEquals(
      "Binary content field is missing or invalid. Please configure BinaryContentTagger"
        + " and make sure the '"
        + GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT
        + "' field is left untouched (e.g. watch out for KeepOnlyTagger)", loggingEvents.get(5).getRenderedMessage());
  }

  @Test
  public void addShouldNotFailWithEmptyContent() throws IOException {
    String emptyContent = "";
    setupConfig.initConfig(new java.util.Properties());
    when(mockDefaultAcl.applyToIfEnabled(any())).thenReturn(false);
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn(null);
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, emptyContent, !CONTENT_ERROR)));
    verify(mockIndexingService)
        .indexItemAndContent(
            eq(goldenItem(APPLY_DOMAIN_ACLS, MIME_PDF)),
            itemContentCaptor.capture(),
            eq(null),
            eq(ContentFormat.RAW),
            eq(RequestMode.ASYNCHRONOUS));
    assertEquals(itemContentCaptor.getValue().getType(), MIME_PDF);
    assertTrue(
    Arrays.equals(
        ByteStreams.toByteArray(itemContentCaptor.getValue().getInputStream()),
        emptyContent.getBytes()));
  }

  @Test
  public void addShouldNotFailWhenIndexingServiceThrowsIOException() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("Text");
    when(mockIndexingService.indexItemAndContent(any(), any(), any(), any(), any()))
        .thenThrow(new IOException());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_TEXT, null, !CONTENT_ERROR)));
    verify(mockIndexingService).indexItemAndContent(any(), any(), any(), any(), any());
  }

  @Test
  public void addShouldNotFailWhenAddItemThrowsRuntimeException() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("Text");
    SettableFuture<Operation> settable = SettableFuture.create();
    // It's ideal to test RuntimeException from createItem, but since we can't mock it,
    // using mockIndexingService.indexItemAndContent() to throw RuntimeException.
    when(mockIndexingService.indexItemAndContent(any(), any(), any(), any(), any()))
        .thenReturn(settable)
        .thenThrow(new RuntimeException())
        .thenReturn(settable);
    subject.loadFromXml(mockConfig);
    subject.commitBatch(
        Arrays.asList(
            addOperation(URL, MIME_PDF, CONTENT_BASE64, !CONTENT_ERROR),
            addOperation(URL, MIME_PDF, CONTENT_BASE64, !CONTENT_ERROR),
            addOperation(URL, MIME_TEXT, CONTENT_BASE64, !CONTENT_ERROR)));
    verify(mockIndexingService, times(3)).indexItemAndContent(any(), any(), any(), any(), any());
  }

  @Test
  public void addSuccessfulWithDefaultUploadFormatAndCustomerDomainAcls() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    when(mockDefaultAcl.applyToIfEnabled(any())).thenReturn(false);
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn(null);
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
    verify(mockIndexingService)
        .indexItemAndContent(
            eq(goldenItem(APPLY_DOMAIN_ACLS, MIME_PDF)),
            itemContentCaptor.capture(),
            eq(null),
            eq(ContentFormat.RAW),
            eq(RequestMode.ASYNCHRONOUS));
    assertEquals(itemContentCaptor.getValue().getType(), MIME_PDF);
    assertTrue(
        Arrays.equals(
            ByteStreams.toByteArray(itemContentCaptor.getValue().getInputStream()),
            CONTENT.getBytes()));
  }

  @Test
  public void addSuccessfulRawContent() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_PDF, CONTENT_BASE64, CONTENT_ERROR)));
    verify(mockIndexingService)
        .indexItemAndContent(
            eq(goldenItem(!APPLY_DOMAIN_ACLS, MIME_PDF)),
            itemContentCaptor.capture(),
            eq(null),
            eq(ContentFormat.RAW),
            eq(RequestMode.ASYNCHRONOUS));
    assertEquals(itemContentCaptor.getValue().getType(), MIME_PDF);
    assertTrue(
        Arrays.equals(
            ByteStreams.toByteArray(itemContentCaptor.getValue().getInputStream()),
            CONTENT.getBytes()));
  }

  @Test
  public void addSuccessfulTextContent() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("Text");
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_TEXT, null, !CONTENT_ERROR)));
    verify(mockIndexingService)
        .indexItemAndContent(
            eq(goldenItem(!APPLY_DOMAIN_ACLS, MIME_TEXT)),
            itemContentCaptor.capture(),
            eq(null),
            eq(ContentFormat.TEXT),
            eq(RequestMode.ASYNCHRONOUS));
    assertEquals(itemContentCaptor.getValue().getType(), MIME_TEXT);
    assertTrue(
        Arrays.equals(
            ByteStreams.toByteArray(itemContentCaptor.getValue().getInputStream()),
            CONTENT.getBytes()));
  }

  @Test
  public void deleteShouldFailWhenUrlIsNull() {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    thrown.expect(CommitterException.class);
    thrown.expectMessage("Delete operation failed: passed url is null or empty!");
    subject.commitBatch(Arrays.asList(deleteOperation(null)));
  }

  @Test
  public void deleteShouldFailWhenUrlIsEmpty() {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    thrown.expect(CommitterException.class);
    thrown.expectMessage("Delete operation failed: passed url is null or empty!");
    subject.commitBatch(Arrays.asList(deleteOperation("")));
  }

  @Test
  public void deleteShouldNotFailWhenIndexingServiceThrowsIOException() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    when(mockIndexingService.deleteItem(any(), any(), any()))
        .thenThrow(new IOException());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(deleteOperation(URL)));
    verify(mockIndexingService, times(1)).deleteItem(URL, Long.toString(CURRENT_MILLIS).getBytes(),
        RequestMode.ASYNCHRONOUS);
  }

  @Test
  public void deleteSuccessful() throws IOException {
    setupConfig.initConfig(new java.util.Properties());
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(deleteOperation(URL)));
    verify(mockIndexingService, times(1)).deleteItem(URL, Long.toString(CURRENT_MILLIS).getBytes(),
        RequestMode.ASYNCHRONOUS);
  }

  @Test
  public void commitBatchShouldThrowExceptionForUnsupportedOperations() {
    setupConfig.initConfig(new java.util.Properties());
    ICommitOperation unsupportedOp =
        new ICommitOperation() {
          @Override
          public void delete() {}
        };
    subject.loadFromXml(mockConfig);
    thrown.expect(CommitterException.class);
    thrown.expectMessage("Unsupported operation");
    subject.commitBatch(Arrays.asList(deleteOperation(URL), unsupportedOp));
  }

  @Test
  public void commitCompleteShouldStopIndexingService() {
    setupConfig.initConfig(new java.util.Properties());
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("Text");
    when(mockIndexingService.isRunning()).thenReturn(true);
    subject.loadFromXml(mockConfig);
    subject.commitBatch(
        Arrays.asList(deleteOperation(URL), addOperation(URL, MIME_TEXT, null, !CONTENT_ERROR)));
    subject.commitComplete();
    verify(mockIndexingService).isRunning();
    verify(mockIndexingService.stopAsync()).awaitTerminated();
  }

  @Test
  public void indexingServiceInitializedForEachBatch() {
    setupConfig.initConfig(new java.util.Properties());
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("Text");
    when(mockIndexingService.isRunning()).thenReturn(true);
    subject.loadFromXml(mockConfig);
    subject.commitBatch(
        Arrays.asList(deleteOperation(URL), addOperation(URL, MIME_TEXT, null, !CONTENT_ERROR)));
    subject.commitBatch(
        Arrays.asList(deleteOperation(URL), addOperation(URL, MIME_PDF, null, !CONTENT_ERROR)));
    subject.commitBatch(
        Arrays.asList(deleteOperation(URL), addOperation(URL, MIME_TEXT, null, !CONTENT_ERROR)));
    subject.commitComplete();
    verify(mockIndexingService, times(3)).startAsync();
  }

  @Test
  public void indexingServiceInitializedAgainAfterCommitComplete() throws Exception {
    setupConfig.initConfig(new java.util.Properties());
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
        .thenReturn("Text");
    when(mockIndexingService.isRunning()).thenReturn(true);
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_TEXT, null, !CONTENT_ERROR)));
    subject.commitComplete();
    subject.commitBatch(Arrays.asList(addOperation(URL, MIME_TEXT, null, !CONTENT_ERROR)));
    InOrder inOrder = Mockito.inOrder(mockHelper, mockIndexingService);
    inOrder.verify(mockHelper).createIndexingService();
    inOrder.verify(mockIndexingService).startAsync();
    inOrder.verify(mockIndexingService).stopAsync();
    inOrder.verify(mockHelper).createIndexingService();
    inOrder.verify(mockIndexingService).startAsync();
  }

  @Test
  public void saveToXmlShouldSaveValuesToXML() throws IOException {
    subject.loadFromXml(mockConfig);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    subject.saveToXML(new XmlStreamWriter(out));
    assertTrue(out.toString().contains("<uploadFormat>raw</uploadFormat>"));
    assertTrue(out.toString().contains("<configFilePath>/path/to/config</configFilePath>"));
  }

  @Test
  public void successfulItemMetadataFields() throws IOException {
    // Title is not set, so the default is used. updateTime is set, but
    // the default has a non-empty value, so the default is used.
    java.util.Properties config = new java.util.Properties();
    config.put(IndexingItemBuilder.UPDATE_TIME_FIELD, "modified");
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, "created");
    config.put(IndexingItemBuilder.CONTENT_LANGUAGE_VALUE, "en");
    setupConfig.initConfig(config);

    String title = "Hello";
    String language = "en";
    String created = new DateTime("2018-08-10T10:10:10.100Z").toString();
    String modified = new DateTime("2018-08-13T15:01:23.100Z").toString();
    String lastModified = new DateTime("2018-08-18T18:01:23.100Z").toString();

    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
      .thenReturn("Text");
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(new IAddOperation() {
      @Override
      public void delete() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getReference() {
        return URL;
      }

      @Override
      public Properties getMetadata() {
        Properties props = new Properties();
        props.addString(GoogleCloudSearchCommitter.FIELD_CONTENT_TYPE, MIME_TEXT);
        props.addString(GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT, null);
        props.addString("title", title);
        props.addString("created", created);
        props.addString("modified", modified);
        props.addString("Last-Modified", lastModified);
        return props;
      }

      @Override
      public InputStream getContentStream() throws IOException {
        return ByteSource.wrap(CONTENT.getBytes(StandardCharsets.UTF_8)).openStream();
      }
    }));

    verify(mockIndexingService)
      .indexItemAndContent(
          itemCaptor.capture(),
          any(),
          any(),
          any(),
          any());

    ItemMetadata expectedItemMetadata =
        new ItemMetadata()
          .setTitle(title)
          .setMimeType(MIME_TEXT)
          .setSourceRepositoryUrl(URL)
          .setContentLanguage(language)
          .setCreateTime(created)
          .setUpdateTime(lastModified);
    assertEquals(expectedItemMetadata, itemCaptor.getValue().getMetadata());
  }

  @Test
  public void successfulItemStructuredData() throws IOException {
    PropertyDefinition prop1 = new PropertyDefinition().setName("approved").setIsRepeatable(false)
        .setIsReturnable(true).setBooleanPropertyOptions(new BooleanPropertyOptions());
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Arrays.asList(
            new ObjectDefinition()
                .setName("schema1")
                .setPropertyDefinitions(Arrays.asList(prop1))));

    java.util.Properties config = new java.util.Properties();
    config.put(IndexingItemBuilder.OBJECT_TYPE, "schema1");
    setupConfig.initConfig(config);

    when(mockIndexingService.getSchema()).thenReturn(schema);
    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
      .thenReturn("Text");
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(new IAddOperation() {
      @Override
      public void delete() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getReference() {
        return URL;
      }

      @Override
      public Properties getMetadata() {
        Properties props = new Properties();
        props.addString(GoogleCloudSearchCommitter.FIELD_CONTENT_TYPE, MIME_TEXT);
        props.addString(GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT, null);
        props.addString("approved", "true");
        return props;
      }

      @Override
      public InputStream getContentStream() throws IOException {
        return ByteSource.wrap(CONTENT.getBytes(StandardCharsets.UTF_8)).openStream();
      }
    }));

    verify(mockIndexingService)
    .indexItemAndContent(
        itemCaptor.capture(),
        any(),
        any(),
        any(),
        any());

    NamedProperty approved = new NamedProperty().setName("approved").setBooleanValue(true);
    StructuredDataObject structuredData =
        new StructuredDataObject().setProperties(Arrays.asList(approved));
    assertEquals(structuredData, itemCaptor.getValue().getStructuredData().getObject());
  }

  //TODO (sveldurthi): Add test for multi-value field.

  @Test
  public void successfulDateFormatFields() throws IOException {
    java.util.Properties config = new java.util.Properties();
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, "created");
    config.put(IndexingItemBuilder.CONTENT_LANGUAGE_VALUE, "en");
    config.put(StructuredData.DATETIME_PATTERNS, "yyyy-MMM-dd h:mm:ss a z");
    setupConfig.initConfig(config);

    String title = "Hello";
    String language = "en";
    String createTime = "2018-AUG-10 3:10:20 AM PDT";
    String modifyTime = "Thu, 16 Aug 2018 11:12:25 GMT";

    when(mockConfig.getString(eq(GoogleCloudSearchCommitter.CONFIG_KEY_UPLOAD_FORMAT), any()))
      .thenReturn("Text");
    subject.loadFromXml(mockConfig);
    subject.commitBatch(Arrays.asList(new IAddOperation() {
      @Override
      public void delete() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getReference() {
        return URL;
      }

      @Override
      public Properties getMetadata() {
        Properties props = new Properties();
        props.addString(GoogleCloudSearchCommitter.FIELD_CONTENT_TYPE, MIME_TEXT);
        props.addString(GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT, null);
        props.addString("title", title);
        props.addString("created", createTime);
        props.addString("Last-Modified", modifyTime);
        return props;
      }

      @Override
      public InputStream getContentStream() throws IOException {
        return ByteSource.wrap(CONTENT.getBytes(StandardCharsets.UTF_8)).openStream();
      }
    }));

    verify(mockIndexingService)
      .indexItemAndContent(
          itemCaptor.capture(),
          any(),
          any(),
          any(),
          any());

    ItemMetadata expectedItemMetadata =
        new ItemMetadata()
          .setTitle(title)
          .setMimeType(MIME_TEXT)
          .setSourceRepositoryUrl(URL)
          .setContentLanguage(language)
          .setCreateTime("2018-08-10T03:10:20.000-07:00")
          .setUpdateTime("2018-08-16T11:12:25.000Z");
    assertEquals(expectedItemMetadata, itemCaptor.getValue().getMetadata());
  }

  private IAddOperation addOperation(
      String url, String mimeType, String binaryContent, boolean textContentError) {
    return new IAddOperation() {

      @Override
      public void delete() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getReference() {
        return url;
      }

      @Override
      public Properties getMetadata() {
        Properties props = new Properties();
        props.addString(GoogleCloudSearchCommitter.FIELD_CONTENT_TYPE, mimeType);
        if (binaryContent != null) {
          props.addString(GoogleCloudSearchCommitter.FIELD_BINARY_CONTENT, binaryContent);
        }
        return props;
      }

      @Override
      public InputStream getContentStream() throws IOException {
        if (textContentError) {
          InputStream contentStream = mock(InputStream.class);
          when(contentStream.read(any())).thenThrow(new IOException());
          return contentStream;
        }
        return ByteSource.wrap(CONTENT.getBytes(StandardCharsets.UTF_8)).openStream();
      }
    };
  }

  private IDeleteOperation deleteOperation(String url) {
    return new IDeleteOperation() {

      @Override
      public void delete() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String getReference() {
        return url;
      }
    };
  }

  private Item goldenItem(boolean applyDomainAcl, String mimeType) {
    Item item =
        new Item()
            .setName(URL)
            .setItemType(ItemType.CONTENT_ITEM.name())
            .setMetadata(new ItemMetadata().setSourceRepositoryUrl(URL).setMimeType(mimeType));
    if (applyDomainAcl) {
      item.setAcl(new ItemAcl().setReaders(Collections.singletonList(Acl.getCustomerPrincipal())));
    }
    return item;
  }
}
