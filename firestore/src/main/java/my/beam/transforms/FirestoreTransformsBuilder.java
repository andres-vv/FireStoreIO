package my.beam.transforms;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PDone;
import org.json.JSONObject;
import org.apache.arrow.flatbuf.Bool;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1;

import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListCollectionIdsResponse;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.DocumentChange;
import com.google.firestore.v1.Write;
import com.google.gson.Gson;
import com.google.api.client.json.GenericJson;
import com.google.common.reflect.TypeToken;
import com.google.protobuf.Descriptors.FieldDescriptor;

import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteSuccessSummary;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListDocuments;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.coders.SerializableCoder;

public class FirestoreTransformsBuilder{

  public static class ListDocumentsTransform extends PTransform<PBegin, PCollection<String>> {

    final String parent;
    final String collectionId;
    

    public ListDocumentsTransform(String parent, String collectionId) {
      this.parent = parent;
      this.collectionId = collectionId;
    }

    class ConvertToKVDoFn extends DoFn<Document, String> {
  
      @ProcessElement
      public void process(@Element Document input, OutputReceiver<String>o) {
        LinkedHashMap<String,Value> map = new LinkedHashMap<>(input.getFieldsMap());
        LinkedHashMap<String, Object> newMap = new LinkedHashMap<String, Object>();
        Set<String> keys = map.keySet();
        for (String key : keys) {
          String type = map.get(key).toString();
          Object value = new Object();
          if (type.contains("integer")) {
            value = map.get(key).getIntegerValue();
          } else if (type.contains("boolean")){
            value = map.get(key).getBooleanValue();
          } else {
            value = map.get(key).getStringValue();
          }
          newMap.put(key,value);
        }
        o.output(new JSONObject(newMap).toString());
      }
    }
  
    @Override
    public PCollection<String> expand(PBegin input) {
      ListDocumentsRequest request = ListDocumentsRequest.newBuilder()
                                                       .setParent(this.parent)
                                                       .setCollectionId(this.collectionId)
                                                       .build();
      PCollection<ListDocumentsRequest> listDocumentsRequests = input.apply(Create.of(request).withCoder(SerializableCoder.of(ListDocumentsRequest.class)));
      PCollection<Document> documents = listDocumentsRequests.apply(FirestoreIO.v1().read().listDocuments().build());
      PCollection<String> documentStrings = documents.apply(ParDo.of(new ConvertToKVDoFn()));
      return documentStrings;
    }
  }

  public static class WriteTransform extends PTransform<PCollection<String>, PDone>{

    final String parent;
    final String collectionId;
    final String projectId;

    public WriteTransform(String parent, String collectionId, String projectId) {
      this.parent = parent;
      this.collectionId = collectionId;
      this.projectId = projectId;
    }

    // process to create document from data
    class ConvertToDocumentDoFn extends DoFn<String, Document> {
      public void addField(String key, Map<String, String> map, Map<String, Value>fieldMap){
        // check if Boolean
        if ("true".equalsIgnoreCase(map.get(key)) || "false".equalsIgnoreCase(map.get(key))){
          Boolean number = Boolean.parseBoolean(map.get(key));
          fieldMap.put(key, Value.newBuilder().setBooleanValue(number).build());
        }
        else{
          // check if Number
          try{
            Long number = Long.parseLong(map.get(key));
            fieldMap.put(key, Value.newBuilder().setIntegerValue(number).build());
          }
          // if none of the above cast as string
          catch(NumberFormatException nfe){
            fieldMap.put(key, Value.newBuilder().setStringValue(map.get(key)).build());
          }
        }
      }
      
      @ProcessElement
      public void processElement(@Element String input, OutputReceiver<Document> o) {
        Map<String, String> map = new Gson().fromJson(input, new TypeToken<HashMap<String, String>>() {}.getType());
        Set<String> keys = map.keySet();
        Map <String, Value> fieldMap = new HashMap<>();
        for (String key : keys) {
          // method to add value based on Value datatype
          addField(key, map, fieldMap);
        }
        Document document = Document.newBuilder()
                                  .setName("projects/" + projectId + "/databases/(default)/documents/" + collectionId + "/" + map.get("id"))
                                  .putAllFields(fieldMap)
                                  .build();
        o.output(document);
      }
    }

    // process to create Write from document
    class ConvertToWriteDoFn extends DoFn<Document, Write> {
      @ProcessElement
      public void processElement(@Element Document input, OutputReceiver<Write> o) {
        Write write = Write.newBuilder()
          .setUpdate(input)
          .build();
      o.output(write);
      }
    }

    

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new ConvertToDocumentDoFn()))
          .apply(ParDo.of(new ConvertToWriteDoFn()))
          .apply(FirestoreIO.v1().write().batchWrite().build());
      return PDone.in(input.getPipeline());
    }
  }
  
  public static class FirestoreListDocumentsBuilder implements
      ExternalTransformBuilder<FirestoreTransformsConfiguration, PBegin, PCollection<String>> {

    @Override
    public PTransform<PBegin, PCollection<String>> buildExternal(
      FirestoreTransformsConfiguration configuration) {
        return new ListDocumentsTransform(configuration.parent, configuration.collectionId);
    }
  }
  
  public static class FirestoreWriteBuilder implements
      ExternalTransformBuilder<FirestoreTransformsConfiguration, PCollection<String>, PDone> {
  
    @Override
    public PTransform<PCollection<String>, PDone> buildExternal(
      FirestoreTransformsConfiguration configuration) {
        return new WriteTransform(configuration.parent, configuration.collectionId, configuration.projectId);
    }
  }

  // public static class FirestoreListCollectionIdsBuilder implements
  //     ExternalTransformBuilder<FirestoreTransformsConfiguration, PCollection<ListCollectionIdsRequest>, PCollection<String>> {
  
  //   @Override
  //   public PTransform<PCollection<ListCollectionIdsRequest>, PCollection<String>> buildExternal(
  //     FirestoreTransformsConfiguration configuration) {
  //     return FirestoreIO.v1().read().listCollectionIds().build();
  //   }
  // }

  // public static class FirestoreBatchGetDocumentsBuilder implements
  //     ExternalTransformBuilder<FirestoreTransformsConfiguration, PCollection<BatchGetDocumentsRequest>, PCollection<BatchGetDocumentsResponse>> {
  
  //   @Override
  //   public PTransform<PCollection<BatchGetDocumentsRequest>, PCollection<BatchGetDocumentsResponse>> buildExternal(
  //     FirestoreTransformsConfiguration configuration) {
  //     return FirestoreIO.v1().read().batchGetDocuments().build();
  //   }
  // }

  // public static class FirestoreRunQueryBuilder implements
  //     ExternalTransformBuilder<FirestoreTransformsConfiguration, PCollection<RunQueryRequest>, PCollection<RunQueryResponse>> {
  
  //   @Override
  //   public PTransform<PCollection<RunQueryRequest>, PCollection<RunQueryResponse>> buildExternal(
  //     FirestoreTransformsConfiguration configuration) {
  //     return FirestoreIO.v1().read().runQuery().build();
  //   }
  // }

  // public static class FirestorePartitionQueryBuilder implements
  //     ExternalTransformBuilder<FirestoreTransformsConfiguration, PCollection<PartitionQueryRequest>, PCollection<RunQueryRequest>> {
  
  //   @Override
  //   public PTransform<PCollection<PartitionQueryRequest>, PCollection<RunQueryRequest>> buildExternal(
  //     FirestoreTransformsConfiguration configuration) {
  //     return FirestoreIO.v1().read().partitionQuery().build();
  //   }
  // }

  
}

