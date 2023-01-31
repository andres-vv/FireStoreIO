package my.beam.transforms;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.json.JSONObject;

import com.google.common.reflect.TypeToken;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.gson.Gson;

public class FirestoreTransformsBuilder {

  public static class ReadTransform extends PTransform<PBegin, PCollection<String>> {

    final String parent;
    final String collectionId;

    public ReadTransform(String parent, String collectionId) {
      this.parent = parent;
      this.collectionId = collectionId;
    }

    class ConvertToStringDoFn extends DoFn<Document, String> {
      @ProcessElement
      public void process(@Element Document input, OutputReceiver<String> o) {
        LinkedHashMap<String, Value> map = new LinkedHashMap<>(input.getFieldsMap());
        LinkedHashMap<String, Object> newMap = new LinkedHashMap<String, Object>();
        // Extract values from Document Values in Primitive Types
        newMap.put("Atk", map.get("Atk").getIntegerValue());
        newMap.put("Def", map.get("Def").getIntegerValue());
        newMap.put("HP", map.get("HP").getIntegerValue());
        newMap.put("Level", map.get("Level").getIntegerValue());
        newMap.put("Name", map.get("Name").getStringValue());
        newMap.put("Nature", map.get("Nature").getStringValue());
        newMap.put("Sp. Atk", map.get("Sp. Atk").getIntegerValue());
        newMap.put("Sp. Def", map.get("Sp. Def").getIntegerValue());
        newMap.put("Speed", map.get("Speed").getIntegerValue());
        newMap.put("Type1", map.get("Type1").getStringValue());
        newMap.put("Type2", map.get("Type2").getStringValue());
        // Convert to JSON String to comply with Beam Encoder datatypes
        o.output(new JSONObject(newMap).toString());
      }
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      // Create Request
      ListDocumentsRequest request = ListDocumentsRequest.newBuilder()
          .setParent(this.parent)
          .setCollectionId(this.collectionId)
          .build();
      // Make PCollections of ListDocumentsRequest
      PCollection<ListDocumentsRequest> listDocumentsRequests = input
          .apply(Create.of(request).withCoder(SerializableCoder.of(ListDocumentsRequest.class)));
      // Collect Documents
      PCollection<Document> documents = listDocumentsRequests.apply(FirestoreIO.v1().read().listDocuments().build());
      // Convert to JSON String
      PCollection<String> documentStrings = documents.apply(ParDo.of(new ConvertToStringDoFn()));
      return documentStrings;
    }
  }

  public static class WriteTransform extends PTransform<PCollection<String>, PDone> {

    final String parent;
    final String collectionId;

    public WriteTransform(String parent, String collectionId) {
      this.parent = parent;
      this.collectionId = collectionId;
    }

    // process to create document from data
    class ConvertToWriteDoFn extends DoFn<String, Write> {
      @ProcessElement
      public void processElement(@Element String input, OutputReceiver<Write> o) {
        // Parse to JSON String to JSOM Object
        Map<String, String> map = new Gson().fromJson(input, new TypeToken<HashMap<String, String>>() {
        }.getType());

        // Build document that needs to be uploaded to Firestore
        Document document = Document.newBuilder()
            .setName(parent + "/" + collectionId + "/" + map.get("Name"))
            .putFields("Atk", Value.newBuilder().setIntegerValue(Long.parseLong(map.get("Atk"))).build())
            .putFields("Def", Value.newBuilder().setIntegerValue(Long.parseLong(map.get("Def"))).build())
            .putFields("HP", Value.newBuilder().setIntegerValue(Long.parseLong(map.get("HP"))).build())
            .putFields("Level", Value.newBuilder().setIntegerValue(Long.parseLong(map.get("Level"))).build())
            .putFields("Name", Value.newBuilder().setStringValue(map.get("Name")).build())
            .putFields("Nature", Value.newBuilder().setStringValue(map.get("Nature")).build())
            .putFields("Sp. Atk", Value.newBuilder().setIntegerValue(Long.parseLong(map.get("Sp. Atk"))).build())
            .putFields("Sp. Def", Value.newBuilder().setIntegerValue(Long.parseLong(map.get("Sp. Def"))).build())
            .putFields("Speed", Value.newBuilder().setIntegerValue(Long.parseLong(map.get("Speed"))).build())
            .putFields("Type1", Value.newBuilder().setStringValue(map.get("Type1")).build())
            .putFields("Type2", Value.newBuilder().setStringValue(map.get("Type2")).build())
            .build();
        // Create write object
        Write write = Write.newBuilder()
            .setUpdate(document)
            .build();
        o.output(write);
      }
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(ParDo.of(new ConvertToWriteDoFn()))
          .apply(FirestoreIO.v1().write().batchWrite().build());
      return PDone.in(input.getPipeline());
    }
  }

  public static class FirestoreReadBuilder implements
      ExternalTransformBuilder<FirestoreTransformsConfiguration, PBegin, PCollection<String>> {
    @Override
    public PTransform<PBegin, PCollection<String>> buildExternal(
        FirestoreTransformsConfiguration configuration) {
      return new ReadTransform(configuration.parent, configuration.collectionId);
    }
  }

  public static class FirestoreWriteBuilder implements
      ExternalTransformBuilder<FirestoreTransformsConfiguration, PCollection<String>, PDone> {
    @Override
    public PTransform<PCollection<String>, PDone> buildExternal(
        FirestoreTransformsConfiguration configuration) {
      return new WriteTransform(configuration.parent, configuration.collectionId);
    }
  }
}
