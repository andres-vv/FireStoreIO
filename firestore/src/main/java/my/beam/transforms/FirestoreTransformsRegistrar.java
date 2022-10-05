package my.beam.transforms;

import com.google.auto.service.AutoService;

import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

import my.beam.transforms.FirestoreTransformsBuilder.FirestoreReadBuilder;
import my.beam.transforms.FirestoreTransformsBuilder.FirestoreWriteBuilder;

@AutoService(ExternalTransformRegistrar.class)
public class FirestoreTransformsRegistrar implements ExternalTransformRegistrar {
  final static String URN_LIST_DOCS = "my.beam.transform.firestore_read";
  final static String URN_WRITE = "my.beam.transform.firestore_write";
  
  @Override
  public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    return ImmutableMap.<String, ExternalTransformBuilder<?, ?, ?>>builder()
      .put(URN_LIST_DOCS,new FirestoreReadBuilder())
      .put(URN_WRITE, new FirestoreWriteBuilder())
      .build();
    }
}