package my.beam.transforms;

import com.google.auto.service.AutoService;

import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

import my.beam.transforms.FirestoreTransformsBuilder.FirestoreListDocumentsBuilder;
// import my.beam.transforms.FirestoreTransformsBuilder.FirestoreListCollectionIdsBuilder;
// import my.beam.transforms.FirestoreTransformsBuilder.FirestoreBatchGetDocumentsBuilder;
// import my.beam.transforms.FirestoreTransformsBuilder.FirestoreRunQueryBuilder;
// import my.beam.transforms.FirestoreTransformsBuilder.FirestorePartitionQueryBuilder;
import my.beam.transforms.FirestoreTransformsBuilder.FirestoreWriteBuilder;

@AutoService(ExternalTransformRegistrar.class)
public class FirestoreTransformsRegistrar implements ExternalTransformRegistrar {

  final static String URN_LIST_DOCS = "my.beam.transform.firestore_list_documents";
  final static String URN_WRITE = "my.beam.transform.firestore_write";
  // final static String URN_LIST_COLL_IDS = "my.beam.transform.firestore_list_collection_ids";
  // final static String URN_BATCH_GET_DOCS = "my.beam.transform.firestore_batch_get_documents";
  // final static String URN_RUN_QUERY = "my.beam.transform.firestore_run_query";
  // final static String URN_PART_QUERY = "my.beam.transform.firestore_partition_query";
  
  @Override
  public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    return ImmutableMap.<String, ExternalTransformBuilder<?, ?, ?>>builder()
      .put(URN_LIST_DOCS,new FirestoreListDocumentsBuilder())
      .put(URN_WRITE, new FirestoreWriteBuilder())
      // .put(URN_LIST_COLL_IDS,new FirestoreListCollectionIdsBuilder())
      // .put(URN_BATCH_GET_DOCS,new FirestoreBatchGetDocumentsBuilder())
      // .put(URN_RUN_QUERY,new FirestoreRunQueryBuilder())
      // .put(URN_PART_QUERY,new FirestorePartitionQueryBuilder())
      .build();
    }
}