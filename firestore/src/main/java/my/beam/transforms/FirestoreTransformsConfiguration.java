package my.beam.transforms;

public class FirestoreTransformsConfiguration {
  String projectId;
  String parent;
  String collectionId;
  String documentName;

  public void setProjectId(String projectId){
    this.projectId = projectId;
  }

  public void setParent(String parent) {
    this.parent = parent;
  }

  public void setCollectionId(String collectionId) {
    this.collectionId = collectionId;
  }
}