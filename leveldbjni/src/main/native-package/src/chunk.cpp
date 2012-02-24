#include "leveldbjni.h"
#include <jni.h>

#include "org_fusesource_leveldbjni_internal_ChunkHelper.h"

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jobjectArray JNICALL Java_org_fusesource_leveldbjni_internal_ChunkHelper_nextChunk
(JNIEnv *env, jclass ignored, jlong iterPtr, jlong size) {
  chunk_pairs(env, (void *)iterPtr, size);
}

jobjectArray chunk_pairs(JNIEnv *env, void *iterPtr, jsize size) {
  leveldb::Iterator *iter = (leveldb::Iterator *)iterPtr;

  // Create the outer array based on an example row
  jbyteArray byteExample = env->NewByteArray(0);
  jobjectArray  objByteExample = env->NewObjectArray(0, env->GetObjectClass(byteExample), 0);

  jobjectArray outer = env->NewObjectArray(size, env->GetObjectClass(objByteExample), 0);

  // Copy each key/value pair into its slot
  for (int i = 0; i < size && iter->Valid(); i++) {
    leveldb::Slice ks = iter->key();
    leveldb::Slice vs = iter->value();

    jbyteArray keyArray = env->NewByteArray(ks.size());
    env->SetByteArrayRegion((jbyteArray)keyArray, 
                            (jsize)0,
                            ks.size(),
                            (const jbyte *)ks.data());

    jbyteArray valArray = env->NewByteArray(vs.size());
    env->SetByteArrayRegion((jbyteArray)valArray,
                            (jsize)0,
                            vs.size(),
                            (const jbyte *)vs.data());

    jobjectArray row = env->NewObjectArray((jsize) 2, env->GetObjectClass(keyArray), 0);
    
    env->SetObjectArrayElement(row, 0, keyArray);
    env->SetObjectArrayElement(row, 1, valArray);

    env->SetObjectArrayElement(outer, i, row);

    iter->Next();
  }
  
  return outer;
}

#ifdef __cplusplus
}
#endif

