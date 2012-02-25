#include "leveldbjni.h"
#include <jni.h>

#include <iostream>
#include <list>

#include "org_fusesource_leveldbjni_internal_ChunkHelper.h"

//#define DEBUG_CHUNKS 1

#ifdef __cplusplus
extern "C" {
#endif

  JNIEXPORT jobject JNICALL Java_org_fusesource_leveldbjni_internal_ChunkHelper_nextChunk
  (JNIEnv *env, jclass ignored, jlong iterPtr, jlong size) {
    chunk_pairs(env, (void *)iterPtr, size);
  }


  jobject chunk_pairs(JNIEnv *env, void *iterPtr, jsize maxByteSize) {
    leveldb::Iterator *iter = (leveldb::Iterator *)iterPtr;
  
    int count = 0;
    long totalKeySize = 0;
    long totalValSize = 0;

    jbyte *keyBuffer = NULL;
    jbyte *valBuffer = NULL;

    jint *keyIndices = NULL;
    jint *valIndices = NULL;

    try {
      keyBuffer = new jbyte[maxByteSize];
      valBuffer = new jbyte[maxByteSize];
      keyIndices = new jint[maxByteSize];
      valIndices = new jint[maxByteSize];
    } catch (std::bad_alloc&) {
      // Failed to allocate a buffer, we need to discard anything we've successfully allocated and throw a Java exception
      if (keyBuffer != NULL) {
        delete keyBuffer;
      }
      if (valBuffer != NULL) {
        delete valBuffer;
      }
      if (keyIndices != NULL) {
        delete keyIndices;
      }
      if (valIndices != NULL) {
        delete valIndices;
      }

      jclass excClass = env->FindClass("java/lang/OutOfMemoryError");
      if (excClass == NULL) {
        std::cerr << "Failed to locate java.lang.OutOfMemoryError class on memory allocation failure!" << std::endl;
        return NULL;
      }
      
      env->ThrowNew(excClass, "Failed to allocate buffers");

      return NULL;
    }
    
#ifdef DEBUG_CHUNKS
    std::cout << "Iterating" << std::endl;
#endif

    // We simply iterate as long as the iterator is valid. We do free space checks on our buffers later
    for (int i = 0; iter->Valid(); ++i) {
      leveldb::Slice ks = iter->key();
      leveldb::Slice vs = iter->value();

      // We have to stop if either key or value would exceed our chunk buffer
      if ((ks.size() + totalKeySize) > maxByteSize || (vs.size() + totalValSize) > maxByteSize) {
        break;
      }

      ++count;

      // Compact key and value onto the end of current buffers
      keyIndices[i] = totalKeySize;
      memcpy((jbyte *)(keyBuffer + totalKeySize), ks.data(), ks.size());
      totalKeySize += ks.size();

      valIndices[i] = totalValSize;
      memcpy((jbyte *)(valBuffer + totalValSize), vs.data(), vs.size());
      totalValSize += vs.size();
    
      iter->Next();
    }

  // Allocate our Java arrays
  jbyteArray keyValueJArray = env->NewByteArray(totalKeySize);
  jbyteArray valValueJArray = env->NewByteArray(totalValSize);
  jintArray keyIndexJArray = env->NewIntArray(count);
  jintArray valIndexJArray = env->NewIntArray(count);

  // TODO: detect and deal with OutOfMemoryError here

#ifdef DEBUG_CHUNKS
  std::cout << "Copying indices" << std::endl;
#endif

  // Copy indices
  env->SetIntArrayRegion(keyIndexJArray,
                         0,
                         count,
                         keyIndices);

  env->SetIntArrayRegion(valIndexJArray,
                         0,
                         count,
                         valIndices);

  delete keyIndices;
  delete valIndices;
  
#ifdef DEBUG_CHUNKS
  std::cout << "Copying data" << std::endl;
#endif
  
  // Copy data
  env->SetByteArrayRegion(keyValueJArray,
                          0,
                          totalKeySize,
                          keyBuffer);

  env->SetByteArrayRegion(valValueJArray,
                          0,
                          totalValSize,
                          valBuffer);

#ifdef DEBUG_CHUNKS
  std::cout << "Allocate return obj" << std::endl;
#endif

  // Locate the proper class, constructor, etc
  jclass chunkClazz = env->FindClass("org/fusesource/leveldbjni/KeyValueChunk");
  if (!chunkClazz) {
    jclass classNotFoundExc = env->FindClass("java/lang/ClassNotFoundException");
    if (classNotFoundExc == NULL) {
      std::cerr << "Could not locate java.lang.ClassNotFoundException!" << std::endl;
      return NULL;
    }

    env->ThrowNew(classNotFoundExc, "Could not locate org.fusesource.leveldbjni.KeyValueChunk");
    
    return NULL;
  }

  jmethodID constructorID = env->GetMethodID(chunkClazz, "<init>", "()V");
  if (!constructorID) {
    std::cerr << "Could not locate chunk constructor!" << std::endl;
    return NULL;
  }

  jobject chunk = env->NewObject(chunkClazz, constructorID);

#ifdef DEBUG_CHUNKS
  std::cout << "Set return obj fields" << std::endl;
#endif

  // Set the fields
  env->SetIntField(chunk, env->GetFieldID(chunkClazz, "size", "I"), count);
  env->SetObjectField(chunk, env->GetFieldID(chunkClazz, "keyIndices", "[I"), keyIndexJArray);
  env->SetObjectField(chunk, env->GetFieldID(chunkClazz, "valIndices", "[I"), valIndexJArray);
  env->SetObjectField(chunk, env->GetFieldID(chunkClazz, "keys", "[B"), keyValueJArray);
  env->SetObjectField(chunk, env->GetFieldID(chunkClazz, "values", "[B"), valValueJArray);

  return chunk;
}

#ifdef __cplusplus
}
#endif

