#include "leveldbjni.h"
#include <jni.h>

#include <iostream>
#include <list>

#include "org_fusesource_leveldbjni_internal_ChunkHelper.h"
#include "buffer_link.hpp"

//#define DEBUG_CHUNKS 1

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jobject JNICALL Java_org_fusesource_leveldbjni_internal_ChunkHelper_nextChunk
(JNIEnv *env, jclass ignored, jlong iterPtr, jlong size) {
  chunk_pairs(env, (void *)iterPtr, size);
}


jobject chunk_pairs(JNIEnv *env, void *iterPtr, jsize size) {
  leveldb::Iterator *iter = (leveldb::Iterator *)iterPtr;
  
  // Need some temporary storage space, since we don't know how large our keys/values are
  #define BUFFER_SIZE 16000
  std::list<BufferLink *> keyList;
  std::list<BufferLink *> valList;

  int count = 0;
  long totalKeySize = 0;
  long totalValSize = 0;

  BufferLink *currentKeyBuffer = new BufferLink(BUFFER_SIZE);
  BufferLink *currentValBuffer = new BufferLink(BUFFER_SIZE);

  jint *keyIndices = new jint[size];
  jint *valIndices = new jint[size];

#ifdef DEBUG_CHUNKS
  std::cout << "Iterating" << std::endl;
#endif

  for (int i = 0; i < size && iter->Valid(); ++i) {
    ++count;

    leveldb::Slice ks = iter->key();
    leveldb::Slice vs = iter->value();

    // We need to allocate new buffers if we can't fit
    if ((ks.size() + currentKeyBuffer->limit) > BUFFER_SIZE) {
      keyList.push_back(currentKeyBuffer);
      currentKeyBuffer = new BufferLink(BUFFER_SIZE);
    }

    if ((vs.size() + currentValBuffer->limit) > BUFFER_SIZE) {
      valList.push_back(currentValBuffer);
      currentValBuffer = new BufferLink(BUFFER_SIZE);
    }

    // Compact key and value onto the end of current buffers
    keyIndices[i] = totalKeySize;
    totalKeySize += ks.size();
    memcpy((jbyte *)(currentKeyBuffer->contents + currentKeyBuffer->limit), ks.data(), ks.size());
    currentKeyBuffer->limit += ks.size();

    valIndices[i] = totalValSize;
    totalValSize += vs.size();
    memcpy((jbyte *)(currentValBuffer->contents + currentValBuffer->limit), vs.data(), vs.size());
    currentValBuffer->limit += vs.size();
    
    iter->Next();
  }

  // Push the final buffer onto our lists
  keyList.push_back(currentKeyBuffer);
  valList.push_back(currentValBuffer);

  // Allocate our Java arrays
  jbyteArray keyValueJArray = env->NewByteArray(totalKeySize);
  jbyteArray valValueJArray = env->NewByteArray(totalValSize);
  jintArray keyIndexJArray = env->NewIntArray(count);
  jintArray valIndexJArray = env->NewIntArray(count);

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
  
  // Compact all of the key values together
  int currentInsertPoint = 0;

#ifdef DEBUG_CHUNKS
  std::cout << "Compacting" << std::endl;
#endif

  for (std::list<BufferLink *>::iterator it = keyList.begin(); it != keyList.end(); it++) {
#ifdef DEBUG_CHUNKS
    std::cout << "  compact key buffer: " << std::dec << (*it)->limit << " bytes at 0x" << std::hex << ((long)(*it)->contents) << std::endl;
#endif
    env->SetByteArrayRegion(keyValueJArray,
                            currentInsertPoint,
                            (*it)->limit,
                            (jbyte *) (*it)->contents);

    currentInsertPoint += (*it)->limit;

    // Clean up the buffer at this point
    delete (*it);
  }

  // Now compact values
  currentInsertPoint = 0;

  for (std::list<BufferLink *>::iterator it = valList.begin(); it != valList.end(); it++) {
#ifdef DEBUG_CHUNKS
    std::cout << "  compact val buffer: " << std::dec << (*it)->limit << " bytes at 0x" << std::hex << ((long)(*it)->contents) << std::endl;
#endif
    env->SetByteArrayRegion(valValueJArray,
                            currentInsertPoint,
                            (*it)->limit,
                            (*it)->contents);

    currentInsertPoint += (*it)->limit;

    // Clean up the buffer at this point
    delete (*it);
  }

#ifdef DEBUG_CHUNKS
  std::cout << "Allocate return obj" << std::endl;
#endif

  // Locate the proper class, constructor, etc
  jclass chunkClazz = env->FindClass("org/fusesource/leveldbjni/KeyValueChunk");
  if (!chunkClazz) {
    std::cerr << "Could not locate chunk class!" << std::endl;
  }

  jmethodID constructorID = env->GetMethodID(chunkClazz, "<init>", "()V");
  if (!constructorID) {
    std::cerr << "Could not locate chunk constructor!" << std::endl;
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

