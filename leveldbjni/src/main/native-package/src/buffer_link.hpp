#include <jni.h>

class BufferLink {
public:
  int limit;
  jbyte *contents;
  
  BufferLink(int size) {
    limit = 0;
    contents = new jbyte[size];
  }
  
  ~BufferLink() {
    delete contents;
  }
};

