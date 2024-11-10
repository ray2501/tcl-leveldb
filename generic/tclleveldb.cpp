/*
 * For C++ compilers, use extern "C"
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

#ifdef __cplusplus
extern "C" {
#endif

#include <tcl.h>

/*
 * Only the _Init function is exported.
 */

extern DLLEXPORT int    Leveldb_Init(Tcl_Interp * interp);

/*
 * end block for C++
 */

#ifdef __cplusplus
}
#endif

typedef struct ThreadSpecificData {
  int initialized;                /* initialization flag */
  Tcl_HashTable *leveldb_hashtblPtr; /* per thread hash table. */
  int dbi_count;
  int itr_count;
  int bat_count;
  int sst_count;
} ThreadSpecificData;

static Tcl_ThreadDataKey dataKey;

TCL_DECLARE_MUTEX(myMutex);


void LEVELDB_Thread_Exit(ClientData clientdata)
{
  ThreadSpecificData *tsdPtr = (ThreadSpecificData *)
      Tcl_GetThreadData(&dataKey, sizeof(ThreadSpecificData));

  if(tsdPtr->leveldb_hashtblPtr) {
    Tcl_DeleteHashTable(tsdPtr->leveldb_hashtblPtr);
    ckfree(tsdPtr->leveldb_hashtblPtr);
  }
}


static int LEVELDB_SST(void *cd, Tcl_Interp *interp, int objc,Tcl_Obj *const*objv){
  int choice;
  const leveldb::Snapshot* shot;
  Tcl_HashEntry *hashEntryPtr;
  char *sstHandle;

  ThreadSpecificData *tsdPtr = (ThreadSpecificData *)
      Tcl_GetThreadData(&dataKey, sizeof(ThreadSpecificData));

  if (tsdPtr->initialized == 0) {
    tsdPtr->initialized = 1;
    tsdPtr->leveldb_hashtblPtr = (Tcl_HashTable *) ckalloc(sizeof(Tcl_HashTable));
    Tcl_InitHashTable(tsdPtr->leveldb_hashtblPtr, TCL_STRING_KEYS);
  }
  static const char *SST_strs[] = {
    "close",
    0
  };

  enum SST_enum {
    SST_CLOSE,
  };

  if( objc < 2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }

  if( Tcl_GetIndexFromObj(interp, objv[1], SST_strs, "option", 0, &choice) ){
    return TCL_ERROR;
  }

  /*
   * Get the leveldb::Snapshot value
   */
  sstHandle = Tcl_GetStringFromObj(objv[0], 0);
  hashEntryPtr = Tcl_FindHashEntry( tsdPtr->leveldb_hashtblPtr, sstHandle );
  if( !hashEntryPtr ) {
    if( interp ) {
        Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

        Tcl_AppendStringsToObj( resultObj, "invalid snapshot handle ", sstHandle, (char *)NULL );
    }

    return TCL_ERROR;
  }

  shot = (leveldb::Snapshot *)(uintptr_t)Tcl_GetHashValue( hashEntryPtr );

  switch( (enum SST_enum)choice ){

    case SST_CLOSE: {
      leveldb::DB* db;
      const char *dbiHandle = NULL;
      Tcl_Size len = 0;
      char *zArg;
      int i = 0;
      Tcl_HashEntry *dbHashEntryPtr;

      if( objc != 4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "-db DB_HANDLE ");
        return TCL_ERROR;
      }

      for(i=2; i+1<objc; i+=2){
        zArg = Tcl_GetStringFromObj(objv[i], 0);

        if( strcmp(zArg, "-db")==0 ){
            dbiHandle = Tcl_GetStringFromObj(objv[i+1], &len);
            if(!dbiHandle || len < 1) {
                return TCL_ERROR;
            }
        } else{
           Tcl_AppendResult(interp, "unknown option: ", zArg, (char*)0);
           return TCL_ERROR;
        }
      }

      /*
       * Get the leveldb::DB value
       */

      if(!dbiHandle) {
        if( interp ) {
            Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

            Tcl_AppendStringsToObj( resultObj, "invalid db handle (null) ", dbiHandle, (char *)NULL );
        }

        return TCL_ERROR;
      }

      dbHashEntryPtr = Tcl_FindHashEntry( tsdPtr->leveldb_hashtblPtr, dbiHandle );
      if( !dbHashEntryPtr ) {
        if( interp ) {
            Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

            Tcl_AppendStringsToObj( resultObj, "invalid db handle ", dbiHandle, (char *)NULL );
        }

        return TCL_ERROR;
      }

      db = (leveldb::DB *)(uintptr_t)Tcl_GetHashValue( dbHashEntryPtr );
      db->ReleaseSnapshot(shot);

      Tcl_MutexLock(&myMutex);
      if( hashEntryPtr )  Tcl_DeleteHashEntry(hashEntryPtr);
      Tcl_MutexUnlock(&myMutex);

      Tcl_DeleteCommand(interp, sstHandle);
      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }
  }

  return TCL_OK;
}


static int LEVELDB_BAT(void *cd, Tcl_Interp *interp, int objc,Tcl_Obj *const*objv){
  int choice;
  leveldb::WriteBatch* batch;
  Tcl_HashEntry *hashEntryPtr;
  char *batHandle;

  ThreadSpecificData *tsdPtr = (ThreadSpecificData *)
      Tcl_GetThreadData(&dataKey, sizeof(ThreadSpecificData));

  if (tsdPtr->initialized == 0) {
    tsdPtr->initialized = 1;
    tsdPtr->leveldb_hashtblPtr = (Tcl_HashTable *) ckalloc(sizeof(Tcl_HashTable));
    Tcl_InitHashTable(tsdPtr->leveldb_hashtblPtr, TCL_STRING_KEYS);
  }
  static const char *BAT_strs[] = {
    "put",
    "delete",
    "close",
    0
  };

  enum BAT_enum {
    BAT_PUT,
    BAT_DELETE,
    BAT_CLOSE,
  };

  if( objc < 2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }

  if( Tcl_GetIndexFromObj(interp, objv[1], BAT_strs, "option", 0, &choice) ){
    return TCL_ERROR;
  }

  /*
   * Get the leveldb::WriteBatch value
   */
  batHandle = Tcl_GetStringFromObj(objv[0], 0);
  hashEntryPtr = Tcl_FindHashEntry( tsdPtr->leveldb_hashtblPtr, batHandle );
  if( !hashEntryPtr ) {
    if( interp ) {
        Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

        Tcl_AppendStringsToObj( resultObj, "invalid batch handle ", batHandle, (char *)NULL );
    }

    return TCL_ERROR;
  }

  batch = (leveldb::WriteBatch *)(uintptr_t)Tcl_GetHashValue( hashEntryPtr );

  switch( (enum BAT_enum)choice ){

    case BAT_PUT: {
      const char *key = NULL;
      const char *data = NULL;
      Tcl_Size key_len = 0;
      Tcl_Size data_len = 0;
      leveldb::Slice key2;
      leveldb::Slice value2;

      if( objc != 4 ) {
        Tcl_WrongNumArgs(interp, 2, objv, "key data ");
        return TCL_ERROR;
      }

      key = Tcl_GetStringFromObj(objv[2], &key_len);
      if( !key || key_len < 1 ){
         Tcl_AppendResult(interp, "Error: key is an empty key ", (char*)0);
         return TCL_ERROR;
      }

      data = Tcl_GetStringFromObj(objv[3], &data_len);
      if( !data || data_len < 1 ){
         Tcl_AppendResult(interp, "Error: data is an empty value ", (char*)0);
         return TCL_ERROR;
      }

      key2 = leveldb::Slice(key, key_len);
      value2 = leveldb::Slice(data, data_len);
      batch->Put(key2, value2);
      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case BAT_DELETE: {
      const char *key = NULL;
      Tcl_Size key_len = 0;
      leveldb::Slice key2;

      if( objc  !=  3 ) {
        Tcl_WrongNumArgs(interp, 2, objv, "key ");
        return TCL_ERROR;
      }

      key = Tcl_GetStringFromObj(objv[2], &key_len);
      if( !key || key_len < 1 ){
         Tcl_AppendResult(interp, "Error: key is an empty key ", (char*)0);
         return TCL_ERROR;
      }

      key2 = leveldb::Slice(key, key_len);
      batch->Delete(key2);
      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case BAT_CLOSE: {
      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      delete batch;

      Tcl_MutexLock(&myMutex);
      if( hashEntryPtr )  Tcl_DeleteHashEntry(hashEntryPtr);
      Tcl_MutexUnlock(&myMutex);

      Tcl_DeleteCommand(interp, batHandle);
      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }
  }

  return TCL_OK;
}


static int LEVELDB_ITR(void *cd, Tcl_Interp *interp, int objc,Tcl_Obj *const*objv){
  int choice;
  leveldb::Iterator* it;
  Tcl_HashEntry *hashEntryPtr;
  char *itrHandle;

  ThreadSpecificData *tsdPtr = (ThreadSpecificData *)
      Tcl_GetThreadData(&dataKey, sizeof(ThreadSpecificData));

  if (tsdPtr->initialized == 0) {
    tsdPtr->initialized = 1;
    tsdPtr->leveldb_hashtblPtr = (Tcl_HashTable *) ckalloc(sizeof(Tcl_HashTable));
    Tcl_InitHashTable(tsdPtr->leveldb_hashtblPtr, TCL_STRING_KEYS);
  }

  static const char *ITR_strs[] = {
    "seektofirst",
    "seektolast",
    "seek",
    "valid",
    "next",
    "prev",
    "key",
    "value",
    "close",
    0
  };

  enum ITR_enum {
    ITR_SEEKTOFIRST,
    ITR_SEEKTOLAST,
    ITR_SEEK,
    ITR_VALID,
    ITR_NEXT,
    ITR_PREV,
    ITR_KEY,
    ITR_VALUE,
    ITR_CLOSE,
  };

  if( objc < 2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }

  if( Tcl_GetIndexFromObj(interp, objv[1], ITR_strs, "option", 0, &choice) ){
    return TCL_ERROR;
  }

  /*
   * Get the leveldb::Iterator value
   */
  itrHandle = Tcl_GetStringFromObj(objv[0], 0);
  hashEntryPtr = Tcl_FindHashEntry( tsdPtr->leveldb_hashtblPtr, itrHandle );
  if( !hashEntryPtr ) {
    if( interp ) {
        Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

        Tcl_AppendStringsToObj( resultObj, "invalid iterator handle ", itrHandle, (char *)NULL );
    }

    return TCL_ERROR;
  }

  it = (leveldb::Iterator *)(uintptr_t)Tcl_GetHashValue( hashEntryPtr );

  switch( (enum ITR_enum)choice ){

    case ITR_SEEKTOFIRST: {
      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      it->SeekToFirst();
      if(!it->status().ok()) {
        Tcl_AppendResult(interp, "Error: seektofirst failed", (char*)0);
        return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case ITR_SEEKTOLAST: {
      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      it->SeekToLast();
      if(!it->status().ok()) {
        Tcl_AppendResult(interp, "Error: seektolast failed", (char*)0);
        return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case ITR_SEEK: {
      const char *key = NULL;
      Tcl_Size len = 0;
      leveldb::Slice slice;

      if( objc != 3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "key ");
        return TCL_ERROR;
      }

      key = Tcl_GetStringFromObj(objv[2], &len);
      if(!key || len < 0) {
        Tcl_AppendResult(interp, "Error: key is empty", (char*)0);
        return TCL_ERROR;
      }

      slice = leveldb::Slice(key, len);
      it->Seek(slice);
      if(!it->status().ok()) {
        Tcl_AppendResult(interp, "Error: seek failed", (char*)0);
        return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case ITR_VALID: {
      bool isValid;

      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      isValid = it->Valid();
      if(!it->status().ok()) {
        Tcl_AppendResult(interp, "Error: valid failed", (char*)0);
        return TCL_ERROR;
      }

      if( isValid ) {
          Tcl_SetObjResult(interp, Tcl_NewBooleanObj( 1 ));
      } else {
          Tcl_SetObjResult(interp, Tcl_NewBooleanObj( 0 ));
      }

      break;
    }

    case ITR_NEXT: {
      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      it->Next();
      if(!it->status().ok()) {
        Tcl_AppendResult(interp, "Error: next failed", (char*)0);
        return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case ITR_PREV: {
      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      it->Prev();
      if(!it->status().ok()) {
        Tcl_AppendResult(interp, "Error: prev failed", (char*)0);
        return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case ITR_KEY: {
      std::string key2;
      Tcl_Obj *pResultStr = NULL;

      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      key2 = it->key().ToString();
      if(!it->status().ok()) {
        Tcl_AppendResult(interp, "Error: key failed", (char*)0);
        return TCL_ERROR;
      }

      pResultStr = Tcl_NewStringObj(key2.c_str(), key2.length());
      Tcl_SetObjResult(interp, pResultStr);

      break;
    }

    case ITR_VALUE: {
      std::string value2;
      Tcl_Obj *pResultStr = NULL;

      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      value2 = it->value().ToString();
      if(!it->status().ok()) {
        Tcl_AppendResult(interp, "Error: value failed", (char*)0);
        return TCL_ERROR;
      }

      pResultStr = Tcl_NewStringObj(value2.c_str(), value2.length());
      Tcl_SetObjResult(interp, pResultStr);

      break;
    }

    case ITR_CLOSE: {
      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      delete it;

      Tcl_MutexLock(&myMutex);
      if( hashEntryPtr )  Tcl_DeleteHashEntry(hashEntryPtr);
      Tcl_MutexUnlock(&myMutex);

      Tcl_DeleteCommand(interp, itrHandle);
      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }
  }

  return TCL_OK;
}

static int LEVELDB_DBI(void *cd, Tcl_Interp *interp, int objc,Tcl_Obj *const*objv){
  int choice;
  leveldb::DB* db;
  Tcl_HashEntry *hashEntryPtr;
  char *dbiHandle;

  ThreadSpecificData *tsdPtr = (ThreadSpecificData *)
      Tcl_GetThreadData(&dataKey, sizeof(ThreadSpecificData));

  if (tsdPtr->initialized == 0) {
    tsdPtr->initialized = 1;
    tsdPtr->leveldb_hashtblPtr = (Tcl_HashTable *) ckalloc(sizeof(Tcl_HashTable));
    Tcl_InitHashTable(tsdPtr->leveldb_hashtblPtr, TCL_STRING_KEYS);
  }

  static const char *DBI_strs[] = {
    "get",
    "put",
    "delete",
    "write",
    "batch",
    "iterator",
    "snapshot",
    "getApproximateSizes",
    "getProperty",
    "close",
    0
  };

  enum DBI_enum {
    DBI_GET,
    DBI_PUT,
    DBI_DELETE,
    DBI_WRITE,
    DBI_BATCH,
    DBI_ITERATOR,
    DBI_SNAPSHOT,
    DBI_GETAPPROXIMATESIZES,
    DBI_GETPROPERTY,
    DBI_CLOSE,
  };

  if( objc < 2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }

  if( Tcl_GetIndexFromObj(interp, objv[1], DBI_strs, "option", 0, &choice) ){
    return TCL_ERROR;
  }

  /*
   * Get the leveldb::DB value
   */
  dbiHandle = Tcl_GetStringFromObj(objv[0], 0);
  hashEntryPtr = Tcl_FindHashEntry( tsdPtr->leveldb_hashtblPtr, dbiHandle );
  if( !hashEntryPtr ) {
    if( interp ) {
        Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

        Tcl_AppendStringsToObj( resultObj, "invalid db handle ", dbiHandle, (char *)NULL );
    }

    return TCL_ERROR;
  }

  db = (leveldb::DB *)(uintptr_t)Tcl_GetHashValue( hashEntryPtr );

  switch( (enum DBI_enum)choice ){

    case DBI_GET: {
      leveldb::ReadOptions read_options;
      leveldb::Status status;
      const char *key = NULL;
      Tcl_Size key_len = 0;
      leveldb::Slice key2;
      std::string value2;
      char *zArg;
      int i = 0;
      Tcl_Obj *pResultStr = NULL;
      const leveldb::Snapshot* shot = NULL;
      Tcl_HashEntry *sstHashEntryPtr;
      const char *sstHandle = NULL;
      Tcl_Size sst_length = 0;

      if( objc < 3 || (objc&1)!=1) {
        Tcl_WrongNumArgs(interp, 2, objv, "key ?-fillCache BOOLEAN? ?-snapshot HANDLE? ");
        return TCL_ERROR;
      }

      key = Tcl_GetStringFromObj(objv[2], &key_len);
      if( !key || key_len < 1 ){
         Tcl_AppendResult(interp, "Error: key is an empty key ", (char*)0);
         return TCL_ERROR;
      }

      for(i=3; i+1<objc; i+=2){
        zArg = Tcl_GetStringFromObj(objv[i], 0);

        if( strcmp(zArg, "-fillCache")==0 ){
            int b;
            if( Tcl_GetBooleanFromObj(interp, objv[i+1], &b) ) return TCL_ERROR;
            if( b ){
              read_options.fill_cache = true;
            }else{
              read_options.fill_cache = false;
            }
        } else if( strcmp(zArg, "-snapshot")==0 ){
            sstHandle = Tcl_GetStringFromObj(objv[i+1], &sst_length);

            if( !sstHandle || sst_length < 1) {
              return TCL_ERROR;
            }
        } else{
           Tcl_AppendResult(interp, "unknown option: ", zArg, (char*)0);
           return TCL_ERROR;
        }
      }

      if(sstHandle) {
          sstHashEntryPtr = Tcl_FindHashEntry( tsdPtr->leveldb_hashtblPtr, sstHandle );
          if( !sstHashEntryPtr ) {
            if( interp ) {
                Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

                Tcl_AppendStringsToObj( resultObj, "invalid snapshot handle ", sstHandle, (char *)NULL );
            }

            return TCL_ERROR;
          }

          shot = (leveldb::Snapshot *)(uintptr_t)Tcl_GetHashValue( sstHashEntryPtr );
          read_options.snapshot = shot;
      }

      key2 = leveldb::Slice(key, key_len);

      status = db->Get(read_options, key2, &value2);
      if(!status.ok()) {
        Tcl_AppendResult(interp, "Error: get failed", (char*)0);
        return TCL_ERROR;
      }

      pResultStr = Tcl_NewStringObj(value2.c_str(), value2.length());
      Tcl_SetObjResult(interp, pResultStr);

      break;
    }

    case DBI_PUT: {
      leveldb::Status status;
      leveldb::WriteOptions write_options;
      const char *key = NULL;
      const char *data = NULL;
      Tcl_Size key_len = 0;
      Tcl_Size data_len = 0;
      leveldb::Slice key2;
      leveldb::Slice value2;
      char *zArg;
      int i = 0;

      if( objc < 4 || (objc&1)!=0) {
        Tcl_WrongNumArgs(interp, 2, objv, "key data ?-sync BOOLEAN? ");
        return TCL_ERROR;
      }

      key = Tcl_GetStringFromObj(objv[2], &key_len);
      if( !key || key_len < 1 ){
         Tcl_AppendResult(interp, "Error: key is an empty key ", (char*)0);
         return TCL_ERROR;
      }

      data = Tcl_GetStringFromObj(objv[3], &data_len);
      if( !data || data_len < 1 ){
         Tcl_AppendResult(interp, "Error: data is an empty value ", (char*)0);
         return TCL_ERROR;
      }

      for(i=4; i+1<objc; i+=2){
        zArg = Tcl_GetStringFromObj(objv[i], 0);

        if( strcmp(zArg, "-sync")==0 ){
            int b;
            if( Tcl_GetBooleanFromObj(interp, objv[i+1], &b) ) return TCL_ERROR;
            if( b ){
              write_options.sync = true;
            }else{
              write_options.sync = false;
            }
        } else{
           Tcl_AppendResult(interp, "unknown option: ", zArg, (char*)0);
           return TCL_ERROR;
        }
      }

      key2 = leveldb::Slice(key, key_len);
      value2 = leveldb::Slice(data, data_len);
      status = db->Put(write_options, key2, value2);
      if(!status.ok()) {
        Tcl_AppendResult(interp, "Error: put failed", (char*)0);
        return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case DBI_DELETE: {
      leveldb::Status status;
      leveldb::WriteOptions write_options;
      const char *key = NULL;
      Tcl_Size key_len = 0;
      leveldb::Slice key2;
      char *zArg;
      int i = 0;

      if( objc < 3 || (objc&1)!=1) {
        Tcl_WrongNumArgs(interp, 2, objv, "key ?-sync BOOLEAN? ");
        return TCL_ERROR;
      }

      key = Tcl_GetStringFromObj(objv[2], &key_len);
      if( !key || key_len < 1 ){
         Tcl_AppendResult(interp, "Error: key is an empty key ", (char*)0);
         return TCL_ERROR;
      }

      for(i=3; i+1<objc; i+=2){
        zArg = Tcl_GetStringFromObj(objv[i], 0);

        if( strcmp(zArg, "-sync")==0 ){
            int b;
            if( Tcl_GetBooleanFromObj(interp, objv[i+1], &b) ) return TCL_ERROR;
            if( b ){
              write_options.sync = true;
            }else{
              write_options.sync = false;
            }
        } else{
           Tcl_AppendResult(interp, "unknown option: ", zArg, (char*)0);
           return TCL_ERROR;
        }
      }

      key2 = leveldb::Slice(key, key_len);

      status = db->Delete(write_options, key2);
      if(!status.ok()) {
        Tcl_AppendResult(interp, "Error: delete failed", (char*)0);
        return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case DBI_WRITE: {
      leveldb::WriteBatch *batch;
      leveldb::Status status;
      Tcl_HashEntry *hashEntryPtr;
      const char *batch_handle = NULL;
      Tcl_Size length = 0;

      if( objc != 3 ) {
        Tcl_WrongNumArgs(interp, 2, objv, "batch_handle ");
        return TCL_ERROR;
      }

      batch_handle = Tcl_GetStringFromObj(objv[2], &length);
      hashEntryPtr = Tcl_FindHashEntry( tsdPtr->leveldb_hashtblPtr, batch_handle );
      if( !hashEntryPtr ) {
        if( interp ) {
            Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

            Tcl_AppendStringsToObj( resultObj, "invalid batch handle ", batch_handle, (char *)NULL );
        }

        return TCL_ERROR;
      }

      batch = (leveldb::WriteBatch *)(uintptr_t)Tcl_GetHashValue( hashEntryPtr );
      status = db->Write(leveldb::WriteOptions(), batch);
      if(!status.ok()) {
        Tcl_AppendResult(interp, "Error: write failed", (char*)0);
        return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case DBI_BATCH: {
      leveldb::WriteBatch *batch;
      Tcl_HashEntry *newHashEntryPtr;
      char handleName[16 + TCL_INTEGER_SPACE];
      Tcl_Obj *pResultStr = NULL;
      int newvalue;

      if( objc != 2 ) {
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      batch = new leveldb::WriteBatch();

      Tcl_MutexLock(&myMutex);
      sprintf( handleName, "levelbat%d", tsdPtr->bat_count++ );

      pResultStr = Tcl_NewStringObj( handleName, -1 );

      newHashEntryPtr = Tcl_CreateHashEntry(tsdPtr->leveldb_hashtblPtr, handleName, &newvalue);
      Tcl_SetHashValue(newHashEntryPtr, (ClientData)(uintptr_t) batch);
      Tcl_MutexUnlock(&myMutex);

      Tcl_CreateObjCommand(interp, handleName, (Tcl_ObjCmdProc *) LEVELDB_BAT,
          (ClientData)NULL, (Tcl_CmdDeleteProc *)NULL);

      Tcl_SetObjResult(interp, pResultStr);

      break;
    }

    case DBI_ITERATOR: {
      leveldb::ReadOptions read_options;
      Tcl_HashEntry *newHashEntryPtr;
      char handleName[16 + TCL_INTEGER_SPACE];
      Tcl_Obj *pResultStr = NULL;
      int newvalue;
      const leveldb::Snapshot* shot = NULL;
      Tcl_HashEntry *sstHashEntryPtr;
      const char *sstHandle = NULL;
      Tcl_Size sst_length = 0;
      char *zArg;
      int i = 0;

      if( objc < 2 || (objc&1)!=0) {
        Tcl_WrongNumArgs(interp, 2, objv, "?-snapshot HANDLE? ");
        return TCL_ERROR;
      }

      for(i=2; i+1<objc; i+=2){
        zArg = Tcl_GetStringFromObj(objv[i], 0);

        if( strcmp(zArg, "-snapshot")==0 ){
            sstHandle = Tcl_GetStringFromObj(objv[i+1], &sst_length);

            if( !sstHandle || sst_length < 1) {
              return TCL_ERROR;
            }
        } else{
           Tcl_AppendResult(interp, "unknown option: ", zArg, (char*)0);
           return TCL_ERROR;
        }
      }

      if(sstHandle) {
          sstHashEntryPtr = Tcl_FindHashEntry( tsdPtr->leveldb_hashtblPtr, sstHandle );
          if( !sstHashEntryPtr ) {
            if( interp ) {
                Tcl_Obj *resultObj = Tcl_GetObjResult( interp );

                Tcl_AppendStringsToObj( resultObj, "invalid snapshot handle ", sstHandle, (char *)NULL );
            }

            return TCL_ERROR;
          }

          shot = (leveldb::Snapshot *)(uintptr_t)Tcl_GetHashValue( sstHashEntryPtr );
          read_options.snapshot = shot;
      }

      leveldb::Iterator* it = db->NewIterator(read_options);

      Tcl_MutexLock(&myMutex);
      sprintf( handleName, "levelitr%d", tsdPtr->itr_count++ );

      pResultStr = Tcl_NewStringObj( handleName, -1 );

      newHashEntryPtr = Tcl_CreateHashEntry(tsdPtr->leveldb_hashtblPtr, handleName, &newvalue);
      Tcl_SetHashValue(newHashEntryPtr, (ClientData)(uintptr_t) it);
      Tcl_MutexUnlock(&myMutex);

      Tcl_CreateObjCommand(interp, handleName, (Tcl_ObjCmdProc *) LEVELDB_ITR,
          (ClientData)NULL, (Tcl_CmdDeleteProc *)NULL);

      Tcl_SetObjResult(interp, pResultStr);

      break;
    }


    case DBI_SNAPSHOT: {
      Tcl_HashEntry *newHashEntryPtr;
      char handleName[16 + TCL_INTEGER_SPACE];
      Tcl_Obj *pResultStr = NULL;
      int newvalue;
      const leveldb::Snapshot* shot;

      if( objc != 2 ) {
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      shot = db->GetSnapshot();

      Tcl_MutexLock(&myMutex);
      sprintf( handleName, "levelsst%d", tsdPtr->sst_count++ );

      pResultStr = Tcl_NewStringObj( handleName, -1 );

      newHashEntryPtr = Tcl_CreateHashEntry(tsdPtr->leveldb_hashtblPtr, handleName, &newvalue);
      Tcl_SetHashValue(newHashEntryPtr, (ClientData)(uintptr_t) shot);
      Tcl_MutexUnlock(&myMutex);

      Tcl_CreateObjCommand(interp, handleName, (Tcl_ObjCmdProc *) LEVELDB_SST,
          (ClientData)NULL, (Tcl_CmdDeleteProc *)NULL);

      Tcl_SetObjResult(interp, pResultStr);

      break;
    }


    case DBI_GETAPPROXIMATESIZES: {
      const char *start = NULL;
      Tcl_Size start_len = 0;
      const char *limit = NULL;
      Tcl_Size limit_len = 0;
      leveldb::Slice start2;
      leveldb::Slice limit2;
      uint64_t sizes;
      Tcl_Obj *pResultStr = NULL;

      if( objc != 4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "start limit ");
        return TCL_ERROR;
      }

      start = Tcl_GetStringFromObj(objv[2], &start_len);
      if( !start || start_len < 1 ){
         Tcl_AppendResult(interp, "Error: start is an empty string ", (char*)0);
         return TCL_ERROR;
      }

      limit = Tcl_GetStringFromObj(objv[3], &limit_len);
      if( !limit || limit_len < 1 ){
         Tcl_AppendResult(interp, "Error: limit is an empty string ", (char*)0);
         return TCL_ERROR;
      }

      start2 = leveldb::Slice(start, start_len);
      limit2 = leveldb::Slice(limit, limit_len);

      leveldb::Range ranges = leveldb::Range(start2, limit2);
      db->GetApproximateSizes(&ranges, 1, &sizes);

      pResultStr = Tcl_NewWideIntObj( (Tcl_WideInt) sizes);
      Tcl_SetObjResult(interp, pResultStr);

      break;
    }

    case DBI_GETPROPERTY: {
      const char *key = NULL;
      Tcl_Size key_len = 0;
      leveldb::Slice property;
      std::string value2;
      bool result;
      Tcl_Obj *pResultStr = NULL;

      if( objc != 3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "property ");
        return TCL_ERROR;
      }

      key = Tcl_GetStringFromObj(objv[2], &key_len);
      if( !key || key_len < 1 ){
         Tcl_AppendResult(interp, "Error: property is an empty string ", (char*)0);
         return TCL_ERROR;
      }

      property = leveldb::Slice(key, key_len);
      result = db->GetProperty(property, &value2);

      if(!result) {
         Tcl_AppendResult(interp, "Error: is not a valid property ", (char*)0);
         return TCL_ERROR;
      }

      pResultStr = Tcl_NewStringObj(value2.c_str(), value2.length());
      Tcl_SetObjResult(interp, pResultStr);

      break;
    }

    case DBI_CLOSE: {
      if( objc != 2 ){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      delete db;

      Tcl_MutexLock(&myMutex);
      if( hashEntryPtr )  Tcl_DeleteHashEntry(hashEntryPtr);
      Tcl_MutexUnlock(&myMutex);

      Tcl_DeleteCommand(interp, dbiHandle);
      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }
  }

  return TCL_OK;
}


static int LEVELDB_MAIN(void *cd, Tcl_Interp *interp, int objc,Tcl_Obj *const*objv){
  int choice;

  ThreadSpecificData *tsdPtr = (ThreadSpecificData *)
      Tcl_GetThreadData(&dataKey, sizeof(ThreadSpecificData));

  if (tsdPtr->initialized == 0) {
    tsdPtr->initialized = 1;
    tsdPtr->leveldb_hashtblPtr = (Tcl_HashTable *) ckalloc(sizeof(Tcl_HashTable));
    Tcl_InitHashTable(tsdPtr->leveldb_hashtblPtr, TCL_STRING_KEYS);
  }

  static const char *DB_strs[] = {
    "open",
    "repair",
    "destroy",
    "version",
    0
  };

  enum DB_enum {
    DB_OPEN,
    DB_REPAIR,
    DB_DESTROY,
    DB_VERSION,
  };

  if( objc < 2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }

  if( Tcl_GetIndexFromObj(interp, objv[1], DB_strs, "option", 0, &choice) ){
    return TCL_ERROR;
  }

  switch( (enum DB_enum)choice ){

    case DB_OPEN: {
      char *zArg;
      leveldb::DB* db;
      leveldb::Options options;
      leveldb::Status status;
      Tcl_HashEntry *newHashEntryPtr;
      const char *path = NULL;
      Tcl_Size len;
      char handleName[16 + TCL_INTEGER_SPACE];
      Tcl_Obj *pResultStr = NULL;
      int newvalue;
      int i = 0;

      if( objc < 4 || (objc&1)!=0 ){
          Tcl_WrongNumArgs(interp, 2, objv,
          "-path path ?-create_if_missing BOOLEAN? ?-error_if_exists BOOLEAN? \
           ?-paranoid_checks BOOLEAN? ?-write_buffer_size size? \
           ?-max_open_files number? ?-block_size size? ?-compression type? "
          );

        return TCL_ERROR;
      }

      for(i=2; i+1<objc; i+=2){
        zArg = Tcl_GetStringFromObj(objv[i], 0);

        if( strcmp(zArg, "-path")==0 ){
            path = Tcl_GetStringFromObj(objv[i+1], &len);
            if(!path || len < 1) {
                return TCL_ERROR;
            }
        } else if( strcmp(zArg, "-create_if_missing")==0 ){
            int b;
            if( Tcl_GetBooleanFromObj(interp, objv[i+1], &b) ) return TCL_ERROR;
            if( b ){
              options.create_if_missing = true;
            }else{
              options.create_if_missing = false;
            }
        } else if( strcmp(zArg, "-error_if_exists")==0 ){
            int b;
            if( Tcl_GetBooleanFromObj(interp, objv[i+1], &b) ) return TCL_ERROR;
            if( b ){
              options.error_if_exists = true;
            }else{
              options.error_if_exists = false;
            }
        } else if( strcmp(zArg, "-paranoid_checks")==0 ){
            int b;
            if( Tcl_GetBooleanFromObj(interp, objv[i+1], &b) ) return TCL_ERROR;
            if( b ){
              options.paranoid_checks = true;
            }else{
              options.paranoid_checks = false;
            }
        } else if( strcmp(zArg, "-write_buffer_size")==0 ){
            int size = 0;

            if(Tcl_GetIntFromObj(interp, objv[i+1], &size) != TCL_OK) {
                return TCL_ERROR;
            }

            if( size > 0 ) {
              options.write_buffer_size = size;
            }
        } else if( strcmp(zArg, "-max_open_files")==0 ){
            int number = 0;

            if(Tcl_GetIntFromObj(interp, objv[i+1], &number) != TCL_OK) {
                return TCL_ERROR;
            }

            if(number < 0) {
                options.max_open_files = -1;
            } else {
                options.max_open_files = number;
            }
        } else if( strcmp(zArg, "-block_size")==0 ){
            int size = 0;

            if(Tcl_GetIntFromObj(interp, objv[i+1], &size) != TCL_OK) {
                return TCL_ERROR;
            }

            if(size > 0) {
                options.block_size = size;
            }
        } else if( strcmp(zArg, "-compression")==0 ){
            const char *compression = NULL;
            Tcl_Size clength = 0;

            compression = Tcl_GetStringFromObj(objv[i+1], &clength);
            if(!compression || clength <= 0) {
                return TCL_ERROR;
            }

            if(!strcmp("no", compression)) {
                options.compression = leveldb::kNoCompression;
            } else if(!strcmp("snappy", compression)) {
                options.compression = leveldb::kSnappyCompression;
            } else {
                return TCL_ERROR;
            }
        } else{
           Tcl_AppendResult(interp, "unknown option: ", zArg, (char*)0);
           return TCL_ERROR;
        }
      }

      if(!path) {
          if( interp ) {
            Tcl_Obj *resultObj = Tcl_GetObjResult( interp );
            Tcl_AppendStringsToObj( resultObj, "No database path", (char *)NULL );
          }

          return TCL_ERROR;
      }

      status = leveldb::DB::Open(options, path, &db);

      if(!status.ok()) {
          if( interp ) {
            Tcl_Obj *resultObj = Tcl_GetObjResult( interp );
            Tcl_AppendStringsToObj( resultObj, "ERROR: open failed", (char *)NULL );
          }

          return TCL_ERROR;
      }

      Tcl_MutexLock(&myMutex);
      sprintf( handleName, "leveldbi%d", tsdPtr->dbi_count++ );

      pResultStr = Tcl_NewStringObj( handleName, -1 );

      newHashEntryPtr = Tcl_CreateHashEntry(tsdPtr->leveldb_hashtblPtr, handleName, &newvalue);
      Tcl_SetHashValue(newHashEntryPtr, (ClientData)(uintptr_t) db);
      Tcl_MutexUnlock(&myMutex);


      Tcl_CreateObjCommand(interp, handleName, (Tcl_ObjCmdProc *) LEVELDB_DBI,
          (ClientData)NULL, (Tcl_CmdDeleteProc *)NULL);

      Tcl_SetObjResult(interp, pResultStr);

      break;
    }

    case DB_REPAIR: {
      leveldb::Options options;
      leveldb::Status status;
      const char *name;
      Tcl_Size name_len = 0;
      std::string name2;

      if( objc != 3){
        Tcl_WrongNumArgs(interp, 2, objv, "name ");
        return TCL_ERROR;
      }

      name = Tcl_GetStringFromObj(objv[2], &name_len);
      if(!name || name_len < 1) {
          return TCL_ERROR;
      }

      name2 = name;

      /*
       * TODO:
       * I don't know how to test this method, so this method does not test actually.
       */
      status = leveldb::RepairDB(name2, options);
      if(!status.ok()) {
          if( interp ) {
            Tcl_Obj *resultObj = Tcl_GetObjResult( interp );
            Tcl_AppendStringsToObj( resultObj, "ERROR: repair failed", (char *)NULL );
          }

          return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case DB_DESTROY: {
      leveldb::Options options;
      leveldb::Status status;
      const char *name;
      Tcl_Size name_len = 0;
      std::string name2;

      if( objc != 3){
        Tcl_WrongNumArgs(interp, 2, objv, "name ");
        return TCL_ERROR;
      }

      name = Tcl_GetStringFromObj(objv[2], &name_len);
      if(!name || name_len < 1) {
          return TCL_ERROR;
      }

      name2 = name;

      status = leveldb::DestroyDB(name2, options);
      if(!status.ok()) {
          if( interp ) {
            Tcl_Obj *resultObj = Tcl_GetObjResult( interp );
            Tcl_AppendStringsToObj( resultObj, "ERROR: destroy failed", (char *)NULL );
          }

          return TCL_ERROR;
      }

      Tcl_SetObjResult(interp, Tcl_NewIntObj( 0 ));

      break;
    }

    case DB_VERSION: {
      int major = 0, minor = 0;
      Tcl_Obj *pResultStr = NULL;

      if( objc != 2){
        Tcl_WrongNumArgs(interp, 2, objv, 0);
        return TCL_ERROR;
      }

      major = leveldb::kMajorVersion;
      minor = leveldb::kMinorVersion;

      pResultStr = Tcl_NewListObj(0, NULL);
      Tcl_ListObjAppendElement(interp, pResultStr, Tcl_NewIntObj(major));
      Tcl_ListObjAppendElement(interp, pResultStr, Tcl_NewIntObj(minor));

      Tcl_SetObjResult(interp,  pResultStr);

      break;
    }
  }

  return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * Leveldb_Init --
 *
 *	Initialize the new package.
 *
 * Results:
 *	A standard Tcl result
 *
 * Side effects:
 *	The LevelDB package is created.
 *
 *----------------------------------------------------------------------
 */

int
Leveldb_Init(Tcl_Interp *interp)
{
    if (Tcl_InitStubs(interp, TCL_VERSION, 0) == NULL) {
	return TCL_ERROR;
    }

    if (Tcl_PkgProvide(interp, PACKAGE_NAME, PACKAGE_VERSION) != TCL_OK) {
	return TCL_ERROR;
    }

    /*
     *  Tcl_GetThreadData handles the auto-initialization of all data in
     *  the ThreadSpecificData to NULL at first time.
     */
    Tcl_MutexLock(&myMutex);
    ThreadSpecificData *tsdPtr = (ThreadSpecificData *)
        Tcl_GetThreadData(&dataKey, sizeof(ThreadSpecificData));

    if (tsdPtr->initialized == 0) {
        tsdPtr->initialized = 1;
        tsdPtr->leveldb_hashtblPtr = (Tcl_HashTable *) ckalloc(sizeof(Tcl_HashTable));
        Tcl_InitHashTable(tsdPtr->leveldb_hashtblPtr, TCL_STRING_KEYS);

        tsdPtr->dbi_count = 0;
        tsdPtr->itr_count = 0;
        tsdPtr->bat_count = 0;
        tsdPtr->sst_count = 0;
    }
    Tcl_MutexUnlock(&myMutex);

    /* Add a thread exit handler to delete hash table */
    Tcl_CreateThreadExitHandler(LEVELDB_Thread_Exit, (ClientData)NULL);

    Tcl_CreateObjCommand(interp, "leveldb", (Tcl_ObjCmdProc *) LEVELDB_MAIN,
        (ClientData)NULL, (Tcl_CmdDeleteProc *)NULL);

    return TCL_OK;
}
