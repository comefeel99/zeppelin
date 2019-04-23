/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import JP from 'jsonpath/jsonpath';

const WEBSTORE_RPC_NAME = 'webstore';

class DocOps {
  constructor(rpc, collection, docId) {
    this.collection = collection;
    this.docId = docId;
    this.ops = [];
    this.rpc = rpc;
  }

  set(path, value) {
    this.ops.push({
      type: 'SET',
      path,
      value,
    });
    return this;
  }

  setDate(path) {
    this.ops.push({
      type: 'SET_DATE',
      path,
    });
    return this;
  }

  get(path) {
    this.ops.push({
      type: 'GET',
      path,
    });
    return this;
  }

  move(fromPath, toPath) {
    this.ops.push({
      type: 'MOVE',
      path: fromPath,
      value: toPath,
    });
    return this;
  }

  delete(path) {
    this.ops.push({
      type: 'DELETE',
      path,
    });
    return this;
  }

  apply(actionName) {
    return this.rpc.invoke(
      WEBSTORE_RPC_NAME,
      'apply',
      [
        this.collection,
        this.docId,
        this.ops,
        actionName,
      ]
    );
  }
}

class DocRef {
  constructor(rpc, collection, docId, watchCallbacks) {
    this.rpc = rpc;
    this.collection = collection;
    this.docId = docId;
    this.watchCallbacks = watchCallbacks;
  }

  /**
   * Get (nested) json object.
   * @param jsonPath e.g. $.user.value
   * return promise
   */
  get(jsonPath) {
    return new DocOps(this.rpc, this.collection, this.docId).get(jsonPath);
  }

  set(jsonPath, value) {
    return new DocOps(this.rpc, this.collection, this.docId).set(jsonPath, value);
  }

  setDate(jsonPath) {
    return new DocOps(this.rpc, this.collection, this.docId).setDate(jsonPath);
  }

  delete(jsonPath) {
    return new DocOps(this.rpc, this.collection, this.docId).delete(jsonPath);
  }

  move(fromPath, toPath) {
    return new DocOps(this.rpc, this.collection, this.docId).move(fromPath, toPath);
  }

  watch(jsonPath, callback) {
    const watchId = this._generateWatchId();
    const watchCallbackKey = [this.collection, this.docId, jsonPath].join('/');
    let watchCallback = this.watchCallbacks[watchCallbackKey];
    if (!watchCallback) {
      watchCallback = {
        watchId,
        collection: this.collection,
        docId: this.docId,
        jsonPath,
        watches: {},
        value: undefined,  // cached value
      };
      this.watchCallbacks[watchCallbackKey] = watchCallback;

      watchCallback.watches[watchId] = {
        collection: this.collection,
        docId: this.docId,
        path: jsonPath,
        callback,
      };

      this.rpc.invoke(
        WEBSTORE_RPC_NAME,
        'watch',
        [
          watchId,
          this.collection,
          this.docId,
          jsonPath,
        ]
      ).catch((e) => {
        throw e;
      });
    } else {
      watchCallback.watches[watchId] = {
        collection: this.collection,
        docId: this.docId,
        path: jsonPath,
        callback,
      };
      callback(watchCallback.value, this.collection, this.docId, jsonPath);
    }

    const self = this;
    return function() {
      self._removeWatch(watchId);
    };
  }

  _removeWatch(watchId) {
    Object.keys(this.watchCallbacks).forEach((key) => {
      const watchCallback = this.watchCallbacks[key];
      delete watchCallback.watches[watchId];
      if (Object.keys(watchCallback.watches).length === 0) {
        delete this.watchCallbacks[key];
        this.rpc.invoke(
          WEBSTORE_RPC_NAME,
          'removeWatch',
          [
            watchCallback.watchId,
          ]
        );
      }
    });
  }


  /**
   * Generate random watch id
   * @returns {string}
   * @private
   */
  _generateWatchId() {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

    for (let i = 0; i < 7; i++) {
      text += possible.charAt(Math.floor(Math.random() * possible.length));
    }

    return text;
  }
}

class LocalCache {
  constructor() {
    this.cache = {}; // <collection, <docId, {paths:[jsonPath], doc:{}}>>
  }

  apply(ops) {
    let ret;
    ops.ops.forEach((op) => {
      if (op.type === 'GET') {
        ret = this.get(ops.collection, ops.docId, op.path);
      } else if (op.type === 'SET') {
        ret = this.set(ops.collection, ops.docId, op.path, op.value);
      } else if (op.type === 'SET_DATE') {
        ret = this.setDate(ops.collection, ops.docId, op.path);
      } else if (op.type === 'MOVE') {
        ret = this.move(ops.collection, ops.docId, op.path, op.value);
      } else if (op.type === 'DELETE') {
        ret = this.delete(ops.collection, ops.docId, op.path);
      }
    });
    return ret;
  }

  // return null when no cached value available
  _getCacheEntry(collection, docId) {
    if (!this.cache[collection]) {
      this.cache[collection] = {};
    }

    if (!this.cache[collection][docId]) {
      this.cache[collection][docId] = {
        paths: [],
        doc: {},
      };
    }

    return this.cache[collection][docId];
  }

  _isJsonPathCached(cacheEntry, jsonPath) {
    return cacheEntry.paths.find((path) => {
      return jsonPath.startsWith(path) || path.startsWith(jsonPath);
    });
  }

  // return null when no cached value available
  get(collection, docId, jsonPath) {
    const cacheEntry = this._getCacheEntry(collection, docId);
    const cached = this._isJsonPathCached(cacheEntry, jsonPath);
    if (cached) {
      return JP.query(cacheEntry.doc, jsonPath)[0];
    }
    return null;
  }

  // update cache
  set(collection, docId, jsonPath, value) {
    const cacheEntry = this._getCacheEntry(collection, docId);
    const previousValue = this.get(collection, docId, jsonPath);

    const reducedPaths = cacheEntry.paths.filter((path) => {
      return jsonPath.startsWith(path);
    });

    if (reducedPaths.length === 0) {
      cacheEntry.paths.push(jsonPath);
    }

    if (value === undefined) {
      const path = JP.paths(cacheEntry.doc, jsonPath)[0];
      delete JP.parent(cacheEntry.doc, jsonPath)[path[path.length - 1]];
    } else {
      JP.value(cacheEntry.doc, jsonPath, value);
    }

    return previousValue;
  }

  // update cache
  setDate(collection, docId, jsonPath) {
    return this.set(collection, docId, jsonPath, new Date());
  }

  // move
  move(collection, docId, fromPath, toPath) {
    const objectToMove = this.delete(collection, docId, fromPath);
    if (!objectToMove) {
      return null;
    }
    return this.set(collection, docId, toPath, objectToMove);
  }

  // update cache
  delete(collection, docId, jsonPath) {
    return this.set(collection, docId, jsonPath, undefined);
  }

}

/* eslint-disable no-unused-vars */
class LocalServer {
  constructor(webstore) {
    this.storage = new LocalCache();
    this.webstore = webstore;
    this.watches = {};
  }

  invoke(rpcName, methodName, param) {
    if (methodName === 'apply') {
      const [collection, docId, ops, actionName] = param;
      return new Promise((resolve, reject) => {
        const ret = this.storage.apply({
          collection,
          docId,
          ops,
        });

        // call watcher
        const pathReducer = (p1, p2) => {
          const array = [p1, p2];
          if (!(p1 && p2)) {
            return p1 || p2;
          }
          const A = array.concat().sort();
          const a1 = A[0];
          const a2 = A[A.length-1];
          const L = a1.length;
          let i = 0;
          while (i < L && a1.charAt(i) === a2.charAt(i)) {
            i++;
          }
          return a1.substring(0, i);
        };
        const watchPath = ops.filter((op) => op.type !== 'GET').map((op) => op.path).reduce(pathReducer, undefined);
        if (watchPath) {
          this.callWatcher(watchPath);
        }

        resolve(ret);
      });
    } else if (methodName === 'watch') {
      const [watchId, collection, docId, jsonPath] = param;
      return new Promise((resolve, reject) => {
        this.watches[watchId] = {
          watchId,
          collection,
          docId,
          jsonPath,
        };
        this.callWatcher(jsonPath);
        resolve();
      });
    } else if (methodName === 'removeWatch') {
      const [watchId] = param;
      return new Promise((resolve, reject) => {
        delete this.watches[watchId];
        resolve();
      });
    }
  }

  callWatcher(watchPath) {
    Object.keys(this.watches).map((k) => this.watches[k])
      .filter((w) => w.jsonPath.startsWith(watchPath) || watchPath.startsWith(w.jsonPath))
      .forEach((w) => {
        const value = this.storage.get(w.collection, w.docId, w.jsonPath);
        if (value !== null) {
          this.webstore.onChange(
            w.watchId,
            w.collection,
            w.docId,
            w.jsonPath,
            value);
        }
      });
  }

  register() {}
}
/* eslint-enable no-unused-vars */

/**
 * Webstore client.
 */
export class Webstore {
  constructor(webrpc) {
    this.watchCallbacks = {};
    this.rpc = webrpc || new LocalServer(this);
    this.rpc.register(WEBSTORE_RPC_NAME, this);
    const reduxDevtoolsExtension = window.__REDUX_DEVTOOLS_EXTENSION__;
    if (reduxDevtoolsExtension) {
      this.devTool = reduxDevtoolsExtension.connect({name: 'Webstore'});
      this.stateCache = {};   // caching object for debug tool support
    }

    this._devMode = false;
  }

  /**
   * Get DocRef.
   */
  getRef(collection, docId) {
    return new DocRef(this.rpc, collection, docId, this.watchCallbacks);
  }

  /**
   * Find DocRef in range by docId (inclusive)
   * @param collection
   * @param startDocId
   * @param endDocId
   */
  findRef(collection, startDocId, endDocId) {
    return new Promise((resolve, reject) => {
      this.rpc.invoke(
        WEBSTORE_RPC_NAME,
        'findDocId',
        [
          collection,
          startDocId,
          endDocId,
        ]
      ).then((idList) => {
        resolve(idList.map((id) => new DocRef(collection, id, this.watchCallbacks)));
      }).catch((e) => {
        reject(e);
      });
    });
  }

  /**
   * on watched doc changes.
   * Server will call this method using webrpc.
   *
   * @param watchId
   * @param collectionId
   * @param docId
   * @param jsonPath
   * @param value
   */
  onChange(watchId, collectionId, docId, jsonPath, value) {
    const watchCallback = this.watchCallbacks[[collectionId, docId, jsonPath].join('/')];
    if (watchCallback) {
      watchCallback.value = value;
      Object.keys(watchCallback.watches).map((k) => watchCallback.watches[k]).forEach((watch) => {
        watch.callback(value, collectionId, docId, jsonPath);
      });
    } else {
      console.error('Callback does not exists for watch ', watchId);
    }
  }

  /**
   * Development mode support.
   * Display document change information in developer console.
   */
  onChangeDocDevMode(collection, docId, actionName, changes) {
    if (!this._devMode) {
      return;
    }

    const cloneAndClean = (json) => {
      if (!json) {
        return json;
      }
      const o = JSON.parse(JSON.stringify(json));
      delete o._rev;
      delete o._id;
      return o;
    };

    const entireDocChange = changes[changes.length - 1];
    const before = cloneAndClean(entireDocChange.before);
    const after = cloneAndClean(entireDocChange.op.value);
    const printableActionName = (actionName === null) ? 'SET' : actionName;

    /* eslint-disable no-console */
    console.groupCollapsed(
      ` %caction:webstore %c${printableActionName}`,
      'color: gray; font-weight: lighter;',
      'color: black'
    );
    console.log(' %cCollection = %o, %cDocId = %o',
      'font-weight: bold', collection, 'font-weight: bold', docId);
    console.log(' %cprev state %o', 'font-weight: bold; color: gray;', before);
    console.log(' %coperations %o', 'font-weight: bold; color: #1AA0EE;',
      changes.slice(0, changes.length - 1).map((c) => c.op));
    console.log(' %cnext state %o', 'font-weight: bold; color: #47A34A;', after);
    console.groupEnd();
    /* eslint-enable no-console */

    // redux dev tool
    if (this.devTool) {
      if (!this.stateCache[collection]) {
        this.stateCache[collection] = {};
      }
      this.stateCache[collection][docId] = after;
      this.devTool.send(printableActionName, this.stateCache);
    }
  }

  devMode(onOff) {
    this._devMode = onOff;
  }

  /**
   * re-register all watches.
   * This method will called when websocket is (re)connected
   */
  registerAllWatches() {
    Object.keys(this.watchCallbacks).map((k) => this.watchCallbacks[k]).forEach((w) => {
      this.rpc.invoke(
        WEBSTORE_RPC_NAME,
        'watch',
        [
          w.watchId,
          w.collection,
          w.docId,
          w.jsonPath,
        ]
      ).catch((e) => {
        throw e;
      });
    });
  }
}
