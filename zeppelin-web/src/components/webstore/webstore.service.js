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
angular.module('zeppelinWebApp').service('webstore', WebstoreService);

// import {Webstore} from '../../webstore';
import {WebRpc, Session} from '../../webrpc';
import {Webstore} from '../../webstore';

class ZeppelinWebsocketSession extends Session {
  constructor(websocketEvents) {
    super();

    websocketEvents._webrpcSession = this;
    this.websocketEvents = websocketEvents;
  }

  open() {
    // WebsocketEventFactory handles open
  }

  close() {
    // WebsocketEventFactory handles open
  }

  isConnected() {
    this.websocketEvents.isConnected();
  }

  invoke(invokeId, rpcName, methodName, params, types) {
    const m = JSON.stringify({
      invokeId,
      rpcName,
      methodName,
      params,
      types,
    });

    this.websocketEvents.sendNewEvent({
      op: 'RPC_INVOKE',
      data: m,
    });
  }

  onMessage(payload) {
    const {
      op,
      data,
    } = payload;

    switch (op) {
      case 'RPC_INVOKE':
        this._callbacks.onInvoke(data.rpcName, data.methodName, data.params);
        break;
      case 'RPC_RETURN':
        this._callbacks.onReturn(data.invokeId, data.value);
        break;
      case 'RPC_EXCEPTION':
        this._callbacks.onException(data.invokeId, data.errorMessage, data.exception);
        break;
    }
  }
}

function WebstoreService($rootScope, websocketEvents) {
  // let session = new WebsocketSession("ws://localhost:18543/webrpc")
  // session.open()
  // let webstore = new Webstore(new WebRpc(session))

  console.warn('INIT WEBSOCKET SERVICE ----------------', websocketEvents);

  let session = new ZeppelinWebsocketSession(websocketEvents);
  this.store = new Webstore(new WebRpc(session));
}
