import {Session} from '../session.js';

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
export class WebsocketSession extends Session {
  constructor(url) {
    super();
    this.url = url;
    this._invokeQueue = [];
  }

  create() {
    this.socket = new WebSocket(this.url);
  }

  open() {
    this.create();
    this.socket.onopen = () => {
      this._onOpen.call(this);
    };
    this.socket.onclose = this._callbacks.onClose;
    this.socket.onerror = this._callbacks.onClose;
    this.socket.onmessage = (event) => {
      const m = JSON.parse(event.data);
      switch (m.type) {
        case 'RPC_INVOKE':
          this._callbacks.onInvoke(m.rpcName, m.methodName, m.params);
          break;
        case 'RPC_RETURN':
          this._callbacks.onReturn(m.invokeId, m.value);
          break;
        case 'RPC_EXCEPTION':
          this._callbacks.onException(m.invokeId, m.errorMessage, m.exception);
          break;
      }
    };
  }

  close() {
    this.socket.close();
  }

  isConnected() {
    return this.socket.readyState === this.socket.OPEN;
  }

  invoke(invokeId, rpcName, methodName, params, types) {
    const m = JSON.stringify({
      invokeId,
      rpcName,
      methodName,
      params,
      types,
    });

    if (this.isConnected()) {
      this.socket.send(m);
    } else {
      this._invokeQueue.push(m);
    }
  }

  _onOpen() {
    this._invokeQueue.forEach((m) => this.socket.send(m));
    this._invokeQueue = [];
    this._callbacks.onOpen();
  }
}
