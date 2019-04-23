/*
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
export {Session} from './transport/session.js';
export {WebsocketSession} from './transport/ws/websocket_session.js';

export class WebRpc {
  constructor(session) {
    this.rpcRequestMap = {};
    this.rpcFunctionMap = {};
    this.setSession(session);
  }

  setSession(session) {
    this.session = session;
    session.on('onReturn', (...params) => this._onReturn.call(this, ...params));
    session.on('onException', (...params) => this._onException.call(this, ...params));
    session.on('onInvoke', (...params) => this._onInvoke.call(this, ...params));
  }

  /**
   * Invoke a method with parameter type inference.
   */
  invoke(rpcName, methodName, params, types) {
    return new Promise((resolve, reject) => {
      const invokeId = this._generateInvokeId();
      this.rpcRequestMap[invokeId] = {
        resolve,
        reject,
      };
      this.session.invoke(invokeId, rpcName, methodName, params, types);
    });
  }

  register(rpcName, rpcObject) {
    this.rpcFunctionMap[rpcName] = rpcObject;
  }

  unregister(rpcName) {
    delete this.rpcFunctionMap[rpcName];
  }

  /**
   * This method is called when server send invoke request.
   * You don't have to call this method manually.
   * @param payload
   */
  _onInvoke(rpcName, methodName, params) {
    const rpcObject = this.rpcFunctionMap[rpcName];
    if (rpcObject && rpcObject[methodName]) {
      rpcObject[methodName](...params);
    }
  }

  /**
   * This method is called when server send return value of invoke.
   * You don't have to call this method manually.
   * @param payload
   */
  _onReturn(invokeId, value) {
    const request = this.rpcRequestMap[invokeId];
    delete this.rpcRequestMap[invokeId];

    if (!request) {
      console.error('Can\'t find rpc request ', invokeId);
      return;
    }

    request.resolve(value);
  }

  _onException(invokeId, errorMessage, exception) {
    const request = this.rpcRequestMap[invokeId];
    delete this.rpcRequestMap[invokeId];

    if (!request) {
      console.error('Can\'t find rpc request ', invokeId);
      return;
    }

    request.reject({exception, errorMessage});
  }

  /**
   * Generate random invoke id
   * @returns {string}
   * @private
   */
  _generateInvokeId() {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

    for (let i = 0; i < 7; i++) {
      text += possible.charAt(Math.floor(Math.random() * possible.length));
    }

    return text;
  }
}
