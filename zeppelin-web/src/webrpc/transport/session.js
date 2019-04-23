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
export class Session {
  constructor() {
    this._callbacks = {
      onOpen: () => {},
      onClose: () => {},
      onReturn: (invokeId, value) => {},
      onException: (invokeId, errorMessage, exception) => {},
      onInvoke: (rpcName, methodName, params) => {},
    };
  }

  open() {
    throw new Error('Override this method');
  }

  invoke(invokeId, rpcName, methodName, params, types) {
    throw new Error('Override this method');
  }

  close() {
    throw new Error('Override this method');
  }

  isConnected() {
    throw new Error('Override this method');
  }

  on(fnName, fn) {
    if (fn) {
      this._callbacks[fnName] = fn;
    } else {
      this._callbacks[fnName] = () => {};
    }
  }
}
