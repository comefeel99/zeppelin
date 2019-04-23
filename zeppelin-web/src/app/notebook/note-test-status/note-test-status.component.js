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

import noteTestStatusTemplate from './note-test-status.html';
import './note-test-status.css';

class NoteTestStatusController {
  constructor($http, $location, baseUrlSrv) {
    'ngInject';
    this.testStatus = 'notStarted'; // one of 'notStarted', 'running', 'fail', 'pass'
    this.apiBaseAddr = baseUrlSrv.getRestApiBase();
    this.location = $location;
    this.http = $http;
  }

  $onInit() {
    this.updateStatus();
  }

  $onDestroy() {
    if (this._t) {
      clearTimeout(this._t);
    }
  }

  updateStatus() {
    let self = this;
    console.warn('Update status', this.noteid, this.revid);
    this.http.get(this.apiBaseAddr + '/notebook/test/' + this.noteid + '/' + this.revid)
      .success(function(data, status) {
        console.warn('Test status', data, status);
        if (data.body.running) {
          self.testStatus = 'running';
          self._t = setTimeout(function() {
            self.updateStatus();
          }, 3000);
        } else {
          if (data.body.result.fail > 0) {
            self.testStatus = 'fail';
          } else {
            self.testStatus = 'pass';
          }
          if (self._t) {
            clearTimeout(self._t);
          }
        }
      })
      .error(function(data, status) {
        self.testStatus = 'notStarted';
        if (self._t) {
          clearTimeout(self._t);
        }
      });
  }

  showNote() {
    console.info('show note');
    this.location.path('/notebook/' + this.noteid + '/task/' + this.revid);
  }

  startTest() {
    console.info('start test');
    let self = this;
    this.http.post(this.apiBaseAddr + '/notebook/test/' + this.noteid + '/' + this.revid)
      .success(function(data, status) {
        self.updateStatus();
      })
      .error(function(data, status) {
        self.testStatus = 'fail';
      });
  }

  stopTest() {
    console.info('stop test');
    let self = this;
    this.http.delete(this.apiBaseAddr + '/notebook/test/' + this.noteid + '/' + this.revid)
      .success(function(data, status) {
        self.updateStatus();
      })
      .error(function(data, status) {
        self.testStatus = 'fail';
      });
  }
}

export const NoteTestStatusComponent = {
  template: noteTestStatusTemplate,
  controller: NoteTestStatusController,
  bindings: {
    noteid: '<',
    revid: '<',
  },
};

export const NoteTestStatusModule = angular
  .module('zeppelinWebApp')
  .component('noteTestStatus', NoteTestStatusComponent)
  .name;
