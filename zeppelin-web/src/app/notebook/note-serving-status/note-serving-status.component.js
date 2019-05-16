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

import noteServingStatusTemplate from './note-serving-status.html';
import './note-serving-status.css';

class NoteServingStatusController {
  constructor($http, $location, baseUrlSrv) {
    'ngInject';
    this.servingStatus = '...'; // one of 'Loading', 'Not running', 'Running', 'Error'
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

    if (this.noteid && this.revid) {
      this.http.get(this.apiBaseAddr + '/notebook/serving/' + this.noteid + '/' + this.revid)
      .success(function(data, status) {
        if (data.body.task) {
          if (Object.keys(data.body.task).length === 0) {
            self.servingStatus = 'Not running';
          } else {
            self.servingStatus = 'Running';
          }
        } else {
          self.servingStatus = 'Not running';
        }
      })
      .error(function(data, status) {
        if (self.noteid && self.revid) {
          self.servingStatus = 'Not running';
        }
      });
    }

    self._t = setTimeout(function() {
      self.updateStatus();
    }, 3000);
  }

  showNote() {
    console.info('show note');
    this.location.path('/notebook/' + this.noteid + '/task/' + this.revid);
  }

  startServing() {
    console.info('start serving');
    let self = this;
    this.http.post(this.apiBaseAddr + '/notebook/serving/' + this.noteid + '/' + this.revid)
      .success(function(data, status) {
        self.updateStatus();
      })
      .error(function(data, status) {
        self.testStatus = 'Error';
      });
  }

  stopServing() {
    console.info('stop serving');
    let self = this;
    this.http.delete(this.apiBaseAddr + '/notebook/serving/' + this.noteid + '/' + this.revid)
      .success(function(data, status) {
        self.updateStatus();
      })
      .error(function(data, status) {
        self.testStatus = 'Error';
      });
  }
}

export const NoteServingStatusComponent = {
  template: noteServingStatusTemplate,
  controller: NoteServingStatusController,
  bindings: {
    noteid: '<',
    revid: '<',
    textonly: '<',
  },
};

export const NoteServingStatusModule = angular
  .module('zeppelinWebApp')
  .component('noteServingStatus', NoteServingStatusComponent)
  .name;
