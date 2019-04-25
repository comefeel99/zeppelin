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

import './serving.html';
import './serving.css';

class ServingController {
  constructor($scope, $http, baseUrlSrv) {
    'ngInject';
    this.$http = $http;
    this.$scope = $scope;
    this.apiBaseAddr = baseUrlSrv.getRestApiBase();
    this.listServings();

    let self = this;
    $scope.refresh = function() {
      self.listServings();
    };
    $scope.loading = false;
  }

  listServings() {
    console.log('list servings');
    let $scope = this.$scope;
    $scope.loading = true;
    this.$http.get(this.apiBaseAddr + '/notebook/serving')
      .success(function(data, status) {
        $scope.loading = false;
        let servings = {};
        console.log(data.body);
        data.body.forEach((s) => {
          if (!servings[s.note.id]) {
            servings[s.note.id] = [];
          }
          servings[s.note.id].push(s);
        });
        $scope.servings = servings;
        if (Object.keys(servings).length === 0) {
          $scope.noServings = true;
        }
        console.log($scope.servings);
      })
      .error(function(data, status) {
        $scope.loading = false;
        // error
      });
  }
}

angular.module('zeppelinWebApp')
  .controller('ServingCtrl', ServingController);
