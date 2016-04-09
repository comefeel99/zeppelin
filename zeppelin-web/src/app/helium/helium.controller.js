/* jshint loopfunc: true */
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
'use strict';

angular.module('zeppelinWebApp').controller('HeliumCtrl', function($scope, $route, $routeParams, $location, $rootScope, $http, baseUrlSrv, ngToast) {
  $scope.allPackages = [];

  var getAllPackages = function() {
    $http.get(baseUrlSrv.getRestApiBase() + '/helium/all')
      .success(function(data, status, headers, config) {
        console.log('All Packages %o', data);
        $scope.allPackages = data.body;
      })
      .error(function(err, status, headers, config) {
        console.log('Error %o', err);
        ngToast.danger('Error ' + status + '. Can not get package list ');
      });
  };

  getAllPackages();
});
