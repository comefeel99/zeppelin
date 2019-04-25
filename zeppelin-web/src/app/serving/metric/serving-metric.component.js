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

import servingMetricTemplate from './serving-metric.html';
import './serving-metric.css';
import Linechart from '../../visualization/builtins/visualization-linechart';

class ServingMetricController {
  constructor($http, $location, baseUrlSrv, $element) {
    'ngInject';
    this.apiBaseAddr = baseUrlSrv.getRestApiBase();
    this.http = $http;
    this.element = $element;
  }

  $onInit() {
    this.refresh();
  }

  $onDestroy() {
  }

  refresh() {
    let self = this;
    this.http.get(this.apiBaseAddr + '/notebook/serving/' +
      this.noteid + '/' + this.revid + '/' + this.endpoint)
      .success(function(data, status) {
        console.log('Metric', data);
        self.metrics = data.body;
        self.drawChart();
      })
      .error(function(data, status) {
        self.drawChart();
      });
  }
  drawChart() {
    let tables = {};
    let allDates = [];
    this.metrics.forEach((m) => {
      let keys = Object.keys(m.metric);
      let date = new Date(m.timestamp * 1000);
      let dateString = date.getHours() + ':' + date.getMinutes();
      allDates.push(dateString);

      keys.forEach((key) => {
        let keys = key.split('.');
        let tableName = keys[0];
        let groupName = (keys.length === 1) ? '' : key.substring(keys[0].length + 1);

        // create new table
        if (!tables[tableName]) {
          tables[tableName] = {
            columns: [
              {
                name: 'date',
                index: 0,
                aggr: 'sum',
              },
              {
                name: 'value',
                index: 1,
                aggr: 'sum',
              },
              {
                name: 'group',
                index: 2,
                aggr: 'sum',
              },
            ],
            rows: [],
          };
        }

        let value = m.metric[key];
        tables[tableName].rows.push([
          dateString,
          value,
          groupName,
        ]);
      });
    });

    this.tableNames = Object.keys(tables);
    let self = this;
    Object.keys(tables).forEach((tableName) => {
      // fill zero in table data row, on missing value
      let tableData = tables[tableName];
      let rowCur = 0;
      let zeroFilledRows = allDates.map((dateString) => {
        if (rowCur < tableData.rows.length) {
          if (tableData.rows[rowCur][0] === dateString) {
            return tableData.rows[rowCur++];
          }
        }

        return [dateString, 0, tableName];
      });
      tableData.rows = zeroFilledRows;

      setTimeout(() => {
        let chartEl = this.element.find('#chart_' +
          self.noteid + self.revid + self.endpoint + tableName);
        console.log('chart el ', chartEl);
        let chart = new Linechart(chartEl, {});
        let pivot = chart.getTransformation();
        pivot.config = {common: {}};
        pivot.config.common.pivot = {
          keys: [{
            name: 'date',
            index: 0,
            aggr: 'sum',
          }],
          groups: [/* {
            name: 'group',
            index: 2,
            aggr: 'sum',
          } */],
          values: [{
            name: 'value',
            index: 1,
            aggr: (tableName.match(/sum/i)) ? 'sum'
              : (tableName.match(/avg/i)) ? 'avg'
              : (tableName.match(/max/i)) ? 'max'
              : (tableName.match(/min/i)) ? 'min'
              : 'sum',
          }],
        };
        chart.render(pivot.transform(tableData));
      }, 500);
    });
  }
}


export const ServingMetricComponent = {
  template: servingMetricTemplate,
  controller: ServingMetricController,
  bindings: {
    noteid: '<',
    revid: '<',
    endpoint: '<',
  },
};

export const ServingMetricModule = angular
  .module('zeppelinWebApp')
  .component('servingMetric', ServingMetricComponent)
  .name;
