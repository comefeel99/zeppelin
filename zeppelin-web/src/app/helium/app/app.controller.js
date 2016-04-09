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

angular.module('zeppelinWebApp').controller('HeliumAppCtrl', function($scope, $route, $routeParams, $location, $rootScope, $http, $timeout, baseUrlSrv, ngToast, websocketMsgSrv) {
  var noteId = $route.current.pathParams.noteId;
  var paragraph;      // paragraph


  // store previous data of paragraph to determin if output need to be updated
  var previousValues = {
    config : undefined,
    dateFinished : undefined
  }


  // apps to be displayed in selector.
  $scope.apps = [];

  // suggestions
  $scope.suggestion = {};


  var setParagraph = function(_paragraph) {
    paragraph = _paragraph;
    previousValues.config = angular.copy(_paragraph.config);
    previousValues.dateFinished = angular.copy(_paragraph.dateFinished);
  }

  var isUpdateRequired = function(_paragraph) {
    return previousValues.dateFinished !== _paragraph.dateFinished ||
      !angular.equals(previousValues.config, _paragraph.config);
  }

  /**
   * Initialize the controller
   * @param paragraph
   */
  $scope.init = function(_paragraph) {
    console.log('initialize %o', _paragraph);
    setParagraph(_paragraph);

    getApplicationStates();
    getSuggestions();

    renderActiveApplication();
  };

  /**
   * When update is required by paragraph change
   */
  $scope.$on('updateParagraph', function(event, data) {
    if (paragraph.id === data.paragraph.id && isUpdateRequired(data.paragraph)) {
      setParagraph(data.paragraph);

      getApplicationStates();
      getSuggestions();

      renderActiveApplication();
    }
  });

  /**
   * Return id of paragraph this app controller belongs to
   * @returns {*}
   */
  $scope.getParagraphId = function() {
    return paragraph.id;
  };

  /**
   * switch app
   * @param appId
   */
  $scope.switchApp = function(appId) {
    console.log('switch %o', appId);
    var app = _.find($scope.apps, { id : appId });
    var config = paragraph.config;
    var settings = paragraph.settings;

    var newConfig = angular.copy(config);
    var newParams = angular.copy(settings.params);

    if (app.builtin) {
      newConfig.graph.mode = appId;
      newConfig.activeApp = undefined;
    } else {
      newConfig.activeApp = appId;
    }

    commitConfig(newConfig, newParams);
  };

  var commitConfig = function(config, params) {
    websocketMsgSrv.commitParagraph(paragraph.id, paragraph.title, paragraph.text, config, params);
  };

  var getActiveApp = function() {
    var config = paragraph.config;
    var appId;
    if (config.activeApp) {
      appId = config.activeApp;
    } else {
      appId = config.graph.mode;
    }
    return _.find($scope.apps, { id : appId });
  };

  /**
   * check if appId is activated
   * @param appId
   */
  $scope.isActiveApp = function(appId) {
    var config = paragraph.config;

    if (config.activeApp) {
      return config.activeApp === appId;
    } else {
      return config.graph.mode === appId;
    }
  };

  $scope.loadApp = function(heliumPackage) {
    console.log('Load application %o', heliumPackage);
    // Get suggested apps
    $http.post(baseUrlSrv.getRestApiBase() + '/helium/load/' + noteId + '/' + paragraph.id,
      heliumPackage)
      .success(function(data, status, headers, config) {
        console.log('Suggested apps %o', data);
        $scope.suggestion = data.body;
      })
      .error(function(err, status, headers, config) {
        console.log('Error %o', err);
        ngToast.danger('Error ' + status + '. Can not get suggested applications');
      });
  };

  /**
   * check if table data result is transfered to front-end.
   * @returns {*|boolean}
   */
  var isTableResultAvailableInFrontEnd = function() {
    var result = paragraph.result;
    return (result && result.type && result.type === 'TABLE');
  };

  var getApplicationStates = function() {
    $scope.apps = [];

    // If paragraph type is table, include built-in visualizations
    if (isTableResultAvailableInFrontEnd()) {
      // ApplicationState for table
      $scope.apps.push({
        id: 'table',
        name: 'Table',
        status: 'LOADED',
        icon: '<i class="fa fa-table"></i>',
        builtin: true,
        viz : TableVisualization,
        transformation : TableDataTransformation
      });

      // ApplicationState for bar chart
      $scope.apps.push({
        id: 'multiBarChart',
        name: 'BarChart',
        status: 'LOADED',
        icon: '<i class="fa fa-bar-chart"></i>',
        builtin: true,
        viz : BarChartVisualization,
        transformation : PivotTransformation
      });

      // ApplicationState for pie chart
      $scope.apps.push({
        id: 'pieChart',
        name: 'PieChart',
        status: 'LOADED',
        icon: '<i class="fa fa-pie-chart"></i>',
        builtin: true
      });

      // ApplicationState for area chart
      $scope.apps.push({
        id: 'stackedAreaChart',
        name: 'AreaChart',
        status: 'LOADED',
        icon: '<i class="fa fa-area-chart"></i>',
        builtin: true
      });

      // ApplicationState for line chart
      $scope.apps.push({
        id: 'lineChart',
        name: 'LineChart',
        status: 'LOADED',
        icon: '<i class="fa fa-line-chart"></i>',
        builtin: true
      });

      // ApplicationState for scatter chart
      $scope.apps.push({
        id: 'scatterChart',
        name: 'ScatterChart',
        status: 'LOADED',
        icon: '<i class="cf cf-scatter-chart"></i>',
        builtin: true
      });
    }

    // Display ApplicationState
    if (paragraph.apps) {
      _.forEach(paragraph.apps, function (app) {
        $scope.apps.push({
          id: app.id,
          name: app.name,
          status: app.status,
          icon: 'He',
          builtin: false
        });
      });
    }
  };

  var getSuggestions = function() {
    // Get suggested apps
    $http.get(baseUrlSrv.getRestApiBase() + '/helium/suggest/' + noteId + '/' + paragraph.id)
      .success(function(data, status, headers, config) {
        console.log('Suggested apps %o', data);
        $scope.suggestion = data.body;
      })
      .error(function(err, status, headers, config) {
        console.log('Error %o', err);
        ngToast.danger('Error ' + status + '. Can not get suggested applications');
      });
  };

  var isEmpty = function (object) {
    return !object;
  };


  var renderActiveApplication = function() {
    var app = getActiveApp();
    if (!app) {
      // no active app found
      return;
    }

    var targetEl = angular.element('#p' + paragraph.id + '_' + app.id);
    var settingTargetEl = angular.element('#p' + paragraph.id + '_' + app.id);
    if (targetEl.length && settingTargetEl.length) {
      if (app.builtin) {
        var data = paragraph.result;

        if (app.transformation) {
          // app._tr keeps reference to the transformation instance
          if (!app._tr) {
            app._tr = new app.transformation(settingTargetEl, data, paragraph.config.graph);
          }
          data = app._tr.transform();
        }

        // app._viz keeps reference to the visualization instance
        if (!app._viz) {
          app._viz = new app.viz(targetEl, data, paragraph.config.graph);
        }
        app._viz.render();
      } else {
        console.log("Helium app render %o", targetEl);
        targetEl.html('helium app' + app.id);
      }
    } else {
      // retry until element is ready
      $timeout(renderActiveApplication, 10);
    }
  };


  /**
   * Base class for built-in result Transformation
   * @param targetEl targetEl to render transformation setting gui
   * @param data input data
   * @param config configuration object.
   */
  var Transformation = function(targetEl, data, config) {
    this.init = function(targetEl, data, config) {
      this.targetEl = targetEl;
      this.data = data;
      this.config = config;
    };

    this.renderSetting = undefined;

    this.transform = function() {
      return this.data;
    };

    return this;
  };


  /**
   * Tranfrom paragraph result to table data
   * @param targetEl targetEl to render transformation setting gui
   * @param data input data
   * @param config configuration object.
   */
  var TableDataTransformation = function(targetEl, data, config) {
    this.init(targetEl, data, config);

    this.transform = function() {
      return this.resultToTableData(this.data);
    };

    /**
     * Paragraph result to table data format
     * @param result paragraph result
     * @returns
     */
    this.resultToTableData = function(result) {
      if (!result) {
        return;
      }

      if (result.type === 'TABLE') {
        var ret = {
          comment : ''
        };
        var columnNames = [];
        var rows = [];
        var array = [];
        var textRows = result.msg.split('\n');
        var comment = false;

        for (var i = 0; i < textRows.length; i++) {
          var textRow = textRows[i];
          if (comment) {
            ret.comment += textRow;
            continue;
          }

          if (textRow === '') {
            if (rows.length>0) {
              comment = true;
            }
            continue;
          }
          var textCols = textRow.split('\t');
          var cols = [];
          var cols2 = [];
          for (var j = 0; j < textCols.length; j++) {
            var col = textCols[j];
            if (i === 0) {
              columnNames.push({name:col, index:j, aggr:'sum'});
            } else {
              cols.push(col);
              cols2.push({key: (columnNames[i]) ? columnNames[i].name: undefined, value: col});
            }
          }
          if (i !== 0) {
            rows.push(cols);
            array.push(cols2);
          }
        }

        ret['msgTable'] = array;
        ret['columnNames'] = columnNames;
        ret['rows'] = rows;
        return ret;
      }
    };

    return this;
  };
  TableDataTransformation.prototype = new Transformation();


  /**
   * Pivot the result data
   * @param targetEl
   * @param data
   * @param config paragraph.config.graph
   * @returns d3 chart data format
   */
  var PivotTransformation = function(targetEl, data, config) {
    this.init(targetEl, data, config);

    /**
     *
     * @param config config object of paragraph
     * @returns {*} pivot data format
     */
    this.transform = function() {
      return pivot(this.resultToTableData(data), this.config);
    };

    var pivot = function(data, config) {
      var keys = config.keys;
      var groups = config.groups;
      var values = config.values;

      var aggrFunc = {
        sum : function(a,b) {
          var varA = (a !== undefined) ? (isNaN(a) ? 1 : parseFloat(a)) : 0;
          var varB = (b !== undefined) ? (isNaN(b) ? 1 : parseFloat(b)) : 0;
          return varA+varB;
        },
        count : function(a,b) {
          var varA = (a !== undefined) ? parseInt(a) : 0;
          var varB = (b !== undefined) ? 1 : 0;
          return varA+varB;
        },
        min : function(a,b) {
          var varA = (a !== undefined) ? (isNaN(a) ? 1 : parseFloat(a)) : 0;
          var varB = (b !== undefined) ? (isNaN(b) ? 1 : parseFloat(b)) : 0;
          return Math.min(varA,varB);
        },
        max : function(a,b) {
          var varA = (a !== undefined) ? (isNaN(a) ? 1 : parseFloat(a)) : 0;
          var varB = (b !== undefined) ? (isNaN(b) ? 1 : parseFloat(b)) : 0;
          return Math.max(varA,varB);
        },
        avg : function(a,b,c) {
          var varA = (a !== undefined) ? (isNaN(a) ? 1 : parseFloat(a)) : 0;
          var varB = (b !== undefined) ? (isNaN(b) ? 1 : parseFloat(b)) : 0;
          return varA+varB;
        }
      };

      var aggrFuncDiv = {
        sum : false,
        count : false,
        min : false,
        max : false,
        avg : true
      };

      var schema = {};
      var rows = {};

      for (var i=0; i < data.rows.length; i++) {
        var row = data.rows[i];
        var newRow = {};
        var s = schema;
        var p = rows;

        for (var k=0; k < keys.length; k++) {
          var key = keys[k];

          // add key to schema
          if (!s[key.name]) {
            s[key.name] = {
              order : k,
              index : key.index,
              type : 'key',
              children : {}
            };
          }
          s = s[key.name].children;

          // add key to row
          var keyKey = row[key.index];
          if (!p[keyKey]) {
            p[keyKey] = {};
          }
          p = p[keyKey];
        }

        for (var g=0; g < groups.length; g++) {
          var group = groups[g];
          var groupKey = row[group.index];

          // add group to schema
          if (!s[groupKey]) {
            s[groupKey] = {
              order : g,
              index : group.index,
              type : 'group',
              children : {}
            };
          }
          s = s[groupKey].children;

          // add key to row
          if (!p[groupKey]) {
            p[groupKey] = {};
          }
          p = p[groupKey];
        }

        for (var v=0; v < values.length; v++) {
          var value = values[v];
          var valueKey = value.name+'('+value.aggr+')';

          // add value to schema
          if (!s[valueKey]) {
            s[valueKey] = {
              type : 'value',
              order : v,
              index : value.index
            };
          }

          // add value to row
          if (!p[valueKey]) {
            p[valueKey] = {
              value : (value.aggr !== 'count') ? row[value.index] : 1,
              count: 1
            };
          } else {
            p[valueKey] = {
              value : aggrFunc[value.aggr](p[valueKey].value, row[value.index], p[valueKey].count+1),
              count : (aggrFuncDiv[value.aggr]) ?  p[valueKey].count+1 : p[valueKey].count
            };
          }
        }
      }

      //console.log("schema=%o, rows=%o", schema, rows);

      return {
        schema : schema,
        rows : rows
      };
    };

    return this;
  };
  PivotTransformation.prototype = new TableDataTransformation();


  /**
   * Base class for built-in Visualizations
   *
   * @targetEl target html element
   * @data paragraph result
   * @config configuration object
   */
  var Visualization = function(targetEl, data, config) {
    this.init = function(targetEl, data, config) {
      this.targetEl = targetEl;
      this.data = data;
      this.config = config;
    };

    this.render = function() {
      // override this method
    };

    this.onResize = function() {
      // overrride this method
    };

    return this;
  };

  /**
   * Table visualization
   */
  var TableVisualization = function(targetEl, data, config) {
    this.init(targetEl, data, config);

    this.render = function () {
      var getTableContentFormat = function(d) {
        if (isNaN(d)) {
          if (d.length>'%html'.length && '%html ' === d.substring(0, '%html '.length)) {
            return 'html';
          } else {
            return '';
          }
        } else {
          return '';
        }
      };

      var formatTableContent = function(d) {
        if (isNaN(d)) {
          var f = getTableContentFormat(d);
          if (f !== '') {
            return d.substring(f.length+2);
          } else {
            return d;
          }
        } else {
          var dStr = d.toString();
          var splitted = dStr.split('.');
          var formatted = splitted[0].replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,');
          if (splitted.length>1) {
            formatted+= '.'+splitted[1];
          }
          return formatted;
        }
      };


      var html = '';
      html += '<table class="table table-hover table-condensed">';
      html += '  <thead>';
      html += '    <tr style="background-color: #F6F6F6; font-weight: bold;">';
      for (var titleIndex in this.data.columnNames) {
        html += '<th>'+ this.data.columnNames[titleIndex].name+'</th>';
      }
      html += '    </tr>';
      html += '  </thead>';
      html += '  <tbody>';
      for (var r in this.data.msgTable) {
        var row = this.data.msgTable[r];
        html += '    <tr>';
        for (var index in row) {
          var v = row[index].value;
          if (getTableContentFormat(v) !== 'html') {
            v = v.replace(/[\u00A0-\u9999<>\&]/gim, function(i) {
              return '&#'+i.charCodeAt(0)+';';
            });
          }
          html += '      <td>'+formatTableContent(v)+'</td>';
        }
        html += '    </tr>';
      }
      html += '  </tbody>';
      html += '</table>';

      this.targetEl.html(html);
    };

    return this;
  };
  TableVisualization.prototype = new Visualization();

  /**
   * Base class for NvD3 chart visualization
   */
  var NvD3Visualization = function(targetEl, data, config) {
    this.init(targetEl, data, config);

    this.chartModel; // override this value

    this.renderD3Chart = function (d3g) {
      if (!this.svgEl) {
        this.svgEl = this.targetEl.append("<svg></svg>").find("svg")[0];
      }

      console.log("Svg El %o", this.svgEl);

      var height = this.config.height;
      var data = this.data;

      var animationDuration = 300;
      var numberOfDataThreshold = 150;
      // turn off animation when dataset is too large. (for performance issue)
      // still, since dataset is large, the chart content sequentially appears like animated.
      try {
        if (data.values.length > numberOfDataThreshold) {
          animationDuration = 0;
        }
      } catch(ignoreErr) {
      }

      var chartEl = d3.select(this.svgEl)
        .attr('height', height)
        .datum(d3g)
        .transition()
        .duration(animationDuration)
        .call(this.chartModel);
      d3.select(this.svgEl).style.height = height+'px';
      nv.utils.windowResize(this.chartModel.update);
    };

    /**
     *
     * @param data
     * @param config graph config from paragraph. i.e paragraph.config.graph
     * @param allowTextXAxis
     * @param fillMissingValues
     * @param chartType
     * @returns {{xLabels: {}, d3g: Array}}
       */
    this.pivotDataToD3ChartFormat = function(data, allowTextXAxis, fillMissingValues, chartType) {
      // construct d3 data
      var d3g = [];

      var config = this.config;
      var schema = data.schema;
      var rows = data.rows;
      var values = config.values;

      var concat = function(o, n) {
        if (!o) {
          return n;
        } else {
          return o+'.'+n;
        }
      };

      var getSchemaUnderKey = function(key, s) {
        for (var c in key.children) {
          s[c] = {};
          getSchemaUnderKey(key.children[c], s[c]);
        }
      };

      var traverse = function(sKey, s, rKey, r, func, rowName, rowValue, colName) {
        //console.log("TRAVERSE sKey=%o, s=%o, rKey=%o, r=%o, rowName=%o, rowValue=%o, colName=%o", sKey, s, rKey, r, rowName, rowValue, colName);

        if (s.type==='key') {
          rowName = concat(rowName, sKey);
          rowValue = concat(rowValue, rKey);
        } else if (s.type==='group') {
          colName = concat(colName, rKey);
        } else if (s.type==='value' && sKey===rKey || valueOnly) {
          colName = concat(colName, rKey);
          func(rowName, rowValue, colName, r);
        }

        for (var c in s.children) {
          if (fillMissingValues && s.children[c].type === 'group' && r[c] === undefined) {
            var cs = {};
            getSchemaUnderKey(s.children[c], cs);
            traverse(c, s.children[c], c, cs, func, rowName, rowValue, colName);
            continue;
          }

          for (var j in r) {
            if (s.children[c].type === 'key' || c === j) {
              traverse(c, s.children[c], j, r[j], func, rowName, rowValue, colName);
            }
          }
        }
      };

      var keys = config.keys;
      var groups = config.groups;
      values = config.values;
      var valueOnly = (keys.length === 0 && groups.length === 0 && values.length > 0);
      var noKey = (keys.length === 0);
      var isMultiBarChart = (chartType === 'multiBarChart');

      var sKey = Object.keys(schema)[0];

      var rowNameIndex = {};
      var rowIdx = 0;
      var colNameIndex = {};
      var colIdx = 0;
      var rowIndexValue = {};

      for (var k in rows) {
        traverse(sKey, schema[sKey], k, rows[k], function(rowName, rowValue, colName, value) {
          //console.log("RowName=%o, row=%o, col=%o, value=%o", rowName, rowValue, colName, value);
          if (rowNameIndex[rowValue] === undefined) {
            rowIndexValue[rowIdx] = rowValue;
            rowNameIndex[rowValue] = rowIdx++;
          }

          if (colNameIndex[colName] === undefined) {
            colNameIndex[colName] = colIdx++;
          }
          var i = colNameIndex[colName];
          if (noKey && isMultiBarChart) {
            i = 0;
          }

          if (!d3g[i]) {
            d3g[i] = {
              values : [],
              key : (noKey && isMultiBarChart) ? 'values' : colName
            };
          }

          var xVar = isNaN(rowValue) ? ((allowTextXAxis) ? rowValue : rowNameIndex[rowValue]) : parseFloat(rowValue);
          var yVar = 0;
          if (xVar === undefined) { xVar = colName; }
          if (value !== undefined) {
            yVar = isNaN(value.value) ? 0 : parseFloat(value.value) / parseFloat(value.count);
          }
          d3g[i].values.push({
            x : xVar,
            y : yVar
          });
        });
      }

      // clear aggregation name, if possible
      var namesWithoutAggr = {};
      var colName;
      var withoutAggr;
      // TODO - This part could use som refactoring - Weird if/else with similar actions and variable names
      for (colName in colNameIndex) {
        withoutAggr = colName.substring(0, colName.lastIndexOf('('));
        if (!namesWithoutAggr[withoutAggr]) {
          namesWithoutAggr[withoutAggr] = 1;
        } else {
          namesWithoutAggr[withoutAggr]++;
        }
      }

      if (valueOnly) {
        for (var valueIndex = 0; valueIndex < d3g[0].values.length; valueIndex++) {
          colName = d3g[0].values[valueIndex].x;
          if (!colName) {
            continue;
          }

          withoutAggr = colName.substring(0, colName.lastIndexOf('('));
          if (namesWithoutAggr[withoutAggr] <= 1 ) {
            d3g[0].values[valueIndex].x = withoutAggr;
          }
        }
      } else {
        for (var d3gIndex = 0; d3gIndex < d3g.length; d3gIndex++) {
          colName = d3g[d3gIndex].key;
          withoutAggr = colName.substring(0, colName.lastIndexOf('('));
          if (namesWithoutAggr[withoutAggr] <= 1 ) {
            d3g[d3gIndex].key = withoutAggr;
          }
        }

        // use group name instead of group.value as a column name, if there're only one group and one value selected.
        if (groups.length === 1 && values.length === 1) {
          for (d3gIndex = 0; d3gIndex < d3g.length; d3gIndex++) {
            colName = d3g[d3gIndex].key;
            colName = colName.split('.')[0];
            d3g[d3gIndex].key = colName;
          }
        }

      }

      return {
        xLabels : rowIndexValue,
        d3g : d3g
      };
    };

    return this;
  };
  NvD3Visualization.prototype = new Visualization();

  /**
   * Barchart visualization
   */
  var BarChartVisualization = function(targetEl, data, config) {
    this.init(targetEl, data, config);

    this.render = function () {
      var model = nv.models['multiBarChart']();
      model.yAxis.axisLabelDistance(50);
      model.yAxis.tickFormat(function(d) {return yAxisTickFormat(d);});
      this.chartModel = model;

      var data = this.pivotDataToD3ChartFormat(this.data);
      this.renderD3Chart(data.d3g);
    };

    var groupedThousandsWith3DigitsFormatter = function(x){
      return d3.format(',')(d3.round(x, 3));
    };

    var customAbbrevFormatter = function(x) {
      var s = d3.format('.3s')(x);
      switch (s[s.length - 1]) {
        case 'G': return s.slice(0, -1) + 'B';
      }
      return s;
    };

    var yAxisTickFormat = function(d) {
      if(d >= Math.pow(10,6)){
        return customAbbrevFormatter(d);
      }
      return groupedThousandsWith3DigitsFormatter(d);
    };

    return this;
  };
  BarChartVisualization.prototype = new NvD3Visualization();

});
