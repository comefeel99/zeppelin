#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# This script expected run on travis CI environment for r interpreter module testing
#
echo "deb http://cran.rstudio.com/bin/linux/ubuntu precise/" | tee -a /etc/apt/sources.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
apt-get update -qq
apt-get install r-base-core r-base-dev
R -e 'install.packages(c("htmltools"), quiet = TRUE, repos = c("http://cran.us.r-project.org"), dependencies = TRUE)'
R -e 'install.packages(c("knitr"), quiet = TRUE, repos = c("http://cran.us.r-project.org"), dependencies = TRUE)'
R -e 'install.packages(c("devtools"), quiet = TRUE, repos = c("http://cran.us.r-project.org"), dependencies = TRUE)'
R -e 'install.packages(c("evaluate"), quiet = TRUE, repos = c("http://cran.us.r-project.org"), dependencies = TRUE)'
R -e 'install.packages(c("googleVis", "rCharts", "base64enc"), quiet = TRUE, repos = c("http://cran.us.r-project.org"), dependencies = TRUE)'
R -e 'devtools::install_github("IRkernel/repr")'
