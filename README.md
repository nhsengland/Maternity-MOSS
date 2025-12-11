# Maternity Outcomes Signal System

## NHS England Maternity and Neonatal Analytics Team

### About the Project

This repo contains code to reproduce analysis for the [Maternity Outcomes Signal System (MOSS)](https://www.england.nhs.uk/publication/maternity-outcomes-signal-system-standard-operating-procedures/).

The main methods used to analyse the data are: 

-   Cusum methodology for a poisson distribution as described in Hawkins, D. M. and Olwell, D. H. (1998) "Cumulative Sum Charts and Charting for Quality Improvement". 
-   Variable Life Adjusted Display (VLAD) charts - cumulative number of excess events over time compared to the national reference rate.

The code produces a summary output that is subsequently fed into the live tool.

### Project Structure
 
-   The MOSS Calculations.R file contains the main calculations used to generate the signals and any other outputs required for the live tool. 
-   The Live Data folder contains a dummy dataset of the events data used for the analysis that is refreshed weekly. This can be replaced by real data as long as the formats remain the same.
-   The Reference Data folder contains dummy datasets of reference data that are refreshed periodically. These can be replaced by real data as long as the formats remain the same. 
-   The Functions.R file contains various functions used throughout the analysis.
-   The Source Raw Data.py and Functions_Py.py files contain code to process and clean raw patient level data in order to generate the required input data for the main calculations. This doesn't form part of the main calculations.

### Built With

[![R v4.4](https://img.shields.io/badge/R%20v4.4-8A2BE2)] 
-  lubridate 
-  dplyr
-  tidyr
-  CUSUMdesign 
-  httr2
-  arrow
-  glue
-  AzureAuth
-  AzureKeyVault
-  AzureStor

Python has also been used to generate the initial input files but this step is optional and not required if your input data matches those of the dummy datasets provided. 

### Getting Started

To get a local copy up and running follow these simple steps.

To clone the repo:

\`git clone
<https://github.com/nhsengland/Maternity-MOSS.git>

### Usage

Update the dummy data files in the Data folders with your own real data, preserving the formats of each column. The section of code titled 'Load Data' will need updating to read from your local files instead of the Azure Storage location.
The calculations can then be run from the MOSS Calculations.R file.
Please note that the code was produced within Databricks and versions of packages were determined by those pre-installed on the cluster.

### Datasets

-  Events - Daily data from the Personal Demographics Service (PDS) listing individual term neonatal deaths and term stillbirths by NHS Site of Birth.
-  Denoms - Monthly aggregated data from PDS of the number of term live births and term total births by site from previous years.
-  Ref_Rate - Summary file containing the national rates of outcomes.
-  Provider Site - List of NHS Site codes and names.

### Outputs

Summary extract that is subsequently used within the MOSS live tool.


### License

Unless stated otherwise, the codebase is released under [the MIT LICENSE](./LICENSE) This covers both the codebase and any sample
code in the documentation.

*See [LICENSE](./LICENSE) for more information.*

The documentation is [© Crown
copyright](http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/)
and available under the terms of the [Open Government
3.0](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)
licence.

### Contact

[NHS England MOSS Project Team](mailto:england.MOSS@nhs.net), NHS England
