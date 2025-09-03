# Databricks notebook source
# DBTITLE 1,Load Libraries
from pyspark.sql.functions import when, col, concat, year, month, lpad, col, lit, to_date, substring
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils

# COMMAND ----------

# DBTITLE 1,Load Functions
# MAGIC %run 
# MAGIC ./Functions_Py

# COMMAND ----------

# DBTITLE 1,Load Config File
# MAGIC %run 
# MAGIC ./Config_Py

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load in Data

# COMMAND ----------

# DBTITLE 1,Load Reference Data
Site_Mergers_df = load_from_Analysis_Lake(SiteMergers_Path)
Site_Mergers_df.createOrReplaceTempView("Site_Mergers")

Provider_Site_df = load_from_Analysis_Lake(Provider_Site_Path)
Provider_Site_df.createOrReplaceTempView("Provider_Site")

Postcode_Mapping_df = load_from_Analysis_Lake(Postcode_Mapping_Path)
Postcode_Mapping_df.createOrReplaceTempView("Postcode_Mapping")

# COMMAND ----------

# DBTITLE 1,Define Start Date of extract
Start_Date = '2025-01-01'

# COMMAND ----------

# DBTITLE 1,Load PDS Data
PDS_df = load_from_warehouse(PDS_Path)
PDS_df_with_date = (PDS_df
                        .withColumn("date_of_birth", to_date(PDS_df["Dob"],"yyyyMMdd")) #  convert date of birth to date
                        .withColumn("Trust_Code", substring(PDS_df["Actual_delivery_Ods_Code"],1,3))) #  compute trust code
PDS_df_filtered = PDS_df_with_date.filter(col("date_of_birth") >= Start_Date)

PDS_df_filtered.createOrReplaceTempView("PDS_raw") # convert to SQL temp view

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean PDS

# COMMAND ----------

# DBTITLE 1,Override Deceased Type Flag and exclude Child Health Codes
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW PDS_raw1 AS 
# MAGIC SELECT Der_Pseudo_Nhs_Number, date_of_birth,Gestation_Age,Actual_delivery_Ods_Code, Postcode_Of_Birth,  Trust_Code,
# MAGIC
# MAGIC case when Dod is null then null -- Override Deceased Type flag where no Dod is present
# MAGIC when Num_Days_From_Dob_To_Dod between 1 and 28 and Deceased_Type_Flag = 'Stillbirth' then 'Neonatal' --Override Deceased Type flag when Dod is after Dob
# MAGIC when Deceased_Type_Flag = 'Neonatal - Assumed' then 'Neonatal' -- Update so rest of code still works
# MAGIC when Deceased_Type_Flag = 'Stillbirth - Assumed' then 'Stillbirth'  -- Update so rest of code still works
# MAGIC else Deceased_Type_Flag
# MAGIC end as Deceased_Type_Flag
# MAGIC
# MAGIC FROM PDS_raw
# MAGIC where Actual_delivery_Ods_Code not in ('R0B1D','R1F46','RD1CH','REFCH','RTE86') -- Exclude Child Health Codes

# COMMAND ----------

# DBTITLE 1,Create List of Single Site Trusts
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Single_Site_Trusts AS 
# MAGIC SELECT Trust_Code, count(Site_Code) as Num_Sites
# MAGIC FROM Provider_Site
# MAGIC GROUP BY Trust_Code
# MAGIC HAVING Num_Sites = 1

# COMMAND ----------

# DBTITLE 1,Create Single Site Mapping
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Single_Site_Mapping AS 
# MAGIC SELECT a.Trust_Code, b.Site_Code
# MAGIC FROM Single_Site_Trusts a LEFT JOIN Provider_Site b ON a.Trust_Code = b.Trust_Code

# COMMAND ----------

# DBTITLE 1,Clean Data: Clean site code for Single Site Trusts and filter to term
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW PDS_Single_Sites_Mapped as
# MAGIC Select a.Der_Pseudo_Nhs_Number as Baby_ID, last_day(date_of_birth) as Period, date_of_birth,
# MAGIC                 a.Deceased_Type_Flag, a.Gestation_Age, 
# MAGIC case when b.Site_Code is not null then b.Site_Code else a.Actual_delivery_Ods_Code end as Site_Code_Mapped_Single_Trusts,
# MAGIC case when Postcode_Of_Birth is null then 'Missing' else Postcode_Of_Birth end as Postcode
# MAGIC from PDS_raw1 a left join Single_Site_Mapping b on a.Trust_Code = b.Trust_Code -- Join to single site mapping
# MAGIC where a.Gestation_Age = 'Gestation_over37'
# MAGIC

# COMMAND ----------

# DBTITLE 1,Clean Data: Apply Mergers
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pds_with_mergers as
# MAGIC SELECT distinct a.Baby_ID, a.Period, a.date_of_birth,
# MAGIC                 a.Deceased_Type_Flag, a.Gestation_Age, a.Postcode,
# MAGIC case when b.New_Code is not null then b.New_Code else a.Site_Code_Mapped_Single_Trusts end as Site_Code_Merged
# MAGIC from PDS_Single_Sites_Mapped a left join Site_Mergers b on a.Site_Code_Mapped_Single_Trusts = b.Old_Code -- Join to site mergers table

# COMMAND ----------

# DBTITLE 1,Assess new postcodes (if required)
# MAGIC %sql
# MAGIC --Select distinct Site_Code_Merged, Postcode
# MAGIC --from pds_with_mergers
# MAGIC --where left(Site_Code_Merged,3) = 'ENTER TRUST CODE'

# COMMAND ----------

# DBTITLE 1,Clean Site Codes using Postcode
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pds_with_cleansed_site_code as 
# MAGIC select distinct Baby_ID, date_of_birth, Period, Deceased_Type_Flag, Gestation_Age,  a.Postcode, b.Map_using_postcode, b.Justification,
# MAGIC case when b.Site_Code_New is not null then b.Site_Code_New else a.Site_Code_Merged end as Site_Code
# MAGIC from pds_with_mergers a left join Postcode_Mapping b on a.Site_Code_Merged = b.Site_Code_Old and a.Postcode = b.PostCode
# MAGIC

# COMMAND ----------

# DBTITLE 1,Check for events at unmapped sites
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Unmapped_Sites_Check as
# MAGIC select distinct Baby_ID, Period, Deceased_Type_Flag, Gestation_Age,  Postcode, Site_Code
# MAGIC from pds_with_cleansed_site_code 
# MAGIC where Map_using_postcode = 1 and Justification != 'Postcode same as existing site' --Only check for sites mapped using postcode where postcode is not same as an existing site

# COMMAND ----------

# DBTITLE 1,Summarise events at unmapped sites
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Unmapped_Sites_Total as
# MAGIC select Period,Deceased_Type_Flag,Site_Code, count(distinct Baby_ID) as Total
# MAGIC from Unmapped_Sites_Check
# MAGIC group by  Period,Deceased_Type_Flag,Site_Code

# COMMAND ----------

# DBTITLE 1,Compute Trust Code
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pds_with_trust_code as
# MAGIC select a.*,
# MAGIC case when b.Trust_Name is not null then 1 else 0 end as Current_Site, -- Compute whether site code is in list of existing sites
# MAGIC case when b.Trust_Code is null then left(a.Site_Code,3) 
# MAGIC else b.Trust_Code end as ods_code -- Use Trust code where it is in list of existing sites, otherwise take first three characters as trust code 
# MAGIC from pds_with_cleansed_site_code a LEFT JOIN Provider_Site b on a.Site_Code = b.Site_Code

# COMMAND ----------

# DBTITLE 1,Filter to Current Sites
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pds_Current_Sites_only as
# MAGIC select distinct Baby_ID, date_of_birth, Period, Deceased_Type_Flag, Site_Code, ods_code
# MAGIC from pds_with_trust_code 
# MAGIC where Current_Site = 1

# COMMAND ----------

# DBTITLE 1,Check for potential new sites
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW new_sites_check as
# MAGIC select distinct a.ods_code, a.Site_Code, a.Postcode, a.Gestation_Age, a.Deceased_Type_Flag, a.Period, a.Baby_ID, 
# MAGIC case when b.Trust_Code is not null then 1 else 0 end as Current_Trust -- Check whether site belongs to a current Trust  
# MAGIC from pds_with_trust_code a left join Provider_Site b on a.ods_code = b.Trust_Code --join on Trust Code to list of current sites 
# MAGIC where a.Current_Site = 0 -- check for new sites 
# MAGIC and a.Site_code not in ('DF905','0CF','X19','NW604','8HK81','XMDA1','8JR36')--but ignore Portland, RAF, CSU etc
# MAGIC and left(a.Site_Code,1) != '7' -- and ignore Wales

# COMMAND ----------

# DBTITLE 1,Summarise potential new sites
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW New_Sites_Count as
# MAGIC select Site_Code, Period, Current_Trust, count(distinct Baby_ID) as Total
# MAGIC from new_sites_check
# MAGIC group by Site_Code, Period, Current_Trust

# COMMAND ----------

# MAGIC %md
# MAGIC ## Denominators

# COMMAND ----------

# DBTITLE 1,Denoms: Total births by month
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Total_Births_by_month as 
# MAGIC select Site_Code, ods_code, Period, COUNT(distinct Baby_ID) as Term_Births 
# MAGIC from pds_Current_Sites_only
# MAGIC group by Site_Code, ods_code, Period

# COMMAND ----------

# DBTITLE 1,Denoms: Stillbirths by month
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Stillbirths_by_month as 
# MAGIC select Site_Code, ods_code, Period, COUNT(distinct Baby_ID) as Term_Stillbirths 
# MAGIC from pds_Current_Sites_only
# MAGIC where Deceased_Type_Flag = 'Stillbirth'
# MAGIC group by Site_Code, ods_code, Period

# COMMAND ----------

# DBTITLE 1,Denoms: Combine Total births and Stillbirths
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW Total_and_Stillbirths as 
# MAGIC select a.Site_Code, a.ods_code,a.Period,a.Term_Births, case when b.Term_Stillbirths is null then 0 else b.Term_Stillbirths end as Term_Stillbirths
# MAGIC from Total_Births_by_month a left join Stillbirths_by_month b on a.Site_code = b.Site_code and a.Period = b.Period

# COMMAND ----------

# DBTITLE 1,Denoms: Compute Live Births
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW All_births as
# MAGIC SELECT Site_Code, Period, Term_Births, Term_Births-Term_Stillbirths as Term_Live_Births
# MAGIC from Total_and_Stillbirths

# COMMAND ----------

# DBTITLE 1,Denoms: Create Final Output
# Clean up output
All_births_df = spark.sql("SELECT * FROM All_births") # convert to spark
Sites = All_births_df.select("Site_Code").distinct() # Distinct Site Code
Periods = All_births_df.select("Period").distinct() # Distinct Periods

all_combinations = Sites.crossJoin(Periods) 

All_Births_Complete = all_combinations.join(All_births_df, ["Site_Code", "Period"], "left_outer") 
All_Births_Complete = All_Births_Complete.fillna({"Term_Births": 0, "Term_Live_Births": 0}) # enter 0's for missing values
All_Births_Complete = All_Births_Complete.withColumn("Year", year(col("Period")))

All_Births_Complete.createOrReplaceTempView("Denoms_Final") # Convert back to SQL
 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Events

# COMMAND ----------

# DBTITLE 1,Events
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pds_events as
# MAGIC SELECT distinct a.ods_code, a.Site_Code, b.Site_Name, b.Trust_Name, a.date_of_birth as test_date, date_format(a.Period, 'yyyy-MM') as year_month,
# MAGIC 1 as event_count, 
# MAGIC case when a.Deceased_Type_Flag = 'Stillbirth' then 'SB'
# MAGIC when a.Deceased_Type_Flag = 'Neonatal' then 'NND' end as outcome, Baby_ID
# MAGIC
# MAGIC FROM pds_Current_Sites_only a left join Provider_Site b on a.Site_Code = b.Site_Code
# MAGIC WHERE a.Deceased_Type_Flag IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to Lake

# COMMAND ----------

save_to_Analysis_Lake("pds_events",Events_Path)
save_to_Analysis_Lake("Unmapped_Sites_Check",Unmapped_Sites_Path)
save_to_Analysis_Lake("New_Sites_Count",New_Sites_Path)
#save_to_Analysis_Lake("Denoms_Final",Denoms_Output_Path) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checks

# COMMAND ----------

#Check - new site with more than 10 births in a month
row_count_New_Sites_Count = spark.sql("SELECT COUNT(*) as count FROM New_Sites_Count where Total >10 and Current_Trust = 0").collect()[0]["count"] 

#Check - new site at a current trust
row_count_New_Site_Current_Trust = spark.sql("SELECT COUNT(*) as count FROM New_Sites_Count where Current_Trust = 1").collect()[0]["count"] 

#Check - unmapped site with an event
row_count_unmapped1 = spark.sql("SELECT COUNT(*) as count FROM Unmapped_Sites_Total where Deceased_Type_Flag is not null").collect()[0]["count"]

#Check - unmapped site with more than 10 births in a month
row_count_unmapped2 = spark.sql("SELECT COUNT(*) as count FROM Unmapped_Sites_Total where Total > 10").collect()[0]["count"]

#Latest event
Latest_Birth_Date = spark.sql("SELECT max(date_of_birth) as Date FROM pds_Current_Sites_only").collect()[0]["Date"]

# COMMAND ----------

    # load libraries

    import requests

    import json
 
    # get teams webhook from your power automate flow

    teams_webhook_url = MS_Teams_Link
 
    # define the logic of the message you want to post in a channel, you can make it more complex

    message = "PDS Data refresh has been successful. The latest birth date is: " + str(Latest_Birth_Date)

    # Create the payload to send to Teams

    payload = {"text": message}

    headers = {"Content-Type": "application/json"}
 
    # Send the message to Teams

    response = requests.post(teams_webhook_url, data=json.dumps(payload), headers=headers)
 
    # Check if the message was sent successfully

    if response.status_code == 200:

        print("Message sent directly to Teams via incoming webhook successfully!")

    elif response.status_code == 202:

        print("Message sent directly to Teams via Power Automate successfully!")

    else:

        print(f"Failed to send message to Teams. Status code: {response.status_code}")

 

# COMMAND ----------

#Raise errors if any validation fails
if row_count_New_Sites_Count >0 :
    raise ValueError("At Least one new site to investigate more than 10 births per month") 
else: 
    print("OK")

if row_count_New_Site_Current_Trust >0 :
    raise ValueError("At Least one new site to investigate at a current trust that needs mapping") 
else: 
    print("OK")

if row_count_unmapped1 >0 :
    raise ValueError("Unmapped Site to Investigate with at least 1 event") 
else: 
    print("OK")

if row_count_unmapped2 >0 :
    raise ValueError("Unmapped Site to Investigate with more than 10 births per month") 
else: 
    print("OK") 

