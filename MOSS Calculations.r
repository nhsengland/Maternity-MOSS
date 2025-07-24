# Databricks notebook source
# DBTITLE 1,Load Functions and Config
source("Functions.R")
source("Config.R")

# COMMAND ----------

# DBTITLE 1,Retrieve SAS Token manually
# Open the link and copy/paste the code
#library(AzureAuth)
#library(AzureKeyVault)
#token <- AzureAuth::get_azure_token(
#  resource = "https://vault.azure.net",
#  tenant = "common", app = Azure_App_ID,
#  auth_type = "device_code"
#)

#vault <- AzureKeyVault::key_vault(Azure_URL, token = token) # Azure_URL - name of URL relating to SAS key vault
#SAStok <- vault$secrets$get(SAS_Secret) # Name of SAS Secret relating to ADLS Container
#sas_value <- SAStok$value

# COMMAND ----------

# DBTITLE 1,Retrieve SAS token from Azure Data Factory
dbutils.widgets.text("secure", "", "Enter SAS Token")
sas_value <-dbutils.widgets.get("secure")

# COMMAND ----------

# DBTITLE 1,Load Libraries
library(dplyr)
library(tidyr)
library(lubridate)
library(AzureStor)
install.packages("CUSUMdesign")
library(CUSUMdesign)
install.packages("httr2")
library("httr2")
library(arrow)
library(glue)

# COMMAND ----------

# Set up Links to Analysis Lake
adls <- storage_endpoint(Azure_Storage_URL, sas = sas_value)
cont <- storage_container(adls, Azure_Storage_Container)

# COMMAND ----------

# DBTITLE 1,Load Data
Data_Ref_Rate <- storage_read_csv(cont, Path_Ref_Rate , show_col_types = FALSE) 
Data_Provider_Site <- storage_read_csv(cont, Path_Provider_Site , show_col_types = FALSE) 
Data_Events <- storage_read_csv(cont, Path_Events, show_col_types = FALSE) %>% mutate(test_date = as.Date(test_date, format="%Y-%m-%d"))
Data_Denoms <- storage_read_csv(cont, Path_Denoms , show_col_types = FALSE)

# COMMAND ----------

# DBTITLE 1,Specify Parameters
selected_measure <- "All" #ALTER TO ANYTHING OTHER THAN "All" TO CHANGE DENOMINATOR TO LIVE BIRTHS ONLY
L2_ARL <- 100
L1_ARL <- 20

# ARL is the estimated number of continuous observations before a false alarm is triggered. 
# It is equivalent to the false positive rate or the Type I Error. A false positive rate of 1% (p=0.01) is the same as ARL=100.
# The CUSUM is designed to be a one tail algorithm, to test for departure from the benchmark upwards or downwards, but not both. 
# If the user wishes to have a two tail test for both at the same time, then he/she needs to use two CUSUMs, one for each tail, but the ARL for each should be half of that required for the one tail situation.

# COMMAND ----------

# MAGIC %md
# MAGIC CUSUM Calculations

# COMMAND ----------

# DBTITLE 1,CUSUM: Data Prep
#Calculate monthly totals of events, allocate correct denominator period
Monthly_Events <- Data_Events %>%
select(-ods_code,-Site_Name,-Trust_Name) %>%
  summarise(monthly_count = sum(event_count), .by = c(Site_Code, year_month))  %>%
mutate(denom_period = as.integer(as.integer(substr(year_month,1,4)) - 1),
      refrate_period = paste0(as.integer(substr(year_month,1,4))-3,"-", as.integer(substr(year_month,1,4))-1))

#Get unique time periods for events and denoms
unique_events_periods <- Monthly_Events %>% distinct(year_month)
unique_denoms_periods <- Monthly_Events %>% distinct(denom_period)
List_Event_Denom_refrate_Periods <- Monthly_Events %>% distinct (year_month, denom_period, refrate_period)

# COMMAND ----------

# DBTITLE 1,CUSUM: Data Prep
# Compute the annual denominators (monthly average number of births) to use in the CUSUM for just the time periods required
Denoms_for_CUSUM <- Data_Denoms %>% 
semi_join(unique_denoms_periods, by = c("Year" = "denom_period")) %>% #Remove any denominator rows falling outside the denom_period set above
summarise(Monthly_Term_Births = mean(Term_Births), Monthly_Term_Live_Births = mean(Term_Live_Births), 
.by = c("Site_Code","Year"))

# COMMAND ----------

# DBTITLE 1,CUSUM: Unique Site List
unique_sites <- Denoms_for_CUSUM %>% distinct(Site_Code)

# COMMAND ----------

# DBTITLE 1,CUSUM: Create All combinations
#Create list of all possible combinations of site codes and periods for the CUSUM calcs 
All_Combos_Sites_Periods <- unique_sites %>%
cross_join(unique_events_periods)

# COMMAND ----------

# DBTITLE 1,CUSUM: Data Prep
#Add events data onto master list of sites + periods. Fill in blanks with zeros to ensure every site has a value for the number of events for every period.
All_Combos_with_events <- All_Combos_Sites_Periods %>%
left_join(Monthly_Events, by = c("Site_Code", "year_month")) %>%
select(-denom_period,-refrate_period) %>% # Remove Denom Period and refrate period columns
complete(Site_Code, year_month, fill = list(monthly_count=0)) %>%
left_join (List_Event_Denom_refrate_Periods, by = c("year_month")) # Add Denom Period and refrate period back in at this stage to ensure it is populated for all rows.

# COMMAND ----------

# DBTITLE 1,CUSUM: Prep Ref Rates
#Obtain national ref rates
National_Ref_Rates <- Data_Ref_Rate %>% 
select(Year, Ref_Rate)

# COMMAND ----------

# DBTITLE 1,CUSUM: Final Prep
#Compute dataframe to use within CUSUM calculations
df_for_cusum <- All_Combos_with_events %>%
left_join(Denoms_for_CUSUM, by = c("Site_Code", "denom_period" = "Year"))%>% # join to denominators table on the denom period
left_join(National_Ref_Rates, by = c("refrate_period" = "Year"))%>% # join to ref rate table on the ref rate period
mutate(Event_Year = substr(year_month,1,4),
  denominator = case_when(selected_measure == "All" ~ Monthly_Term_Births,
TRUE ~ Monthly_Term_Live_Births)) %>% #Use live births only when measures have been set to not all 4
select(-Monthly_Term_Live_Births, -Monthly_Term_Births, -denom_period)%>% 
rename(numerator = monthly_count)

# COMMAND ----------

# DBTITLE 1,CUSUM: Calculations
# Compute CUSUM calculations for all sites

output_table <- data.frame()  #Blank Output table

for (i in 1:nrow(unique_sites)){ # For each Site
  site_code_for_loop <- unique_sites[i,] %>% pull() # Obtain Site Code
  Data <- df_for_cusum %>% filter(Site_Code == site_code_for_loop) # Obtain data to use in CUSUM Calcs for each site

  # Define Model parameters
  theModel = "Z"   #F for Fast Initial Response (Cusum starts at 0.5),   Z for zero (Cusum starts at 0)  

  # Calculate In-control and Out of Control values for each year
  Annual_IC_OC <- Data %>%
  distinct (Event_Year,Ref_Rate,denominator) %>%
  mutate(inControlCount = round(Ref_Rate*denominator,2)) %>% #In control number of deaths per months
  mutate(outControlCount = 2*inControlCount) %>% # Out control number of deaths e.g. Double of reference rate
  mutate(ref_value_unrounded = (outControlCount - inControlCount) / (log(outControlCount) - log(inControlCount)) )%>%
  mutate(ref_value = round(ref_value_unrounded/0.01,0)*0.01 )

  #Get a list of years from the events data to loop through
  Unique_Years <- Data %>% 
  distinct(Event_Year)

  #Create a table ready to populate with CUSUM inputs
  cusum_inputs <- Unique_Years %>%
  mutate(k=0, h_L1 = 0, h_L2 = 0)

  #Calculate the thresholds for year separately
  for(j in 1:nrow(Unique_Years)) {
    Year <- Unique_Years[j,] %>%pull()
    ref_value <- Annual_IC_OC %>% filter(Event_Year == Year) %>% select(ref_value) %>% pull()
    ref_value_unrounded <- Annual_IC_OC %>% filter(Event_Year == Year) %>% select(ref_value_unrounded) %>% pull()
    inControl <- Annual_IC_OC %>% filter(Event_Year == Year) %>% select(inControlCount) %>% pull()
    outControl <- Annual_IC_OC %>% filter(Event_Year == Year) %>% select(outControlCount) %>% pull()

    ##LEVEL 1 THRESHOLD
    # Calculate k and h for Level 1 Threshold
    arl = L1_ARL
 
    t1 <- try(getH(distr=3, ref=ref_value, ICmean=inControl, OOCmean=outControl, ARL=arl, type=theModel),silent = TRUE)#Assumes Poisson Distribution
    t2 <- try(getH(distr=3, ref=round(ref_value_unrounded/0.1,0)*0.1, ICmean=inControl, OOCmean=outControl, ARL=arl, type=theModel), silent = TRUE) #Assumes Poisson Distribution
    t3 <- try(getH(distr=3, ref=round(ref_value_unrounded/0.2,0)*0.2, ICmean=inControl, OOCmean=outControl, ARL=arl, type=theModel), silent = TRUE) #Assumes Poisson Distribution
    t4 <- try(getH(distr=3, ref=round(ref_value_unrounded/0.5,0)*0.5, ICmean=inControl, OOCmean=outControl, ARL=arl, type=theModel), silent = TRUE) #Assumes Poisson Distribution
    
    result <- if("try-error" %in% class(t1)) {if("try-error" %in% class(t2)) {
    if("try-error" %in% class(t3)) {t4} else{t3}  }   else {t2}} else {t1}
  
    k = result$ref
    h_L1 = round(result$DI, digits = 6)
  
    ## LEVEL 2 THRESHOLD
    # Calculate h for Level 2 Threshold
    arl = L2_ARL
  
    t1 <- try(getH(distr=3, ref=ref_value, ICmean=inControl, OOCmean=outControl, ARL=arl, type=theModel),silent = TRUE)#Assumes Poisson Distribution
    t2 <- try(getH(distr=3, ref=round(ref_value_unrounded/0.1,0)*0.1, ICmean=inControl, OOCmean=outControl, ARL=arl, type=theModel), silent = TRUE) #Assumes Poisson Distribution
    t3 <- try(getH(distr=3, ref=round(ref_value_unrounded/0.2,0)*0.2, ICmean=inControl, OOCmean=outControl, ARL=arl, type=theModel), silent = TRUE) #Assumes Poisson Distribution
    t4 <- try(getH(distr=3, ref=round(ref_value_unrounded/0.5,0)*0.5, ICmean=inControl, OOCmean=outControl, ARL=arl, type=theModel), silent = TRUE) #Assumes Poisson Distribution
  
    result2 <- if("try-error" %in% class(t1)) {if("try-error" %in% class(t2)) {
    if("try-error" %in% class(t3)) {t4} else{t3}  }   else {t2}} else {t1}
  
    h_L2 = round(result2$DI, digits = 6)

    cusum_inputs[j,2] = k
    cusum_inputs[j,3] = h_L1
    cusum_inputs[j,4] = h_L2
  }
  
  #Join the caclaulated thresohlds back to the event data ready for the  cusum calculations
  Data2 <- Data %>%
  left_join(cusum_inputs, by = c("Event_Year"))

  # Calculate the CUSUM for each site taking into account the yearly thresholds and reset in the same month
  output_of_one_cusum = Cusum_Poisson_Counts_Immediate(Data2)  
  
  # Add site_code to output table
  output_of_one_cusum$Site_Code <- site_code_for_loop
  
  output_table = rbind(output_table, output_of_one_cusum) #combines outputs from each run into one big table 
}

# COMMAND ----------

# DBTITLE 1,CUSUM: Format Output
# Format CUSUM Output
CUSUM_Output_formatted  <- output_table %>%
mutate(period_formatted = as.Date(paste0(period,"-01"))) %>% # convert to date format
mutate(Cusum_Period = ceiling_date(period_formatted, "month") - days(3), # Set period to be third to last day of the month to allow for a potential reset row + threshold row
       Level_of_Signal = case_when(
             Cusum_Statistic >= h_L2 ~ 2,
             Dat == 0 ~ 0, # Remove level 1 signals where there have been no events in a month
             Cusum_Statistic >= h_L1 ~1,
             TRUE ~ 0),
       Reset_Flag = "",
       Line_Part_Increment = 0,
       Line_Type = "CUSUM",
       Date_of_signal = format(period_formatted, "%B %Y"))  

# COMMAND ----------

# DBTITLE 1,CUSUM: Events Description
# Compute Events Description for tooltip on CUSUM Chart and add to output
Event_Description_Tooltip <- Data_Events %>%
  select(-ods_code, -Site_Name, -Trust_Name) %>%
  summarise(monthly_count_outcomes = sum(event_count), .by = c(Site_Code, year_month, outcome)) %>% # Count monthly event types by site
  mutate(outcome = case_when(
    outcome == 'NND' ~ 'Neonatal Death(s)',
    outcome == 'SB' ~ 'Stillbirth(s)',
    outcome == 'HIE' ~ 'HIE(s)',
    TRUE ~ 'Unknown'
  )) %>% # Replace codes with correct text
  mutate(event_type = paste(as.integer(monthly_count_outcomes), outcome)) %>% # Combine number of events with text
  group_by(Site_Code, year_month) %>%
  summarise(Events_Desc = toString(event_type), .groups = "drop") # Concatenate all monthly event types together into a single string

# Add in text for events desc when no events
CUSUM_Output_formatted_with_tooltip <- CUSUM_Output_formatted %>% 
  left_join(Event_Description_Tooltip, by = c("Site_Code", "period" =  "year_month")) %>%
  mutate(Events_Desc = coalesce(Events_Desc, "No events"))

# COMMAND ----------

# DBTITLE 1,CUSUM: Signal Summary
#Summarise number of signals over 6 and 12 months amd add to output
CUSUM_output_with_tooltip_signal_summary <- CUSUM_Output_formatted_with_tooltip %>%
mutate (max_period = max(Cusum_Period)) %>% #Get latest month in table
mutate(within_6months = if_else (abs(interval(floor_date(Cusum_Period,"month"), floor_date(max_period, "month")) %/% months(1)) <=5,1,0), , #Columns for last 6 months
       within_12months = if_else (abs(interval(floor_date(Cusum_Period,"month"), floor_date(max_period, "month")) %/% months(1)) <=11,1,0) #Columns for last 12 months
       ) %>%
#Columns for where level 1 and 2 signals occur in last 6 and 12 months
mutate(Level1_6months = if_else(within_6months ==1 & Level_of_Signal ==1, 1, 0),
       Level2_6months = if_else(within_6months ==1 & Level_of_Signal ==2, 1, 0),
       Level1_12months = if_else(within_12months ==1 & Level_of_Signal ==1, 1, 0),
       Level2_12months = if_else(within_12months ==1 & Level_of_Signal ==2, 1, 0))

# COMMAND ----------

# DBTITLE 1,CUSUM: Output without reset
CUSUM_Output_without_reset_rows <- CUSUM_output_with_tooltip_signal_summary %>% 
select (Date = Cusum_Period,
Date_of_signal,
Site_Code,
Cusum_Statistic,
Level1_Threshold = h_L1,
Level2_Threshold = h_L2,
Level_of_Signal,
Reset_Flag,
Events_Desc,
Level1_6months,
Level2_6months,
Level1_12months,
Level2_12months,
Line_Part_Increment,
Line_Type,
denominator, # only needed for VLAD calculations
Ref_Rate # only needed for VLAD calculations
)

# COMMAND ----------

# DBTITLE 1,CUSUM: Reset Rows
Reset_Rows <- CUSUM_Output_without_reset_rows %>%
filter(Level_of_Signal ==2) %>%
  mutate(Date = Date + days(1), # Assign 2nd to last day of month
         Cusum_Statistic = 0, # Reset Cusum to Zero
         Level_of_Signal = 0, # Display as no signal
         Reset_Flag = "Reset", # Add in label for chart
         Events_Desc = "Reset", # Amend label for tooltip
         Level1_6months = 0, #Override wih 0
         Level2_6months = 0, #Override wih 0
         Level1_12months = 0, #Override wih 0
         Level2_12months = 0, #Override wih 0
         Line_Part_Increment = 1)  %>% #Increment line_part up one for each reset
  select(-Level1_Threshold,-Level2_Threshold, -denominator, -Ref_Rate)

# COMMAND ----------

# DBTITLE 1,CUSUM: Threshold Rows
Thresholds <- CUSUM_Output_without_reset_rows %>%
  mutate(Start_Date = as.Date( paste0(year(Date),"/01/01")) , #Start of Year
         End_Date = as.Date( paste0(year(Date),"/12/31"))) %>% #End of Year
  distinct(Site_Code, Start_Date, End_Date,Level1_Threshold,Level2_Threshold) %>%
  pivot_longer(cols = starts_with("Level"), names_to= "Line_Type", values_to= "Cusum_Statistic")

CUSUM_Min_Period <- CUSUM_Output_without_reset_rows %>% 
  summarise(First_Month = min(Date)) %>%
  mutate(First_Date = First_Month - days(1)) %>% # Adjust by a day
  pull()

CUSUM_Max_Period <- CUSUM_Output_without_reset_rows %>% 
  summarise(Latest_Month = max(Date)) %>% 
  mutate(Latest_Month2 = ceiling_date(Latest_Month, "month") - days(1)) %>% 
    pull() # Assign last day of latest month

Threshold_Rows <- Thresholds %>%
  pivot_longer(c("Start_Date","End_Date"), names_to = "Names", values_to = "Date")%>%
  mutate(Date_of_signal = if_else(Date>CUSUM_Max_Period,"Ref_End","Ref"))%>%
  mutate(Date = case_when(Date>CUSUM_Max_Period ~ CUSUM_Max_Period,
                          Date<CUSUM_Min_Period ~ CUSUM_Min_Period,
                          TRUE ~ Date), # Adjust the date so that it doesn't exceed the latest month of available events data
         Level_of_Signal = case_when(Date_of_signal=="Ref_End" & Line_Type=="Level1_Threshold" ~ 1,
                                     Date_of_signal=="Ref_End" & Line_Type=="Level2_Threshold" ~ 2,
                                     TRUE ~ 0),
         Reset_Flag = case_when(Date_of_signal=="Ref_End" & Line_Type=="Level1_Threshold" ~ "Level 1 Threshold",
                                Date_of_signal=="Ref_End" & Line_Type=="Level2_Threshold" ~ "Level 2 Threshold",
                                TRUE ~ ""),
         Events_Desc = "NA",
         Level1_6months = 0,
         Level2_6months = 0,
         Level1_12months = 0,
         Level2_12months = 0,
         Line_Part = 0)%>%
  select(Date,
         Date_of_signal,
         Site_Code,
         Cusum_Statistic,
         Level_of_Signal,
         Reset_Flag,
         Events_Desc,
         Level1_6months,
         Level2_6months,
         Level1_12months,
         Level2_12months,
         Line_Type,
         Line_Part)

# COMMAND ----------

# DBTITLE 1,CUSUM: Final Output
#Add Reset rows to the final CUSUM output and compute Line Part
CUSUM_Final_Output <- CUSUM_Output_without_reset_rows %>%
  select(-Level1_Threshold,-Level2_Threshold, -denominator, -Ref_Rate) %>%
    rbind(Reset_Rows) %>%
  arrange(Site_Code,Date) %>%
  group_by(Site_Code) %>%
  mutate(Line_Part = as.integer(1L + cumsum(Line_Part_Increment)))%>%
  select(-Line_Part_Increment) %>% # remove columns not needed
  rbind(Threshold_Rows)

# COMMAND ----------

# MAGIC %md
# MAGIC Daily Events Summary

# COMMAND ----------

# DBTITLE 1,Events: Description
# Compute Events Description for tooltip on VLAD Chart
Daily_Event_Type_Tooltip <- Data_Events %>%
  select(-ods_code, -Site_Name, -Trust_Name) %>%
  summarise(daily_count_outcomes = sum(event_count), .by = c(Site_Code, test_date, outcome)) %>% # Count monthly event types by site
  mutate(outcome = case_when(
    outcome == 'NND' ~ 'Term Neonatal Death(s)',
    outcome == 'SB' ~ 'Term Stillbirth(s)',
    outcome == 'HIE' ~ 'Term HIE(s)',
    TRUE ~ 'Unknown'
  )) %>% # Replace codes with correct text
  mutate(event_type = paste(as.integer(daily_count_outcomes), outcome)) %>% # Combine number of events with text
  group_by(Site_Code, test_date) %>%
  summarise(Event_Type = toString(event_type), .groups = "drop") # Concatenate all daily event types together into a single string



# COMMAND ----------

# DBTITLE 1,Events: Daily Totals
Daily_Events <- Data_Events %>%
select(-ods_code,-Site_Name,-Trust_Name,-year_month,-outcome) %>%
  summarise(daily_count = sum(event_count), .by = c(Site_Code, test_date))

# COMMAND ----------

# DBTITLE 1,Events: Final Table
#Combine tooltip and count of daily events into a single table
Daily_Event_Summary <- Daily_Events %>%
left_join(Daily_Event_Type_Tooltip, by = c("Site_Code", "test_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC VLAD

# COMMAND ----------

# DBTITLE 1,VLAD: Calculation
#Create empty output table
output_table_vlad <- data.frame() 

#Loop through VLAD calculations for every site
for (i in 1:nrow(unique_sites)){
  Site <- unique_sites[i,1] %>% pull() #Get site_code
  CUSUM_Output_for_vlad <- CUSUM_Output_without_reset_rows %>% filter(Site_Code == Site) %>% select(Date, denominator, event_rate = Ref_Rate, Site_Code, Signal_Flag = Level_of_Signal) #Make monthly site table
  Site_daily_events <- Daily_Event_Summary %>% filter(Site_Code == Site) #Make daily site table
  
  #Create initial daily table containing Site Code, Level of signal for the month, Date, average births per day.
  Site_vlad_daily <- CUSUM_Output_for_vlad  %>% rowwise() %>% 
  mutate(
    Date_VLAD = list(seq.Date(from = ceiling_date(Date, "month") - months(1), to = ceiling_date(Date, "month") - days(1), by = "day"))
    ) %>%  #List of dates in month
  unnest(Date_VLAD) %>% #Expand list of dates out into one row for each
  ungroup() %>% #Remove rowwise operation
  mutate(births_in_day = denominator / days_in_month(Date_VLAD)) %>% #Divide average births/month by days in month to get average births/day 
  select(-Date, -denominator) %>%

  left_join(Site_daily_events, by = c("Date_VLAD" = "test_date", "Site_Code")) %>% #Join to daily events table
  replace_na(list(daily_count = 0)) %>% #Fill nulls with 0s 
  
  mutate(expected_events = births_in_day*event_rate, #Use national rate to calculate expected events per day
    excess_events = daily_count - expected_events, #Outcome less Expected per day
    cmltv_excess_events = cumsum(excess_events),
    Signal_Flag = ifelse(daily_count > 0, Signal_Flag, 0) #Ensure signal is 0 when no events on that day
    ) %>% #Calculate cumulative excess events  using national rate to calculate expected events
  select(Date_VLAD, Site_Code, cmltv_excess_events, Signal_Flag, daily_count, Event_Type) 

  output_table_vlad = rbind(output_table_vlad, Site_vlad_daily) #combines outputs from each run into one big table
}

# COMMAND ----------

# MAGIC %md
# MAGIC Final Output

# COMMAND ----------

# DBTITLE 1,Final Output Part 1
#Join VLAD to Provider Site
VLAD_Site_Info <- output_table_vlad %>%
left_join(Data_Provider_Site, by = "Site_Code") %>%
select(Date = Date_VLAD,
Site_Code,
Site_Name,
ODS_Code = Trust_Code,
ODS_Name = Trust_Name,
Excess_Events = cmltv_excess_events,
Signal_Flag,
Event_Type)

# COMMAND ----------

# DBTITLE 1,Final Output part 2
#Create final output by joining to CUSUM
Final_Table_without_email_notification <- VLAD_Site_Info %>%
  left_join(CUSUM_Final_Output, by = c("Site_Code", "Date")) %>%
  mutate(Excess_Events = case_when(is.na(Line_Type) ~ Excess_Events,
                                   Line_Type != "Level2_Threshold" ~ Excess_Events,
                                   TRUE ~ NA_real_))

# COMMAND ----------

# DBTITLE 1,Summarise Current Signals
Signal_Summary <- Final_Table_without_email_notification %>%
filter(Level_of_Signal %in% c(1,2) & Line_Type == "CUSUM") %>%
mutate(ID = paste0(Site_Code,"-",Date_of_signal,"-",Level_of_Signal)) %>%
select (ID,Site_Code,Site_Name,ODS_Code,ODS_Name)

# COMMAND ----------

# DBTITLE 1,Load in Previous Signal Summary
Old_Signal_Summary <- storage_read_csv(cont, "/MOSSmart/MOSSmart/Final_outputs/Signal_Summary.csv", show_col_types = FALSE)

# COMMAND ----------

# DBTITLE 1,Compute new signals
Signal_Compare_New_Signals <- Signal_Summary %>%
left_join(Old_Signal_Summary, by = "ID") %>% 
rename(Site_Code_New = Site_Code.y) %>%
filter(is.na(Site_Code_New)) %>%
select(ID, Site_Name = Site_Name.x) %>%
separate(ID, into = c("Site_Code","Date_of_signal","Level_of_Signal"), sep = "-") %>%
select(-Level_of_Signal)%>%
mutate(Email_Trust=1) 

# COMMAND ----------

# DBTITLE 1,Final Output: Add in Email Notification
Final_Table <- Final_Table_without_email_notification %>%
left_join(Signal_Compare_New_Signals, by = c("Site_Code","Date_of_signal","Site_Name"))%>%
mutate(Email_Trust = case_when(Email_Trust == 1 ~ 1,
TRUE ~ 0))

# COMMAND ----------

# DBTITLE 1,Check: Compute Signals that are now lower - Part 1
Signal_Compare_Lower_Signals <- Old_Signal_Summary %>%
left_join(Signal_Summary, by = "ID") %>% 
rename(Site_Code_New = Site_Code.y) %>%
filter(is.na(Site_Code_New)) %>%
select(ID) %>%
separate(ID, into = c("Site_Code","Date_of_signal","Level_of_Signal_Old"), sep = "-")

# COMMAND ----------

# DBTITLE 1,Check: Compute Signals that are now lower - Part 2
Signal_Compare_Lower_Signals2 <- Signal_Compare_Lower_Signals %>%
left_join(Final_Table_without_email_notification, by = c("Site_Code","Date_of_signal"))%>%
mutate(Level_of_Signal_Old = as.numeric(Level_of_Signal_Old),
       Level_of_Signal_New = as.numeric(Level_of_Signal)) %>%
select(Site_Code,Date_of_signal,Level_of_Signal_Old,Level_of_Signal_New) %>%
filter(Level_of_Signal_New < Level_of_Signal_Old)

# COMMAND ----------

# MAGIC %md
# MAGIC Save Outputs

# COMMAND ----------

# DBTITLE 1,Save Signal Summary
storage_write_csv(Signal_Summary, cont, file = Path_Output_Signal_Summary) # Save latest version for use next time code is run
Daily_Path =paste0(Signal_Summary_Folder,Sys.Date(),".csv")
storage_write_csv(Signal_Summary, cont, file = Daily_Path) # Keep a record of all daily summaries

# COMMAND ----------

# DBTITLE 1,Save Output PROD
storage_write_csv(Final_Table, cont, file = Path_Output_PROD)
Write_to_Lake_Parquet(Final_Table, file = Path_Output_PROD_Parquet)

# COMMAND ----------

# DBTITLE 1,Save Output UAT
#storage_write_csv(Final_Table, cont, file = Path_Output_UAT)
#Write_to_Lake_Parquet(Final_Table, file = Path_Output_UAT_Parquet)

# COMMAND ----------

# DBTITLE 1,Send Notification to copyback PROD data
# Copy the file from UDALCB_InstructionFile_Source to UDALCB_InstructionFile_Destination
Copyback_File_PROD <- storage_read_csv(cont,UDAL_CB_Source_Location_PROD)
storage_write_csv(Copyback_File_PROD, cont, file = UDAL_CB_Destination_PROD)

# COMMAND ----------

# DBTITLE 1,Send Notification to copy back UAT Data
# Copy the file from UDALCB_InstructionFile_Source to UDALCB_InstructionFile_Destination
#Copyback_File_UAT <- storage_read_csv(cont,UDAL_CB_Source_Location_UAT)
#storage_write_csv(Copyback_File_UAT, cont, file = UDAL_CB_Destination_UAT)

# COMMAND ----------

# DBTITLE 1,Compute text for Teams notification
Latest_Date <- Final_Table %>%
filter(!is.na(Event_Type))%>%
summarise(Latest_Date = max(Date))%>%
pull()

Number_new_signals <- Signal_Compare_New_Signals %>% nrow()
Number_lower_signals <- Signal_Compare_Lower_Signals2 %>% nrow()

New_Site_Names <- if (Number_new_signals==0) {paste("")}
      else { if(Number_new_signals==1) {paste0("(",Signal_Compare_New_Signals[1,3],")")}

             if(Number_new_signals==2) {paste0("(",Signal_Compare_New_Signals[1,3]," and ",Signal_Compare_New_Signals[2,3],")")}    

             else{paste0("(",Signal_Compare_New_Signals[1,3]," and ",Signal_Compare_New_Signals[2,3]," plus others)")}      
      }

text_to_send = case_when(Number_lower_signals ==0 ~ glue('MOSS Calculations have run successfully. The latest event date is {Latest_Date}. {Number_new_signals} new signal(s) were generated today {New_Site_Names}'),
                         TRUE ~ glue('MOSS Calculations have run successfully. The latest event date is {Latest_Date}. {Number_new_signals} new signal(s) were generated today. {New_Site_Names}. PLEASE NOTE that {Number_lower_signals} site(s) had a lower level of signal today.')
)

# COMMAND ----------

# DBTITLE 1,Send Notification to Teams
response <- request(teams_webhook_url) |> 
  req_body_json(list(text = text_to_send)) |> 
  req_perform()
