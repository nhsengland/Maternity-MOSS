#Cusum Poisson counts with reset to 0 occurring immediately, rather than in following month
Cusum_Poisson_Counts_Immediate <- function(Data) { #
  dat=Data$numerator 
  denominator=Data$denominator
  Ref_Rate = Data$Ref_Rate
  h_L1 = Data$h_L1
  h_L2 = Data$h_L2
  k = Data$k

  cusum <- tibble(period = Data$year_month 
                  , Dat = dat,  Cusum_Statistic = 0, denominator = denominator, h_L1 = h_L1, h_L2 = h_L2, k=k, Ref_Rate = Ref_Rate)
  Cusum_Statistic = 0
  if(theModel=="F"){
    Cusum_Statistic = h / 2
  }
  for(i in 1 : length(dat)){
    h_L1 <- Data %>% filter(row_number() == i) %>% distinct(h_L1) %>% pull()
    h_L2 <- Data %>% filter(row_number() == i) %>% distinct(h_L2) %>% pull()
    k <- Data %>% filter(row_number() == i) %>% distinct(k) %>% pull()
        
    if(round(Cusum_Statistic, digits = 6) >= h_L2) {
      Cusum_Statistic = dat[i] - k
    if(outControl>inControl){# Up
      if(Cusum_Statistic<0){
        Cusum_Statistic = 0
      }
    }
    else {                              # down
      if(Cusum_Statistic>0){
        Cusum_Statistic = 0
      }
    }
    } 
    
    else {
    
    Cusum_Statistic = Cusum_Statistic + dat[i] - k
   # Reset_Flag = 0
    if(outControl>inControl){# Up
      if(Cusum_Statistic<0){
        Cusum_Statistic = 0
      }
    }
    else {                              # down
      if(Cusum_Statistic>0){
        Cusum_Statistic = 0
      }
    }
    }  
  
    cusum[i,3] = Cusum_Statistic
  }
  cusum$Cusum_Statistic <- round(cusum$Cusum_Statistic, digits = 6)
  return(cusum)
}

Write_to_Lake_Parquet <- function(Table, file) {
local_parquet_path <- tempfile(fileext = "temp.parquet")  # Write the data frame to a Parquet file locally
write_parquet(Table, local_parquet_path)
storage_upload(cont, local_parquet_path, dest = file)
}
