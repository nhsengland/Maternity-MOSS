#Cusum Poisson counts with reset to 0 occurring immediately, rather than in following month
Cusum_Poisson_Counts_Immediate <- function(Data) { #
  dat=Data$numerator 
  denominator=Data$denominator
  Ref_Rate = Data$Ref_Rate
  h_L1 = Data$h_L1
  h_L2 = Data$h_L2
  k = Data$k
  denrat = Data$denrat

  cusum <- tibble(period = Data$year_month, Dat = dat,  Cusum_Statistic = 0, denominator = denominator, h_L1 = h_L1, h_L2 = h_L2, k=k, Ref_Rate = Ref_Rate, denrat=denrat)
  Cusum_Statistic = 0

  for(i in 1 : length(dat)){

  # Adjust previous CUSUM value where denrat changes to ensure it is a multiple of the new denrat
    if(i > 1 && denrat[i] != denrat[i-1]) {
    Cusum_Statistic <- round(Cusum_Statistic * denrat[i]) / denrat[i]
    }    

  # Reset CUSUM when level 2 threshold is crossed so following month starts from 0
  # For this, we need to compare the scaled versions of the CUSUM and the threshold to avoid any rounding issues from floating point representation
    scaled_cusum <- round(Cusum_Statistic * denrat[i])
    scaled_h_L2 <- round(h_L2[i] * denrat[i])

    if(scaled_cusum >= scaled_h_L2) {
      Cusum_Statistic = dat[i] - k[i]
    }
    
  # Standard CUSUM calculation if there is no reset
    else {
      Cusum_Statistic = Cusum_Statistic + dat[i] - k[i]
    }

  # CUSUM cannot be below zero
    Cusum_Statistic = max(0, Cusum_Statistic)
  
    cusum[i,3] = Cusum_Statistic
  }
  # We only round the finished values, to avoid shifting the CUSUM or the thresholds off the 'grid' of values while the loop is running
  cusum$Cusum_Statistic <- round(cusum$Cusum_Statistic, digits = 6)
  cusum$h_L1 <- round(cusum$h_L1, digits = 6)
  cusum$h_L2 <- round(cusum$h_L2, digits = 6)
  return(cusum)
}

Write_to_Lake_Parquet <- function(Table, file) {
local_parquet_path <- tempfile(fileext = "temp.parquet")  # Write the data frame to a Parquet file locally
write_parquet(Table, local_parquet_path)
storage_upload(cont, local_parquet_path, dest = file)
}
