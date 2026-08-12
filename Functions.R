#Cusum Poisson counts with reset to 0 occurring immediately, rather than in following month
Cusum_Poisson_Counts_Immediate <- function(Data) { #
  dat=Data$numerator 
  denominator=Data$denominator
  Ref_Rate = Data$Ref_Rate
  h_L1 = Data$h_L1
  h_L2 = Data$h_L2
  k = Data$k
  denrat = Data$denrat
  scaled_dat = round(dat*denrat)
  scaled_h_L1 = round(h_L1*denrat)
  scaled_h_L2 = round(h_L2*denrat)
  scaled_k = round(k*denrat)

# Create tibble with scaled versions of numerator, thresholds and k
# Scaled versions necessary to ensure we tweak the CUSUM correctly where we have a denrat change and the new CUSUM is equidistant between two possible values
  cusum <- tibble(period = Data$year_month, Dat = dat, denominator = denominator, h_L1 = h_L1, h_L2 = h_L2, k=k, Ref_Rate = Ref_Rate, denrat=denrat,
  scaled_dat = scaled_dat, scaled_h_L1 = scaled_h_L1, scaled_h_L2 = scaled_h_L2, scaled_k = scaled_k, scaled_cusum = 0)
  scaled_cusum = 0

  for(i in 1 : length(dat)){

  # Adjust previous CUSUM value where denrat changes to ensure it is a multiple of the new denrat
  # This converts scaled CUSUM to unscaled, then multiplies by the new denrat to make it scaled
  # We then round to the nearest integer, going upwards if halfway between two
    if(i > 1 && denrat[i] != denrat[i-1]) {
      scaled_cusum <- round_half_up((scaled_cusum * denrat[i]) / denrat[i-1])
    }    

  # Reset CUSUM when level 2 threshold is crossed so following month starts from 0
    if (round(scaled_cusum) >= round(scaled_h_L2[i])) {
      scaled_cusum = round(scaled_dat[i] - scaled_k[i])
    }
    
  # Standard CUSUM calculation if there is no reset
    else {
      scaled_cusum = round(scaled_cusum + scaled_dat[i] - scaled_k[i])
    }

  # CUSUM cannot be below zero
    scaled_cusum = max(0, scaled_cusum)
  # Write CUSUM value into tibble using column name
    cusum[i, "scaled_cusum"] <- scaled_cusum
  }
  return(cusum)
}

Write_to_Lake_Parquet <- function(Table, file) {
local_parquet_path <- tempfile(fileext = "temp.parquet")  # Write the data frame to a Parquet file locally
write_parquet(Table, local_parquet_path)
storage_upload(cont, local_parquet_path, dest = file)
}