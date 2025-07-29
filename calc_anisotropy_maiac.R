# Script: Calculate MAIAC anisotropy bands ------------------------------------------------------------
# Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
# Date Created: 2021-03-08
# R version 4.0.2 (2020-06-22)
#

# clean environment
rm(list = ls()); gc()

# libraries
library(raster)
library(rgdal)
library(rgeos)
library(gdalUtils)
library(sp)

# define if EC2 or WS
machine = "EC2"


# library -----------------------------------------------------------------

# get the folder where the script R functions and config file are placed
functions_dir = paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")

# load config.txt file that should be in the same directory of the scripts
machine = "EC2"
source(paste0(functions_dir, "config_mosaic_vi.txt"))

# load functions
source(paste0(functions_dir, "maiac_processing_functions.R"))

# number of cores
no_cores = 16


# config ------------------------------------------------------------------

# directory where the files are located
input_back = "s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/backscat/"
input_forward = "s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/forwardscat/"

# output directory
output_dir = "s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/anisotropy/"
dir.create(output_dir, showWarnings = F)

# list of bands
band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","evi","ndvi","gcc")

# list back files
list_back = s3_list_bucket(input_back)

# list forward files
list_forward = s3_list_bucket(input_forward)

# get dates
unique_dates = unique(substr(basename(list_back), 27, 27+6))

# create list of items to run
expanded_grid = expand.grid(unique_dates, band_names)
run_df = paste0(expanded_grid$Var1, "_", expanded_grid$Var2)
length(run_df)
head(run_df)
tail(run_df)

# test
if (FALSE) {
  no_cores = 16
  run_df = run_df[1:64]
  
  no_cores = 4
  run_df = run_df[1:8]
}

# code ---------------------------------------------------------------------

# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# run in parallel
i=1
#for (i in 1:length(band_names)) {
f=foreach(i = 1:length(run_df), .packages=c("raster","gdalUtils","rgdal"), .errorhandling="remove") %dopar% {
  
  if (TRUE) {
    
    # create output filename
    output_fname_s3 = paste0(output_dir, "maiac_month_1000_anisotropy_",run_df[i],"_latlon.tif")
    
    # skip if output exists
    if (S3_file_exists(output_fname_s3)) {
      print(paste("Output already exists in s3:", i, "-", output_fname_s3))
      next
    }
    
    # get input filenames
    list_back_band = grep(run_df[i], list_back, value=T)
    list_forward_band = grep(run_df[i], list_forward, value=T)
    
    # got data?
    if (length(list_back_band) != 1 | length(list_forward_band) != 1) {
      print(paste("Missing data for", i))
      next
    }
    
    # # calc using raster
    # r_back = raster(s3_to_vsis(list_back_band))
    # r_forward = raster(s3_to_vsis(list_forward_band))
    # r_anisotropy = r_back - r_forward
    # tmp_fname = paste0(tempfile(), ".tif")
    # writeRaster(r_anisotropy, tmp_fname, overwrite=T, datatype = "INT2S", options = c("COMPRESS=LZW","PREDICTOR=2"))
    
    # calc using gdal_calc
    tmp_fname = paste0(tempfile(), ".tif")
    gdal_calc_run = paste("gdal_calc.py",
                          paste0("--calc \" A - B \" "), 
                          "-A", s3_to_vsis(list_back_band),
                          "-B", s3_to_vsis(list_forward_band),
                          "--format GTiff",
                          "--type Int16",
                          "--overwrite",
                          "--co COMPRESS=DEFLATE",
                          "--quiet",
                          "--outfile", tmp_fname)
    system(gdal_calc_run)
    # file.info(tmp_fname)$size/(1024*1024)
    
    # copy file to s3
    S3_copy_single(tmp_fname, output_fname_s3, "ctrees")
    
    # print message
    #print(paste(i, length(band_names), b_name))
    #rm(r_back, r_forward, r_anisotropy, b_name, f_name, tmp_fname)
    print(i)
    rm(list_back_band, list_forward_band, tmp_fname, output_fname_s3)
    
  }
  
}
# 24.5 sec in c5.4xlarge

# finish cluster
stopCluster(cl)

