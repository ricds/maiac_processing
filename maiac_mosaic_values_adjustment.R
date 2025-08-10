# Script: Adjust values in the final mosaic ------------------------------------------------------------
# Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
# Date Created: 2025-08-10
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
input_nadir = "s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/nadir/"
input_anisotropy = "s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/anisotropy/"

# list files
list_nadir = s3_list_bucket(input_nadir)
list_anisotropy = s3_list_bucket(input_anisotropy)
#list_back = s3_list_bucket(input_back)
#list_forward = s3_list_bucket(input_forward)

# create list of items to run
#file_list = c(list_back, list_forward, list_nadir, list_anisotropy)
file_list = c(list_nadir, list_anisotropy)
file_list_output = gsub("mosaic", "mosaic_final", file_list)
length(file_list)


# code ---------------------------------------------------------------------

# test run
#test_run = 300:320
#test_run = 5000:5020

# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# run in parallel
i=300
#for (i in 1:length(band_names)) {
#f=foreach(i = test_run, .packages=c("raster","gdalUtils","rgdal"), .errorhandling="remove") %dopar% {
f=foreach(i = 1:length(file_list), .packages=c("raster","gdalUtils","rgdal"), .errorhandling="remove") %dopar% {
  
  if (TRUE) {
    
    # skip if output exists
    if (S3_file_exists(file_list_output[i])) {
      #print(paste("Output already exists in s3:", i, "-", file_list_output[i]))
      next
    }
    
    # check if this data is GCC or not
    idx = grep("gcc", file_list[i])
    IS_GCC = length(idx) == 1
    
    # check if this data is ANISOTROPY or not
    idx = grep("anisotropy", file_list[i])
    IS_ANISOTROPY = length(idx) == 1
    
    # check if this data is NO_SAMPLES or not
    idx = grep("no_samples", file_list[i])
    IS_NO_SAMPLES = length(idx) == 1
    
    # filter for non-anisotropy and non-nosamples
    if (!IS_ANISOTROPY & !IS_NO_SAMPLES) {
      # calc using gdal_calc
      tmp_fname = paste0(tempfile(), ".tif")
      gdal_calc_run = paste("gdal_calc.py",
                            paste0("--calc \" numpy.where(logical_or(A<=10, A>10000),-32767, A) \" "), 
                            "-A", s3_to_vsis(file_list[i]),
                            "--format GTiff",
                            "--type Int16",
                            "--overwrite",
                            "--co COMPRESS=DEFLATE",
                            "--quiet",
                            "--outfile", tmp_fname)
      system(gdal_calc_run)
      # file.info(tmp_fname)$size/(1024*1024)
    }
    
    # filter for non-anisotropy and non-nosamples
    if (!IS_ANISOTROPY & IS_NO_SAMPLES) {
      # calc using gdal_calc
      tmp_fname = paste0(tempfile(), ".tif")
      gdal_calc_run = paste("gdal_calc.py",
                            paste0("--calc \" numpy.where(A==0,-32767, A) \" "), 
                            "-A", s3_to_vsis(file_list[i]),
                            "--format GTiff",
                            "--type Int16",
                            "--overwrite",
                            "--co COMPRESS=DEFLATE",
                            "--quiet",
                            "--outfile", tmp_fname)
      system(gdal_calc_run)
      # file.info(tmp_fname)$size/(1024*1024)
    }
    
    # filter for GCC
    if (IS_ANISOTROPY) {
      # calc using gdal_calc
      tmp_fname = paste0(tempfile(), ".tif")
      gdal_calc_run = paste("gdal_calc.py",
                            paste0("--calc \" numpy.where(logical_or(A < -10000,A > 10000),-32767, numpy.where(logical_and(A >= -30, A <= 30),-32767, A)) \" "), 
                            "-A", s3_to_vsis(file_list[i]),
                            "--format GTiff",
                            "--type Int16",
                            "--overwrite",
                            "--co COMPRESS=DEFLATE",
                            "--quiet",
                            "--outfile", tmp_fname)
      system(gdal_calc_run)
      # file.info(tmp_fname)$size/(1024*1024)
    }
    
    # filter for GCC
    if (IS_GCC) {
      tmp_fname2 = paste0(tempfile(), ".tif")
      gdal_calc_run = paste("gdal_calc.py",
                            paste0("--calc \" numpy.where(A==3333,-32767, A) \" "), 
                            "-A", tmp_fname,
                            "--format GTiff",
                            "--type Int16",
                            "--overwrite",
                            "--co COMPRESS=DEFLATE",
                            "--quiet",
                            "--outfile", tmp_fname2)
      system(gdal_calc_run)
      # file.info(tmp_fname)$size/(1024*1024)
      
      # replace
      file.remove(tmp_fname)
      tmp_fname = tmp_fname2
    }
    
    # copy file to s3
    S3_copy_single(tmp_fname, file_list_output[i], "ctrees")
    
    # print message
    print(i)
    file.remove(tmp_fname)
    
  }
  
}
# 24.5 sec in c5.4xlarge

# finish cluster
stopCluster(cl)

