##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: calculate vegetation indices NDVI and EVI using the maiac composites
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-06-27
## Notes: get files from "mosaic_output_dir" from "config.txt", calc VI and save them in the same directory
##################################################

# clean environment
rm(list = ls())

# load required libraries
library(raster)  #install.packages("raster")
library(foreach)  #install.packages("foreach") # click yes if asked
library(doParallel)  #install.packages("doParallel")
library(rstudioapi)  #install.packages("rstudioapi")
library(compiler)  #install.packages("compiler")
library(itertools)  #install.packages("itertools")

# get the folder where the script R functions and config file are placed
functions_dir = paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")

# load config.txt file that should be in the same directory of the scripts
source(paste0(functions_dir, "config.txt"))

# read composite vector from the functions folder
composite_vec = read.csv(paste0(functions_dir,"composite_vec_",composite_no,".csv"), header=F)

# list files
list_crop = list.files(mosaic_output_dir, pattern="crop", full.names = TRUE)
list_crop_band1 = grep(list_crop, pattern="band1", value = TRUE)
list_crop_band2 = grep(list_crop, pattern="band2", value = TRUE)
list_crop_band3 = grep(list_crop, pattern="band3", value = TRUE)

# function to calc evi
f_evi = function(NIR, RED, BLUE) {
  C1 = 6
  C2 = 7.5
  L = 1
  G = 2.5
  EVI = G*{NIR-RED}/{NIR+{C1*RED}-{C2*BLUE}+L}
  EVI*10000
}
ff_evi = cmpfun(f_evi)

# function to calc ndvi
f_ndvi = function(NIR, RED) {
  NDVI = {NIR-RED}/{NIR+RED}
  NDVI*10000
}
ff_ndvi = cmpfun(f_ndvi)

# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# process in parallel
f=foreach(i = 1:dim(composite_vec)[1], .packages=c("raster"), .errorhandling="remove") %dopar% {
  # check if files exist for given composite
  file_list = list.files(mosaic_output_dir, pattern=as.character(composite_vec[i,]), full.names=TRUE)
  
  # if there are files with the given composite name
  if (length(file_list)!=0) {
    
    # get data
    if (!file.exists(paste0(dirname(list_crop_band1[i]), "/", mosaic_base_filename, "_ndvi_", composite_vec[i,], ".tif")) || !file.exists(paste0(dirname(list_crop_band1[i]), "/", mosaic_base_filename, "_evi_", composite_vec[i,], ".tif"))) {
      maiac_band1 = raster(list_crop_band1[i])/10000
      maiac_band2 = raster(list_crop_band2[i])/10000
      maiac_band3 = raster(list_crop_band3[i])/10000
    }
    
    # apply function
    if (!file.exists(paste0(dirname(list_crop_band1[i]), "/", mosaic_base_filename, "_ndvi_", composite_vec[i,], ".tif"))) {
      writeRaster(x=overlay(maiac_band2, maiac_band1, fun=ff_ndvi),
                  filename = paste0(dirname(list_crop_band1[i]), "/", mosaic_base_filename, "_ndvi_", composite_vec[i,], ".tif"),
                  overwrite = TRUE, format = "GTiff",
                  datatype = "INT2S",
                  options = c("COMPRESS=LZW","PREDICTOR=2"))
    }
    
    if (!file.exists(paste0(dirname(list_crop_band1[i]), "/", mosaic_base_filename, "_evi_", composite_vec[i,], ".tif"))) {
      writeRaster(x=overlay(maiac_band2, maiac_band1, maiac_band3, fun=ff_evi),
                  filename = paste0(dirname(list_crop_band1[i]), "/", mosaic_base_filename, "_evi_", composite_vec[i,], ".tif"),
                  overwrite = TRUE, format = "GTiff",
                  datatype = "INT2S",
                  options = c("COMPRESS=LZW","PREDICTOR=2"))
    }
    
  }
  
  # clear
  rm("maiac_band2","maiac_band1","maiac_band3")
  gc()
  removeTmpFiles(h=0)
}

# finish cluster
stopCluster(cl)

# message
print("Processing finished.")

# check file size
sum(file.info(list.files(mosaic_output_dir, pattern="ndvi", full.names = TRUE))$size)/(1024*1024*1024)  # 3.756398
sum(file.info(list.files(mosaic_output_dir, pattern="evi", full.names = TRUE))$size)/(1024*1024*1024)  # 3.737279