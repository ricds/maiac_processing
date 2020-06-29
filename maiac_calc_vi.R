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
source(paste0(functions_dir, "config_mosaic_vi.txt"))

# load functions
source(paste0(functions_dir, "maiac_processing_functions.R"))

# read composite vector from the functions folder
composite_vec = data.frame(createCompDates(composite_no, end_year = end_year))

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

# Initiate cluster - Only use parallel if the mosaic is not huge like the whole south america
#cl = parallel::makeCluster(no_cores)
#registerDoParallel(cl)

# process
i=1
for (i in 1:dim(composite_vec)[1]) {
#f=foreach(i = 1:dim(composite_vec)[1], .packages=c("raster"), .errorhandling="remove") %dopar% { # for parallel
  # check if files exist for given composite
  file_list = list.files(mosaic_output_dir, pattern=as.character(composite_vec[i,]), full.names=TRUE)
  
  # if there are files with the given composite name
  if (length(file_list)!=0) {
    
    # check if vi exist
    if(length(grep(file_list, pattern="ndvi", value = T))==0 || length(grep(file_list, pattern="evi", value = T))==0) {
      
      # get bands
      maiac_band1 = raster(grep(file_list, pattern="band1", value=T))/10000
      maiac_band2 = raster(grep(file_list, pattern="band2", value=T))/10000
      maiac_band3 = raster(grep(file_list, pattern="band3", value=T))/10000
      
      # apply function
      if (length(grep(file_list, pattern="ndvi", value = T))==0) {
        writeRaster(x=overlay(maiac_band2, maiac_band1, fun=ff_ndvi),
                    filename = paste0(dirname(grep(file_list, pattern="band1", value=T)), "/", mosaic_base_filename, "_", composite_vec[i,], "_ndvi_latlon.tif"),
                    overwrite = TRUE, format = "GTiff",
                    datatype = "INT2S",
                    options = c("COMPRESS=LZW","PREDICTOR=2"))
      }
      
      if (length(grep(file_list, pattern="evi", value = T))==0) {
        writeRaster(x=overlay(maiac_band2, maiac_band1, maiac_band3, fun=ff_evi),
                    filename = paste0(dirname(grep(file_list, pattern="band1", value=T)), "/", mosaic_base_filename, "_", composite_vec[i,], "_evi_latlon.tif"),
                    overwrite = TRUE, format = "GTiff",
                    datatype = "INT2S",
                    options = c("COMPRESS=LZW","PREDICTOR=2"))
      }
      
      rm("maiac_band2","maiac_band1","maiac_band3")
    }
  }
  
  # progress monitor
  print(paste(i,dim(composite_vec)[1]))
  
  # clear
  gc()
  removeTmpFiles(h=0)
}

# finish cluster
#stopCluster(cl)

# message
print("Processing finished.")

# check file size
#sum(file.info(list.files(mosaic_output_dir, pattern="ndvi", full.names = TRUE))$size)/(1024*1024*1024)
#sum(file.info(list.files(mosaic_output_dir, pattern="evi", full.names = TRUE))$size)/(1024*1024*1024)