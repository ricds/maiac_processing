##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to process MAIAC daily data into composites using the functions from maiac_processing_functions.R
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-02-09
##################################################
## The main steps of the processing workflow:
## 1) get filenames from the available brf and rtls data
## 2) convert files from hdf to tif using gdal_translate in parallel
## 3) load .tif files needed for BRF normalization
## 4) apply brf normalization in parallel for each date using the respective RTLS parameters or nearest RTLS file, and the equation from MAIAC documentation: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
## 5) (optional) load QA layers, create a mask for each date excluding bad pixels (possibly cloud, adjacent cloud, cloud shadows, etc.) and apply the mask
## 6) (optional) create and apply mask based on extreme sun angles >80 deg.
## 7) calculate the median of each pixel using the available data on each pixel in parallel for each band and return a brick with 9 rasters (1-8 band, and no_samples of band 1)
## 8) plot a preview image of the composite and save it on the disk
## 9) save the processed composite - one .tif file per band and no_samples of band1
##
## IMPORTANT - READ THIS PRIOR TO RUNNING:
## 1) GDAL must be installed on the computer prior to processing. It can be found in websites like this: https://trac.osgeo.org/osgeo4w/
## 2) A config file named "config.txt" must exist in the script folder describing the tiles to process and directories. A example file is provided.
## 3) At this momment the script tries to process all MAIAC time series and there is no way to specify a start or end date. However it is possible to run specific composite/year/tile using the manual_run variable in the "config.txt"
## 4) It is necessary to install the median2rcpp library "by hand"
##
## Notes to self:
## sds_name = c("sur_refl", "Sigma_BRFn", "Snow_Fraction", "Snow_Grain_Diameter", "Snow_Fit", "Status_QA", "sur_refl_500m", "cosSZA", "cosVZA", "RelAZ", "Scattering_Angle", "Glint_Angle", "SAZ", "VAZ", "Fv", "Fg")
## sds_data_type = c("INT16", "INT16", "INT16", "INT16", "INT16", "UINT16","INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "FLOAT32", "FLOAT32")
## sds_parameters_name = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
## http://www.ctahr.hawaii.edu/grem/mod13ug/sect0005.html, http://glcf.umd.edu/data/modis/
## to list memory: sort( sapply(ls(),function(x){object.size(get(x))})) 
##################################################
## Some Websites:
## https://lpdaac.usgs.gov/products/mcd19a1v006/
## https://ladsweb.modaps.eosdis.nasa.gov/filespec/MODIS/6/MCD19A1
## https://ladsweb.modaps.eosdis.nasa.gov/missions-and-measurements/products/maiac/MCD19A1/
##################################################

# clean environment
rm(list = ls())

# load required libraries
library(raster)  #install.packages("raster")
library(gdalUtils)  #install.packages("gdalUtils")
library(rgdal)  #install.packages("rgdal")
library(foreach)  #install.packages("foreach") # click yes if asked
library(RCurl)  #install.packages("RCurl")
library(doParallel)  #install.packages("doParallel")
library(rstudioapi)  #install.packages("rstudioapi")
library(compiler)  #install.packages("compiler")
library(Rcpp)  #install.packages("Rcpp")
library(itertools)  #install.packages("itertools")
library(plyr)  #install.packages("plyr")
library(median2rcpp)  # download.file("https://www.dropbox.com/s/chneec889dl0nck/median2rcpp_0.1.0.tar.gz?raw=1", destfile = "median2rcpp_0.1.0.tar.gz", mode="wb"); system({'R CMD INSTALL "median2rcpp_0.1.0.tar.gz"'})

# pre-compile code to try to speed things up - not sure if it works - must run the command twice don't know why
enableJIT(3)
enableJIT(3)

# remove temporary files older than 2 hours
removeTmpFiles(h=2)


# LOAD FUNCTIONS AND INITIALIZE VARIABLES ---------------------------------

# get the folder where the script R functions and config file are placed
functions_dir = paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")

# load config.txt file that should be in the same directory of the scripts
source(paste0(functions_dir, "config.txt"))

# load functions
source(paste0(functions_dir, "maiac_processing_functions.R"))

# test if input_dir_vec is empty
if (length(list.files(input_dir_vec[1]))==0) {
  # message
  print(paste0(Sys.time(), ": ERROR Input directory is Empty."))
  stop(paste0(Sys.time(), ": ERROR Input directory is Empty."))
}

# set gdal directory
gdal_setInstallation(gdal_dir)

# create process directory if it doesnt exist
dir.create(file.path(process_dir), showWarnings = FALSE, recursive=T)

# output directory, the one to export the processed composites
output_dir = paste0(process_dir, "MAIAC_ProcessedTiles/")

# directory to place downloads in case of corrupted or missing files
downloaded_files_dir = paste0(process_dir, "MAIAC_DownloadedFiles/")

# log file path, this file will contain the text output from each core running, useful for debugging
log_fname = paste0(output_dir, "log.txt")

# nan tiles directory, the one where nan tiles are stored, to use in case of non-existant RTLS file for processing
nan_tiles_dir = paste0(process_dir, "MAIAC_NanTiles/")

# preview directory, the one to to export preview ".png" images
tile_preview_dir = paste0(process_dir, "MAIAC_PreviewTiles/")

# create preview directory if it doesnt exist
dir.create(file.path(tile_preview_dir), showWarnings = FALSE, recursive=T)

# create nan tiles directory if it doesnt exist
dir.create(file.path(nan_tiles_dir), showWarnings = FALSE, recursive=T)

# create processed composites/tiles directory if it doesnt exist
dir.create(file.path(output_dir), showWarnings = FALSE, recursive=T)

# create downloaded files directory if it doesnt exist
dir.create(file.path(downloaded_files_dir), showWarnings = FALSE, recursive=T)

# product name MAIACTBRF, MAIACABRF, MAIACRTLS, don't change this
product = c("MAIACTBRF","MAIACABRF","MCD19A1")
parameters = c("MAIACRTLS","MCD19A3")

# url to download maiac files for south america in case of corrupted .hdf or missing RTLS file
maiac_ftp_url = "ftp://maiac@dataportal.nccs.nasa.gov/DataRelease/SouthAmerica/"

# define the output base filename
composite_fname = CreateCompositeName(composite_no, product, is_qa_filter, is_ea_filter, view_geometry, product_res = product_res)

# create matrix of days on each composite
day_mat = CreateDayMatrix(composite_no)

# create loop matrix containing all the information to iterate
loop_mat = CreateLoopMat(day_mat, composite_no, input_dir_vec, tile_vec, manual_run)



# START OF PROCESSING ---------------------------------

# Loop through the loop_mat matrix 
f=foreach(j = 1:dim(loop_mat)[1], .packages=c("raster","gdalUtils","rgdal","RCurl"), .export=ls(.GlobalEnv), .errorhandling="remove") %do% {
  
  # message
  print("...")
  print(paste0(Sys.time(), ": Start processing a new composite... j=", j, " from ", dim(loop_mat)[1]))
  
  # measure time
  t1 = mytic()
  
  # get input_dir from loop_mat
  input_dir = as.character(loop_mat[j,3])
  
  # get tile from loop_mat
  tile = as.character(loop_mat[j,4])
  
  # get year from loop_mat
  year = as.numeric(loop_mat[j,2])
  
  # get the day vector from loop_mat, if its monthly get the dates according to the year, otherwise just get the values from day_mat
  if (composite_no == "month") {
    days_of_months = matrix(diff(seq(as.Date("2000-01-01"), as.Date("2021-01-01"), by = "month")), ncol = 12, byrow = T)
    day = format(seq(from = as.Date(paste0(year,"-", as.numeric(loop_mat[j,1]), "-01")), to = as.Date(paste0(year,"-", as.numeric(loop_mat[j,1]), "-", days_of_months[which(2000:2020==year),as.numeric(loop_mat[j,1])])), by = 1), "%j")
  } else {
    day = day_mat[as.numeric(loop_mat[j,1]),]
  }
  
  # test if this iteration is old MAIAC files or new MCD19A1 files
  isMCD = IsMCD(input_dir, day, year, tile)
  if (isMCD) {
    product = "MCD19A1"
    parameters = "MCD19A3"
  } else {
    product = c("MAIACTBRF","MAIACABRF")
    parameters = "MAIACRTLS"
  }
  
  # check if composite processed file already exist, otherwise just skip to next iteration; manual_run overrides this and overwrite files
  if (IsTileCompositeProcessed(composite_fname, tile, year, day, output_dir, overwrite_files))
    return(0)
  
  # if no brf or rtls is available for given day, year, tile, (1) try to download it (in case of rtls), or (2) return nan output, log the information and go to next iteration
  if (!IsDataAvailable(product, tile, year, day, nan_tiles_dir, output_dir, obs="brf", maiac_ftp_url, composite_fname, downloaded_files_dir, composite_no, isMCD, product_res) | !IsDataAvailable(parameters, tile, year, day, nan_tiles_dir, output_dir, obs="rtls", maiac_ftp_url, composite_fname, downloaded_files_dir, composite_no, isMCD, product_res))
    return(0)

  # set temporary directory
  tmp_dir = paste0(tempdir(), "\\tmp_",tile,"_",year,day[length(day)],"/")
  
  # create temporary directory
  dir.create(file.path(tmp_dir), showWarnings = FALSE)
  
  # 1) get filenames from the available products and parameters files for the iteration
  product_fname = GetFilenameVec(product, input_dir, downloaded_files_dir, tile, year, day, offset_days=0)
  parameter_fname = GetFilenameVec(parameters, input_dir, downloaded_files_dir, tile, year, day, offset_days=24)

  # 2) (parallel computing) convert files from hdf to geotif using gdal_translate
  ConvertHDF2TIF(product_fname, parameter_fname, input_dir, output_dir, tmp_dir, maiac_ftp_url, no_cores, log_fname, is_ea_filter, is_qa_filter, downloaded_files_dir, download_enabled, process_dir, isMCD, product_res)
  
  # remove directory from filenames, return only the "filenames".hdf
  product_fname = basename(product_fname)
  parameter_fname = basename(parameter_fname)
  
  # 3) load .tif files needed for BRF normalization
  brf_reflectance = LoadMAIACFiles(product_fname, output_dir, tmp_dir, product_res, isMCD)
  brf_fv = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Fv", isMCD)
  brf_fg = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Fg", isMCD)
  rtls_kiso = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kiso", isMCD)
  rtls_kvol = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kvol", isMCD)
  rtls_kgeo = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kgeo", isMCD)
  
  # 4) (parallel computing) apply brf normalization to nadir for each date using the respective RTLS parameters or nearest RTLS file, and the eq. from MAIAC documentation: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
  nadir_brf_reflectance = ConvertBRFNadir(brf_reflectance, brf_fv, brf_fg, rtls_kiso, rtls_kvol, rtls_kgeo, tile, year, output_dir, no_cores, log_fname, view_geometry, isMCD, product_res)
  rm(list = c("brf_reflectance", "brf_fv", "brf_fg", "rtls_kiso", "rtls_kvol", "rtls_kgeo"))
  
  # test if nadir_brf_reflectance is empty, and return nan tile if it is true
  if (length(nadir_brf_reflectance) == 0) {
    nan_tile = GetNanTile(tile, nan_tiles_dir, isMCD, product_res)
    SaveProcessedTileComposite(nan_tile, output_dir, composite_fname, tile, year, day, composite_no)
    return(0)
  }
  
  # 5 (optional) load QA layers, create a mask for each date excluding bad pixels (possibly cloud, adjacent cloud, cloud shadows, etc.) and apply the mask
  # remove pixels such as: possibly cloud, cloud adjacent, cloud shadow, etc.
  if (is_qa_filter) {
    qa_brick = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Status_QA")
    qa_mask = CreateQAMask(qa_brick)
    nadir_brf_reflectance = ApplyMaskOnBrick(nadir_brf_reflectance, qa_mask)
    rm(list = c("qa_brick","qa_mask"))
  }
  
  # 6) (optional) create and apply mask based on extreme sun angles >80 deg.
  if (is_ea_filter) {
    nadir_brf_reflectance = FilterEA(nadir_brf_reflectance, product_fname, output_dir, tmp_dir)
  }
  
  # put the bands together so its easier to calc the median
  nadir_brf_reflectance_per_band = ReorderBrickPerBand(nadir_brf_reflectance, output_dir, tmp_dir)
  rm(list = c("nadir_brf_reflectance"))
  
  # 7) calculate the median of each pixel using the remaining (best) pixels, and return a brick with 9 rasters (1-8 band, and no_samples of band1)
  median_brf_reflectance = CalcMedianBRF(nadir_brf_reflectance_per_band, no_cores, log_fname, output_dir, tmp_dir)
  rm(list = c("nadir_brf_reflectance_per_band"))
  
  # 8) plot a preview image of the composite and save it on the disk
  # define the composite number or name
  if (composite_no == "month") {
    composite_num = paste0("_",format(as.Date(paste0(day[length(day)],"-", year), "%j-%Y"), "%m"))
  } else {
    composite_num = day[length(day)]
  }
  png(filename=paste0(tile_preview_dir,"fig_",composite_fname,"_",tile,"_",year,composite_num,".png"), type="cairo", units="cm", width=15, height=15, pointsize=10, res=300)
  par(oma=c(4,4,4,4))
  plot(stack(median_brf_reflectance[[1:(nlayers(median_brf_reflectance)-1)]]/10000, median_brf_reflectance[[nlayers(median_brf_reflectance)]]))
  dev.off()
  
  # 9) save the processed composite
  SaveProcessedTileComposite(median_brf_reflectance, output_dir, composite_fname, tile, year, day, composite_no, product_res)
  rm(list = c("median_brf_reflectance"))
  
  # delete temporary directory
  unlink(file.path(tmp_dir), recursive=TRUE)
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Last composite processed finished in ", t2))
  
  # do some garbage collection, may enhance memory
  gc()
  
  # End Loop Composite
  return(0)
}

# message
print(paste0(Sys.time(), ": Processing is now stopped. Either finished or some problem happened. Good luck!"))

# make sure to stop cluster
stopCluster(cl)
closeAllConnections()
