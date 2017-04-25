##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to process MAIAC daily data into composites using the functions from maiac_processing_functions.R
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-02-09
##################################################
## General processing workflow:
## 1) get filenames from the available products and parameters files for the iteration
## 2) (parallel computing) convert files from hdf to geotif using gdal_translate
## 3) load .tif files needed for BRF normalization
## 4) filter bad or fill values from the data
## 5) (parallel computing) apply brf normalization to nadir for each date using the respective RTLS parameters or nearest RTLS file, and the eq. from MAIAC documentation: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
## 6) (optional) load QA layers, create a mask for each date excluding bad pixels (possibly cloud, adjacent cloud, cloud shadows, etc.) and apply the mask
## 7) (optional) create and apply mask based on extreme sun angles >80 deg.
## 8) calculate the median of each pixel using the remaining (best) pixels, and return a brick with 9 rasters (1-8 band, and no_samples)
## 9) plot a preview image of the composite and save it on the disk
## 10) save the processed composite
##
## IMPORTANT: It is also needed to have GDAL installed on the computer. One possible repository is https://trac.osgeo.org/osgeo4w/
## At this momment the script tries to process all MAIAC time series and there is no way to specify a begin or end date.
##
## Notes:
## sds_name = c("sur_refl", "Sigma_BRFn", "Snow_Fraction", "Snow_Grain_Diameter", "Snow_Fit", "Status_QA", "sur_refl_500m", "cosSZA", "cosVZA", "RelAZ", "Scattering_Angle", "Glint_Angle", "SAZ", "VAZ", "Fv", "Fg")
## sds_data_type = c("INT16", "INT16", "INT16", "INT16", "INT16", "UINT16","INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "FLOAT32", "FLOAT32")
## sds_parameters_name = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
## http://www.ctahr.hawaii.edu/grem/mod13ug/sect0005.html, http://glcf.umd.edu/data/modis/
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

# pre-compile code to try to speed things up - not sure if it works
library(compiler)
enableJIT(3)


# CONFIG ------------------------------------------------------------------

# functions filename with full directory
functions_fname = "D:/2_Projects/1_Author/4_MAIAC_process/maiac_processing/maiac_processing_functions.R"

# log file path, this file will contain the text output from each core running, useful for debugging
log_fname = "D:/_MAIAC/MAIAC_ProcessedTiles/log.txt"

# output directory, the one to export the processed composites
output_dir = "D:/_MAIAC/MAIAC_ProcessedTiles/"

# latlon tiles directory, the one where latlon tiles are stored - this files are used to create nan tiles
latlon_tiles_dir = "D:/_MAIAC/MAIAC_LatLonTiles/"

# nan tiles directory, the one where nan tiles are stored, to use in case of non-existant RTLS file for processing
nan_tiles_dir = "D:/_MAIAC/MAIAC_NanTiles/"

# preview directory, the one to to export preview ".png" images
tile_preview_dir = "D:/_MAIAC/MAIAC_PreviewTiles/"

# raw files input directory, can set up more than one directory
input_dir_vec = c("D:/h00v01/", "E:/h01v04/", "E:/h01v05/", "E:/h01v06/", "E:/h02v00/", "E:/h02v01/", "E:/h02v02/")

# tile(s) to process, can set up more than one directory, put in the same order of input_dir_vec
tile_vec = c("h00v01", "h01v04", "h01v05", "h01v06", "h02v00", "h02v01", "h02v02")

# url to download maiac files for south america in case of corrupted .hdf or missing RTLS file
maiac_ftp_url = "ftp://maiac@dataportal.nccs.nasa.gov/DataRelease/SouthAmerica/"

# product name MAIACTBRF, MAIACABRF, MAIACRTLS, don't change this
product = c("MAIACTBRF","MAIACABRF")
parameters = "MAIACRTLS"

# enables filtering by Quality Assessment bits (adjacent clouds and stuff), may reduce number of available pixels
is_qa_filter = FALSE

# enables filtering by Extreme Angles > 80, may reduce number of available pixels
is_ea_filter = FALSE

# number of days on each composite, should be 8, 16 or 32
composite_no = 16

# number of cores to use while processing, should use total number of cores minus one, to try to prevent the computer from freezing
no_cores = 7



# SEQUENCE FOR SEPARATE TILE PROCESSING -------------------------------------------------------------------

# load functions
source(functions_fname)

# define the output base filename
composite_fname = CreateCompositeName(composite_no, product, is_qa_filter, is_ea_filter)

# create matrix of days on each composite
day_mat = CreateDayMatrix(composite_no)

# create loop matrix containing all the information to iterate
loop_mat = CreateLoopMat(day_mat, composite_no, input_dir_vec, tile_vec)

# create preview directory if it doesnt exist
dir.create(file.path(tile_preview_dir), showWarnings = FALSE)

# create nan tiles directory if it doesnt exist
dir.create(file.path(nan_tiles_dir), showWarnings = FALSE)

# Loop through the loop_mat matrix 
foreach(j = 1:dim(loop_mat)[1], .packages=c("raster","gdalUtils","rgdal","RCurl"), .export=ls(.GlobalEnv), .errorhandling="remove") %do% {
  #j=144
  
  # get input_dir from loop_mat
  input_dir = as.character(loop_mat[j,3])
  
  # get tile from loop_mat
  tile = as.character(loop_mat[j,4])
  
  # get year from loop_mat
  year = as.numeric(loop_mat[j,2])
  
  # get the day vector from loop_mat
  day = day_mat[as.numeric(loop_mat[j,1]),]
  
  # create nan rasters for tile in case it is needed
  CreateNanTiles(tile, nan_tiles_dir, latlon_tiles_dir)
  
  # check if composite processed file already exist, otherwise just skip to next iteration
  if (IsTileCompositeProcessed(composite_fname, tile, year, day, output_dir))
    return(0)
  
  # if no brf or rtls is available for given day, year, tile, (1) try to download it (in case of rtls), or (2) return nan output, log the information and go to next iteration
  if (!IsDataAvailable(product, tile, year, day, nan_tiles_dir, output_dir, obs="brf", maiac_ftp_url) | !IsDataAvailable(parameters, tile, year, day, nan_tiles_dir, output_dir, obs="rtls", maiac_ftp_url))
    return(0)
  
  # set temporary directory
  tmp_dir = paste0("tmp",year,day[length(day)],"/")
  
  # create temporary directory
  dir.create(file.path(output_dir, tmp_dir), showWarnings = FALSE)
  
  # 1) get filenames from the available products and parameters files for the iteration
  product_fname = GetFilenameVec(product, input_dir, tile, year, day)
  parameter_fname = GetFilenameVec(parameters, input_dir, tile, year, day)
  
  # filter product names by only the product tiles/dates that have RTLS tiles
  # TODO: remove this function? this function seems to be obsolete now that we process by tile
  #product_fname = FilterProductTilesbyRTLSTiles(product_fname, parameter_fname, output_dir)
  
  # 2) (parallel computing) convert files from hdf to geotif using gdal_translate
  ConvertHDF2TIF(product_fname, input_dir, output_dir, tmp_dir, maiac_ftp_url, no_cores, log_fname)
  ConvertHDF2TIF(parameter_fname, input_dir, output_dir, tmp_dir, maiac_ftp_url, no_cores, log_fname)
  
  # remove directory from filenames, return only the "filenames".hdf
  product_fname = basename(product_fname)
  parameter_fname = basename(parameter_fname)
  
  # 3) load .tif files needed for BRF normalization
  brf_reflectance = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "sur_refl")
  brf_fv = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Fv")
  brf_fg = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Fg")
  rtls_kiso = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kiso")
  rtls_kvol = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kvol")
  rtls_kgeo = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kgeo")
  
  # 4) filter bad or fill values from the data
  # for Fv and Fg, the -99999 is a fill value and should be removed
  brf_fv = FilterBadValues(brf_fv, equal=-99999)
  brf_fg = FilterBadValues(brf_fg, equal=-99999)
  #brf_reflectance = FilterBadValues(brf_reflectance, min=0, max=1) # it seems that if you filter the final result from 0 to 1, this step is not needed
  
  # 5) (parallel computing) apply brf normalization to nadir for each date using the respective RTLS parameters or nearest RTLS file, and the eq. from MAIAC documentation: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
  nadir_brf_reflectance = ConvertBRFNadir(brf_reflectance, brf_fv, brf_fg, rtls_kiso, rtls_kvol, rtls_kgeo, tile, year, output_dir, no_cores, log_fname)
  rm(list = c("brf_reflectance", "brf_fv", "brf_fg", "rtls_kiso", "rtls_kvol", "rtls_kgeo"))
  
  # 6) (optional) load QA layers, create a mask for each date excluding bad pixels (possibly cloud, adjacent cloud, cloud shadows, etc.) and apply the mask
  # remove pixels such as: possibly cloud, cloud adjacent, cloud shadow, etc.
  if (is_qa_filter) {
    qa_brick = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Status_QA")
    qa_mask = CreateQAMask(qa_brick)
    nadir_brf_reflectance = ApplyMaskOnBrick(nadir_brf_reflectance, qa_mask)
    rm(list = c("qa_brick","qa_mask"))
  }
  
  # 7) (optional) create and apply mask based on extreme sun angles >80 deg.
  if (is_ea_filter) {
    nadir_brf_reflectance = FilterEA(nadir_brf_reflectance, product_fname, output_dir, tmp_dir)
  }
  
  # put the bands together so its easier to calc the median
  nadir_brf_reflectance_per_band = ReorderBrickPerBand(nadir_brf_reflectance)
  rm(list = c("nadir_brf_reflectance"))
  
  # 8) calculate the median of each pixel using the remaining (best) pixels, and return a brick with 9 rasters (1-8 band, and no_samples)
  # Couldn't implement multi-thread by clusterR (raster package) or doParallel, probably due to memory issues... single processing takes ~4min, which is not much
  median_brf_reflectance = CalcMedianBRF(nadir_brf_reflectance_per_band)
  rm(list = c("nadir_brf_reflectance_per_band"))
  
  # 9) plot a preview image of the composite and save it on the disk
  png(filename=paste0(tile_preview_dir,"fig_",composite_fname,"_",tile,"_",year,day[length(day)],".png"), type="cairo", units="cm", width=15, height=15, pointsize=10, res=300)
  par(oma=c(4,4,4,4))
  plot(median_brf_reflectance)
  dev.off()
  
  # 10) save the processed composite
  SaveProcessedTileComposite(median_brf_reflectance, output_dir, composite_fname, tile, year, day)
  rm(list = c("median_brf_reflectance"))
  
  # delete temporary directory
  unlink(file.path(output_dir, tmp_dir), recursive=TRUE)
  
  # End Loop Composite
  return(0)
}

# make sure to stop cluster
stopCluster(cl)
closeAllConnections()

# message
print(paste0(Sys.time(), ": Processing is now stopped. Either finished or some problem happened."))


# list memory
#sort( sapply(ls(),function(x){object.size(get(x))})) 