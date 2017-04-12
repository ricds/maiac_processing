##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to provide a sequence of processing using functions from maiac_processing_functions.R
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-02-09
## Notes: 
##################################################
## Current processing workflow:
## TODO: improve this workflow listing... it's not updated
## 1) get all filenames from a given 8-day period for all available tiles to south america
## 2) filter product names by only the product tiles/dates that have RTLS tiles
## 3) convert MAIACTBRF and MAIACRTLS hdfs to geotif (gdal_translate)
## 4) load all tif files needed
## 5) apply brf normalization to nadir for each date using the 8-day RTLS parameters file, and the eq. from MAIAC documentation: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
## 6) load QA layers and create a mask for each date excluding bad pixels (cloud, etc.)
## 7) apply mask to the brfn of each date
## 8) calc the median of each pixel using the remaining (best) values
## 9) (work in progress) mosaic the resulting median tiles (mosaic_rasters, also from gdal)
## 10) (work in progress) reproject the resulting 8-day composite (gdalwarp) and save as geotif
## 11) clean temporary files
## (repeat to all 8-day composites in the available time series)
## 12) (to do) merge each 8-day composites tif into one tif containing all time series, one big geotif for each band
## obs: the step 3 and 8 is where I'm working on at this momment, the rest is somewhat ready.
##
## Additional notes:
## sdsName = c("sur_refl", "Sigma_BRFn", "Snow_Fraction", "Snow_Grain_Diameter", "Snow_Fit", "Status_QA", "sur_refl_500m", "cosSZA", "cosVZA", "RelAZ", "Scattering_Angle", "Glint_Angle", "SAZ", "VAZ", "Fv", "Fg")
## sdsDataType = c("INT16", "INT16", "INT16", "INT16", "INT16", "UINT16","INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "FLOAT32", "FLOAT32")
## sdsParametersName = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
## http://www.ctahr.hawaii.edu/grem/mod13ug/sect0005.html, http://glcf.umd.edu/data/modis/
##################################################

# clean environment
rm(list = ls())

# libraries used on the script
library(raster)  #install.packages("raster")
library(gdalUtils)  #install.packages("gdalUtils")
library(rgdal)  #install.packages("rgdal")
library(foreach)  #install.packages("foreach") # click yes if asked
library(RCurl)  #install.packages("RCurl")
library(doParallel)  #install.packages("doParallel")


# CONFIG ------------------------------------------------------------------

# functions filename with full directory
functions_fname = "D:/2_Projects/1_Author/4_MAIAC_process/maiac_processing/maiac_processing_functions.R"

# output directory, the one to export the processed tiles
output_dir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/MAIAC_ProcessedTiles/"

# latlon tiles directory, the one where latlon tiles are stored
latlon_tiles_dir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/MAIAC_LatLonTiles/"

# nan tiles directory, the one where nan tiles are stored, to use in case of non-existant RTLS file for processing
nan_tiles_dir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/MAIAC_NanTiles/"

# preview directory, the one to to export preview images "png"
tile_preview_dir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/MAIAC_PreviewTiles/"

# input_dir, the one where the raw files are
input_dir = "D:/h00v01/"

# tile to process
# TODO: one per time is working, more than one not sure
tile = c("h00v01")

# url to download maiac files for south america, in case of needed
maiac_ftp_url = "ftp://maiac@dataportal.nccs.nasa.gov/DataRelease/SouthAmerica/"

# process date range
# TODO: apply this somehow
process_from_date = "04-08-2016"
process_to_date = "11-08-2016"

# product name MAIACTBRF, MAIACABRF, MAIACRTLS
product = c("MAIACTBRF","MAIACABRF")
parameters = "MAIACRTLS"

# enables filtering by Quality Assessment bits (adjacent clouds and stuff), may reduce number of available pixels
is_qa_filter = FALSE

# enables filtering by Extreme Angles > 80, may reduce number of available pixels
is_ea_filter = FALSE

# parameters
MEASURE_RUN_TIME_ENABLED = TRUE
PARALLEL_PROCESS_ENABLED = TRUE


# SEQUENCE FOR SEPARATE TILE PROCESSING -------------------------------------------------------------------

# load functions
source(functions_fname)

# define the output base filename
composite_fname = "SR_Nadir_8day"
if (length(product)==1) composite_fname = paste0(composite_fname,"_",product) else composite_fname = paste0(composite_fname,"_MAIACTerraAqua")
if (is_qa_filter & is_ea_filter) composite_fname = paste0(composite_fname,"_FilterQA-EA") else
  if (is_qa_filter) composite_fname = paste0(composite_fname,"_FilterQA") else
    if (is_ea_filter) composite_fname = paste0(composite_fname,"_FilterEA")

# create matrix of days on each 8-day composite
eight_day_mat = matrix(sprintf("%03d",1:360),ncol=8,byrow=T)

# create preview directory if it doesnt exist
dir.create(file.path(tile_preview_dir), showWarnings = FALSE)

# create nan rasters for every tile in case it is needed
CreateNanTiles(input_dir, latlon_tiles_dir, nan_tiles_dir, tile)

# measure time
if (MEASURE_RUN_TIME_ENABLED)
  start.time = Sys.time()

# Calculate the number of cores minus 1
if (PARALLEL_PROCESS_ENABLED) {
  # detect number of cores
  no_cores = detectCores() - 1
  
  # Initiate cluster
  cl = makeCluster(no_cores)
  registerDoParallel(cl)
  
  # send packages to clusters
  clusterEvalQ(cl, library(raster)) # pra passar packages pros workers
  clusterEvalQ(cl, library(gdalUtils)) # pra passar packages pros workers
  clusterEvalQ(cl, library(rgdal)) # pra passar packages pros workers
  clusterEvalQ(cl, library(RCurl)) # pra passar packages pros workers
}

# Loop year
for (k in 1:17) {
  #k=14
  year = 1999+k
  
  # send variables to clusters
  if (PARALLEL_PROCESS_ENABLED)
    clusterExport(cl, varlist=as.vector(c(lsf.str(), ls())))
  
  # Loop 8-day composite
  foreach(j = 1:dim(eight_day_mat)[1]) %dopar% {
  #for (j in 1:dim(eight_day_mat)[1]) {
    #j=28
    
    # get the day vector
    day = eight_day_mat[j,]
    
    # check if 8-day tile composite processed file already exist, otherwise just skip to next
    if (IsTileCompositeProcessed(composite_fname, tile, year, day, output_dir))
      return(0)
      #next
    
    # set temporary directory
    tmp_dir = paste0("tmp",year,day[8],"/")
    
    # create temporary directory
    dir.create(file.path(output_dir, tmp_dir), showWarnings = FALSE)
    
    # get filenames of each 8-day product and parameters files
    product_fname = GetFilenameVec(product, input_dir, tile, year, day)
    parameter_fname = GetFilenameVec(parameters, input_dir, tile, year, day)
    
    # if no brf or rtls for given day, year, tile, return nan output, log the information and go to next iteration
    if (IsDataAvailable(product_fname, tile, year, day, nan_tiles_dir, output_dir, obs="brf") | IsDataAvailable(parameter_fname, tile, year, day, nan_tiles_dir, output_dir, obs="rtls")) {
      unlink(file.path(output_dir, tmp_dir), recursive=TRUE)
      return(0)
    }
    
    # filter product names by only the product tiles/dates that have RTLS tiles
    # TODO: remove this function? this function seems to be obsolete now that we process by tile
    #product_fname = FilterProductTilesbyRTLSTiles(product_fname, parameter_fname, output_dir)
    
    # convert the files from hdf to tif, <NO PARALELL>
    # time no parallel: 57 min (366 files)
    # time parallel: estimado 60% do tempo 34 min (366 files), deu 33% -> 19 min (366 files)
    ConvertHDF2TIF(product_fname, input_dir, output_dir, tmp_dir, maiac_ftp_url)
    ConvertHDF2TIF(parameter_fname, input_dir, output_dir, tmp_dir, maiac_ftp_url)
    
    # remove directory from filenames, return only the "filenames".hdf
    #product_fname = RemoveDirectoryFromFilenameVec(product_fname)
    #parameter_fname = RemoveDirectoryFromFilenameVec(parameter_fname)
    product_fname = basename(product_fname)
    parameter_fname = basename(parameter_fname)
    
    # load files needed for BRF normalization
    brf_reflectance = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "sur_refl")
    brf_fv = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Fv")
    brf_fg = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Fg")
    rtls_kiso = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kiso")
    rtls_kvol = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kvol")
    rtls_kgeo = LoadMAIACFiles(parameter_fname, output_dir, tmp_dir, "Kgeo")
    
    # filter bad values or fill values
    # Fv and Fg: -99999 is a fill value, should be removed
    brf_fv = FilterBadValues(brf_fv, equal=-99999)
    brf_fg = FilterBadValues(brf_fg, equal=-99999)
    #brf_reflectance = FilterBadValues(brf_reflectance, min=0, max=1) # not needed i think, if you filter the final result it's ok
    
    # convert BRF to Nadir
    nadir_brf_reflectance = ConvertBRFNadir(brf_reflectance, brf_fv, brf_fg, rtls_kiso, rtls_kvol, rtls_kgeo)
    rm(list = c("brf_reflectance", "brf_fv", "brf_fg", "rtls_kiso", "rtls_kvol", "rtls_kgeo"))
    
    # create and apply QA filter mask following a set of rules in function filterQuality()
    # remove pixels such as: possibly cloud, cloud adjacent, cloud shadow, etc.
    if (is_qa_filter) {
      qa_brick = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "Status_QA")
      qa_mask = CreateQAMask(qa_brick)
      nadir_brf_reflectance = ApplyMaskOnBrick(nadir_brf_reflectance, qa_mask)
      rm(list = c("qa_brick","qa_mask"))
    }
    
    # create and apply a mask based on extreme sun angles >80
    if (is_ea_filter) {
      nadir_brf_reflectance = FilterEA(nadir_brf_reflectance, product_fname, output_dir, tmp_dir)
    }
    
    # put the bands together so its easier to calc the median
    nadir_brf_reflectance_per_band = ReorderBrickPerBand(nadir_brf_reflectance)
    rm(list = c("nadir_brf_reflectance"))
    
    # create median value BRF 8-day composite from the BRF brick and masked QA brick, the output is 9 rasters (1-8 band, and no_samples)
    median_brf_reflectance = CalcMedianBRF8day(nadir_brf_reflectance_per_band)
    rm(list = c("nadir_brf_reflectance_per_band"))
    
    # plot brfnadir to file to verify it later
    png(filename=paste0(tile_preview_dir,"fig_",composite_fname,"_",year,day[8],".png"), type="cairo", units="cm", width=15, height=15, pointsize=10, res=300)
    par(oma=c(4,4,4,4))
    plot(median_brf_reflectance)
    dev.off()
    
    # save the tile median in the composite directory
    SaveProcessedTileComposite(median_brf_reflectance, output_dir, composite_fname, tile, year, day)
    rm(list = c("median_brf_reflectance"))
    
    # delete temporary directory
    unlink(file.path(output_dir, tmp_dir), recursive=TRUE)

    # End Loop 8-day composite
    return(1)
  }
  
  
  # End of loop year
}

# stop cluster
if (PARALLEL_PROCESS_ENABLED) {
  stopCluster(cl)
  closeAllConnections()
}

# message
print("All tile composites processed. Good job!")

# measure run time
if (MEASURE_RUN_TIME_ENABLED) {
  end.time <- Sys.time()
  time.taken <- end.time - start.time
  time.taken
}

# TO DO: MAKE CODE TO MOSAIC ALL TILES AT ONCE
#
# # list and load tiles
# processed_tiles = list.files(paste0(output_dir,tmp_dir), pattern = "Processed")
# 
# # mosaic the tiles
# mosaic_brf = MosaicTilesAndSave(processed_tiles, output_dir, tmp_dir, year, day)
# 
# # plot brfnadir to file to verify it later
# png(filename=paste0(tile_preview_dir,"fig_",composite_fname,"_",year,day[8],".png"), type="cairo", units="cm", width=15, height=15, pointsize=10, res=300)
# par(oma=c(4,4,4,4))
# plot(mosaic_brf)
# dev.off()




