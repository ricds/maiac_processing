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
## 2) A config file named "config.txt" must exist in the script folder describing the tiles to process, dates, and directories. The "config_example.txt" example file is provided.
## 3) It is necessary to manually install the median2rcpp library, which is basically a faster median function
## 4) After running, if some "missing_files.txt" is produced, this correspond to files with missing data which have been filled with NA
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
#library(itertools)  #install.packages("itertools")
library(plyr)  #install.packages("plyr")
#library(median2rcpp)  # download.file("https://www.dropbox.com/s/chneec889dl0nck/median2rcpp_0.1.0.tar.gz?raw=1", destfile = "median2rcpp_0.1.0.tar.gz", mode="wb"); system({'R CMD INSTALL "median2rcpp_0.1.0.tar.gz"'})
#library(benchmarkme)

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

# set gdal directory
#gdal_setInstallation(gdal_dir)

# create process directory if it doesnt exist
dir.create(file.path(process_dir), showWarnings = FALSE, recursive=T)

# output directory, the one to export the processed composites
output_dir = paste0(process_dir, "MAIAC_ProcessedTiles/")

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

# product name MAIACTBRF, MAIACABRF, MAIACRTLS, don't change this
# product = c("MAIACTBRF","MAIACABRF","MCD19A1")
# parameters = c("MAIACRTLS","MCD19A3D")
product = "MCD19A1"
parameters = "MCD19A3D"
isMCD = TRUE

# define the output base filename
composite_fname = CreateCompositeName(composite_no, product, is_qa_filter, is_ea_filter, view_geometry, product_res = product_res)

# create matrix of days on each composite
day_mat = CreateDayMatrix(composite_no)

# create loop matrix containing all the information to iterate
loop_mat = CreateLoopMat(day_mat, composite_no, input_dir_vec, tile_vec, manual_run)

# create the earthdata file
write.table(paste0("machine urs.earthdata.nasa.gov login ",login_earthdata," password ",pwd_earthdata), file = "earthdata.netrc", quote = F, row.names = F, col.names = F)


# get file list --------------------------------------------------

# determine unique years of data
unique_years = as.numeric(unique(loop_mat[,2]))

# loop years
fname_list = list()
for (i in 1:length(unique_years)) {
  # list files from S3 to local machine
  fname_list[[i]] = get_filenames_to_download(functions_dir, unique_years[i])
}



# START OF PROCESSING ---------------------------------

# Loop through the loop_mat matrix 
f=foreach(j = 1:dim(loop_mat)[1], .packages=c("raster","gdalUtils","rgdal","RCurl"), .export=ls(.GlobalEnv), .errorhandling="remove") %do% {
  
  # message
  print("...")
  print(paste0(Sys.time(), ": Start processing a new composite... j=", j, " from ", dim(loop_mat)[1]))
  
  # measure time
  t1 = mytic()
  
  # For testing: manually setting the tile and year and input
  if (FALSE) {
    input_dir = "E:/MAIAC_Download/"
    tile = "h12v10"
    year = 2023
  }
  
  # get input_dir from loop_mat
  input_dir = as.character(loop_mat[j,3])
  
  # get tile from loop_mat
  tile = as.character(loop_mat[j,4])
  
  # get year from loop_mat
  year = as.numeric(loop_mat[j,2])
  
  # get the day vector from loop_mat, if its monthly get the dates according to the year, otherwise just get the values from day_mat
  if (composite_no == "month") {
    days_of_months = matrix(diff(seq(as.Date("2000-01-01"), as.Date("2040-01-01"), by = "month")), ncol = 12, byrow = T)
    day = format(seq(from = as.Date(paste0(year,"-", as.numeric(loop_mat[j,1]), "-01")), to = as.Date(paste0(year,"-", as.numeric(loop_mat[j,1]), "-", days_of_months[which(2000:2040==year),as.numeric(loop_mat[j,1])])), by = 1), "%j")
  } else {
    day = day_mat[as.numeric(loop_mat[j,1]),]
  }
  
  # get files for the given year, tile, and days
  fnames = fname_list[[grep(year, unique_years)]]
  fnames = grep(tile, fnames, value=T)
  fnames = grep(paste(paste0(year,day), collapse="|"), fnames, value=T)
  fnames_s3 = paste0(ifelse(as.numeric(sapply("MCD19A1", grepl, fnames)) == TRUE, "s3://lp-prod-protected/MCD19A1.061/", "s3://lp-prod-protected/MCD19A3D.061/"), sub(".hdf", "", fnames), "/", fnames)
  
  # clean download folder (hdf files)
  unlink(file.path(manual_dir_tiles[1]), recursive=T)
  
  # list files and download from s3
  downloaded_files = list.files(manual_dir_tiles[1], pattern=".hdf$")
  idx_missing_download = which(!(fnames %in% downloaded_files))
  if (length(idx_missing_download) > 0) {
    fnames_s3 = fnames_s3[idx_missing_download]
  } else {
    fnames_s3 = integer(0)
  }
  
  # download files if needed
  if (length(fnames_s3) > 0) {
    myt = timer("Downloading HDF files")
    
    # refresh credentials from earth data prior to download
    refresh_credentials_earthdata()
    
    # parallel download all files we need
    files_to_download = fnames_s3
    output_dir = manual_dir_tiles[1]
    snowrun(fun = S3_download_single_file,
            values = 1:length(files_to_download),
            no_cores = no_cores,
            var_export = c("files_to_download", "output_dir"))
    
    timer(myt)
  }
  
  # test if input dir is empty
  if (length(list.files(manual_dir_tiles[1]))==0) {
    # message
    print(paste0(Sys.time(), ": ERROR Input directory is Empty."))
    stop(paste0(Sys.time(), ": ERROR Input directory is Empty."))
  }
  
  # # check if composite processed file already exist, otherwise just skip to next iteration; manual_run overrides this and overwrite files
  # if (IsTileCompositeProcessed(composite_fname, tile, year, day, output_dir, overwrite_files))
  #   return(0)
  # 
  # # if no brf or rtls is available for given day, year, tile, return nan output, log the information and go to next iteration
  # if (!IsDataAvailable(product, tile, year, day, nan_tiles_dir, output_dir, obs="brf", composite_fname, composite_no, isMCD, product_res) | !IsDataAvailable(parameters, tile, year, day, nan_tiles_dir, output_dir, obs="rtls", composite_fname, composite_no, isMCD, product_res))
  #   return(0)
  
  # set temporary directory
  tmp_dir = paste0(tempdir(), "/tmp_",tile,"_",year,day[length(day)],"/")
  
  # delete temporary directory - to make sure it is clean
  unlink(file.path(tmp_dir), recursive=TRUE)

  # create temporary directory
  dir.create(file.path(tmp_dir), showWarnings = FALSE)
  
  # 1) get filenames from the available products and parameters files for the iteration
  product_fname = GetFilenameVec(product, input_dir, tile, year, day, offset_days=0)
  parameter_fname = GetFilenameVec(parameters, input_dir, tile, year, day, offset_days=24)
  
  # 2) (parallel computing) convert files from hdf to geotif using gdal_translate
  c2t = ConvertHDF2TIF(product_fname, parameter_fname, input_dir, output_dir, tmp_dir, no_cores, log_fname, is_ea_filter, is_qa_filter, process_dir, isMCD, product_res)
  if (!c2t) {
    write(j, file=paste0(process_dir,"hdf2tif_convert_fail_iteration.txt"), append=TRUE)
    print(paste0(Sys.time(), ": Skipping to next iteration."))
    return(0)
  }
  
  # remove directory from filenames, return only the "filenames".hdf
  product_fname = basename(product_fname)
  parameter_fname = basename(parameter_fname)
  
  # check if we have A1 and A3D at the same day for all dates, remove if not
  day_list = list(vector("character"), vector("character"))
  for (w in 1:length(day)) {
    val1 = grep(paste0("A", year, day[w]), product_fname, value=T)
    val2 = grep(paste0("A", year, day[w]), parameter_fname, value=T)
    day_list[[1]][w] = ifelse(length(val1) == 0, NA, val1[1])
    day_list[[2]][w] = ifelse(length(val2) == 0, NA, val2[1])
  }
  df=cbind(day_list[[1]], day_list[[2]])
  df = df[rowSums(is.na(df))==0,]
  product_fname=df[,1]
  parameter_fname=df[,2]
  
  # 3) organize the data according to the number of observations per day and the available bands, the way we have one object for each observation
  # message
  myt = timer("Loading files")
  brf_reflectance = LoadMAIACFilesGDALParallel(product_fname, output_dir, tmp_dir, product_res, isMCD, "A1") # 28 sec
  brf_fv = LoadMAIACFilesGDALParallel(product_fname, output_dir, tmp_dir, "Fv", isMCD, "A1")
  brf_fg = LoadMAIACFilesGDALParallel(product_fname, output_dir, tmp_dir, "Fg", isMCD, "A1")
  rtls_kiso = LoadMAIACFilesGDALParallel(parameter_fname, output_dir, tmp_dir, "Kiso", isMCD, "A3")
  rtls_kvol = LoadMAIACFilesGDALParallel(parameter_fname, output_dir, tmp_dir, "Kvol", isMCD, "A3")
  rtls_kgeo = LoadMAIACFilesGDALParallel(parameter_fname, output_dir, tmp_dir, "Kgeo", isMCD, "A3")
  timer(myt)
  
  # get the dates of each observation
  brf_reflectance_dates = LoadMAIACFilesGDALParallel(product_fname, output_dir, tmp_dir, product_res, isMCD, "A1", dateOnly = TRUE)
  
  # match the RTLS to the observations
  rtls_kiso = rtls_kiso[brf_reflectance_dates]
  rtls_kvol = rtls_kvol[brf_reflectance_dates]
  rtls_kgeo = rtls_kgeo[brf_reflectance_dates]
  
  # resample brf_fv and brf_fg
  myt = timer("Resampling FV and FG")
  brf_fv_resampled = resample_f(brf_fv, brf_reflectance)
  brf_fg_resampled = resample_f(brf_fg, brf_reflectance)
  file.remove(brf_fv)
  file.remove(brf_fg)
  brf_fv = brf_fv_resampled
  brf_fg = brf_fg_resampled
  timer(myt)
  
  # 4) load QA layers, create a mask for each date excluding bad pixels (possibly cloud, adjacent cloud, cloud shadows, etc.) and apply the mask
  # remove pixels such as: possibly cloud, cloud adjacent, cloud shadow, etc.
  if (is_qa_filter) {
    myt = timer("Applying QA to BRF")
    # filtering
    qa_brick = LoadMAIACFilesGDALParallel(product_fname, output_dir, tmp_dir, "Status_QA", isMCD, "A1")
    qa_mask = CreateQAMask(qa_brick)
    brf_reflectance_filtered = ApplyMaskOnBrick(brf_reflectance, qa_mask)
    
    # clean up
    file.remove(qa_mask)
    file.remove(qa_brick)
    file.remove(brf_reflectance)
    rm(list = c("qa_brick","qa_mask"))
    
    # rename
    brf_reflectance = brf_reflectance_filtered
    timer(myt)
  }
  
  # 6) (parallel computing) apply brf normalization to nadir for each date using the respective RTLS parameters or nearest RTLS file, and the eq. from MAIAC documentation: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
  #nadir_brf_reflectance = ConvertBRFNadir(brf_reflectance, brf_fv, brf_fg, rtls_kiso, rtls_kvol, rtls_kgeo, tile, year, output_dir, no_cores, log_fname, view_geometry, isMCD, product_res, tmp_dir)
  nadir_brf_reflectance = ConvertBRFNadirGDAL(brf_reflectance, brf_fv, brf_fg, rtls_kiso, rtls_kvol, rtls_kgeo, tile, year, output_dir, no_cores, log_fname, view_geometry, isMCD, product_res, tmp_dir)
  rm(list = c("brf_reflectance", "brf_fv", "brf_fg", "rtls_kiso", "rtls_kvol", "rtls_kgeo"))
  
  # test if nadir_brf_reflectance is empty, and return nan tile if it is true
  if (length(nadir_brf_reflectance) == 0) {
    nan_tile = GetNanTile(tile, nan_tiles_dir, isMCD, product_res)
    SaveProcessedTileComposite(nan_tile, output_dir, composite_fname, tile, year, day, composite_no)
    return(0)
  }
  
  # put the bands together so its easier to calc the median
  #nadir_brf_reflectance_per_band = ReorderBrickPerBand(nadir_brf_reflectance, output_dir, tmp_dir)
  nadir_brf_reflectance_per_band = ReorderBrickPerBandGDAL(nadir_brf_reflectance)
  rm(list = c("nadir_brf_reflectance"))
  
  # 7) calculate the median of each pixel using the remaining (best) pixels, and return a brick with 9 rasters (1-8 band, and no_samples of band1)
  #median_brf_reflectance = CalcMedianBRF(nadir_brf_reflectance_per_band, no_cores, log_fname, output_dir, tmp_dir)
  median_brf_reflectance = CalcMedianBRFGDAL(nadir_brf_reflectance_per_band)
  # sum(file.size(median_brf_reflectance))/(1024*1024)
  rm(list = c("nadir_brf_reflectance_per_band"))
  
  # define the composite number or name
  if (composite_no == "month") {
    composite_num = paste0("_",format(as.Date(paste0(day[length(day)],"-", year), "%j-%Y"), "%m"))
  } else {
    composite_num = day[length(day)]
  }
  
  # define filenames for the files
  if (product_res == 1000) {
    band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")
  } else {
    band_names = c("band1","band2","band3","band4","band5","band6","band7","no_samples")
  }
  output_filenames = paste0(output_dir,composite_fname,".",tile,".",year, composite_num,".",band_names,".tif")
  
  # 9) move final files from temporary dir to their output directory
  file.rename(median_brf_reflectance, output_filenames)
  rm(list = c("median_brf_reflectance"))
  
  # upload files to S3 and delete local files
  if (upload_to_s3) {
    myt = timer("Uploading files to S3")
    
    # upload
    input_files = output_filenames
    output_files = paste0(S3_output_path, view_geometry, "/", year, "/", basename(output_filenames))
    snowrun(fun = S3_download_upload,
            values = 1:length(input_files),
            no_cores = no_cores,
            var_export = c("input_files", "output_files", "S3_profile"))
    
    # delete local files
    file.remove(output_filenames)
    timer(myt)
  }
  
  # delete temporary directory
  unlink(file.path(tmp_dir), recursive=TRUE)
  
  # clean download folder (hdf files)
  unlink(file.path(manual_dir_tiles[1]), recursive=T)
  
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


# turn off ----------------------------------------------------------------

# stop instance when finish
if (TRUE) {
  instance_id = system('ec2metadata --instance-id', intern=T)
  system(paste0("aws ec2 stop-instances --instance-ids ", instance_id))
}