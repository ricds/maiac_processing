##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to mosaic, reproject and (optionally) crop MAIAC composites
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-05-26
##################################################
## General processing workflow:
## 1) mosaic selected tiles
## 2) reproject files from sinusoidal to geographic projection WGS-84 (lat/lon coordinates)
## 3) (optional) crop the reprojected mosaic to a selected extent given lat/lon coordinates
##################################################

# clear all
rm(list = ls())

# packages
library(rgdal)
library(gdalUtils)
library(raster)
library(foreach)
library(doParallel)
library(rstudioapi)  #install.packages("rstudioapi")


# PROCESS -----------------------------------------------------------------

# get the folder where the script R functions and config file are placed
functions_dir = paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")

# load config.txt file that should be in the same directory of the scripts
source(paste0(functions_dir, "config.txt"))

# read composite vector from the functions folder
composite_vec = read.csv(paste0(functions_dir,"maiac_composite_vec_",composite_no,".csv"), header=F)

# create output folder in case it does not exist
dir.create(file.path(mosaic_output_dir), showWarnings = FALSE, recursive=T)

# define the extent for cropping - if enabled
if (!is.na(crop_polygon)) {
  CROP_POLYGON = readOGR(crop_polygon)
  crop_ext = extent(CROP_POLYGON)
}
if (!is.na(crop_raster)) {
  CROP_RASTER = raster(crop_raster)
  crop_ext = extent(CROP_RASTER)
}

# define mask
if (!is.na(mask_polygon)) {
  MASK_POLYGON = readOGR(mask_polygon)
}


# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# process in parallel
i=1
f=foreach(i = 1:dim(composite_vec)[1], .packages=c("raster","gdalUtils","rgdal"), .errorhandling="remove") %dopar% {
  # list files and filter for composite_vec i
  file_list = list.files(mosaic_input_dir, pattern=as.character(composite_vec[i,]), full.names=TRUE)
  
  # filter for tiles
  file_list = grep(file_list, pattern = paste(tiles_to_mosaic, collapse="|"), value=T)
  
  # if there are files with the given composite name
  if (length(file_list)!=0) {
    
    # process each band
    j=1
    for (j in 1:length(band_names)) {
      # proceed only if mosaic, latlon and crop file doesn't exist
      if (all(!file.exists(c(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))))) {
        # mosaic
        mosaic_rasters(gdalfile = grep(file_list, pattern = band_names[j], value=T),
                       dst_dataset = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),
                       overwrite=TRUE,
                       output_Raster = FALSE,
                       ot="Int16",
                       co = c("COMPRESS=LZW","PREDICTOR=2"))
      }
      
      # proceed only if latlon and crop file doesn't exist
      if (all(!file.exists(c(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"),paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop_mask.tif"))))) {

        # set source crs
        source_srs = "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
        # if it is from the MCD (new product) - we have to adjust this - we check it by the original resolution equal to 926.6254
        if (res(stack(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],".tif")))[1] != 1000) {
          source_srs = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
        }
                
        if (!is_crop_enable) {
          # reproject from sinusoidal to latlon
          gdalwarp(srcfile = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),
                   dstfile = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),
                   overwrite = TRUE,
                   t_srs = "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                   s_srs = source_srs,
                   ot="Int16",
                   tr=c(0.009107388, 0.009107388), # 0.009107388 is the resolution from the old series
                   wo = "INIT_DEST = NO_DATA",
                   co = c("COMPRESS=LZW","PREDICTOR=2"),
                   te = c(-113.9209, -61.15665, -2.073097, 14.38913),
                   r = "near")
        } else {
          gdalwarp(srcfile = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),
                   dstfile = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"),
                   overwrite = TRUE,
                   t_srs = "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                   s_srs = source_srs,
                   ot="Int16",
                   tr=c(0.009107388, 0.009107388), # 0.009107388 is the resolution from the old series
                   te = c(extent(crop_ext)[1],extent(crop_ext)[3],extent(crop_ext)[2],extent(crop_ext)[4]),
                   wo = "INIT_DEST = NO_DATA",
                   co = c("COMPRESS=LZW","PREDICTOR=2"),
                   r = "near")
        }
        
        # delete pre-warp file
        unlink(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"))
      }
      
      # proceed only if mask file doesn't exist and is enabled
      if (is_mask_enable && all(!file.exists(c(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"),paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop_mask.tif"))))) {

        # define file name
        if (!is_crop_enable) {
          fname = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon")
        } else {
          fname = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop")
        }
        mask(x = raster(paste0(fname, ".tif")),
             mask = MASK_POLYGON,
             filename = paste0(fname, "_mask.tif"),
             overwrite = TRUE,
             format="GTiff",
             datatype = "INT2S",
             options = c("COMPRESS=LZW","PREDICTOR=2")
        )
        
        # delete pre-mask file
        unlink(paste0(fname, ".tif"))
      }
      
    }
  }
  
}

# finish cluster
stopCluster(cl)

# message
print("Processing finished.")

# check file size
#sum(file.info(list.files(mosaic_output_dir, pattern="latlon", full.names = TRUE))$size)/(1024*1024*1024)
#sum(file.info(list.files(mosaic_output_dir, pattern="crop", full.names = TRUE))$size)/(1024*1024*1024)