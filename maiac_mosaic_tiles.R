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

# band names
band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")

# read crop polygon/raster
if (!is.na(crop_polygon)) {
  CROP_RASTER = raster(crop_raster)
  CROP_POLYGON = readOGR(crop_polygon)
}

# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# process in parallel
f=foreach(i = 1:dim(composite_vec)[1], .packages=c("raster","gdalUtils","rgdal"), .errorhandling="remove") %dopar% {
  # list files and filter for composite_vec i
  file_list = list.files(mosaic_input_dir, pattern=as.character(composite_vec[i,]), full.names=TRUE)
  
  # filter for tiles
  file_list = grep(file_list, pattern = paste(tiles_to_mosaic, collapse="|"), value=T)
  
  # if there are files with the given composite name
  if (length(file_list)!=0) {
    
    # process each band
    for (j in 1:9) {
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
      if (all(!file.exists(c(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))))) {
        # reproject from sinusoidal to latlon
        gdalwarp(srcfile = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),
                 dstfile = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),
                 overwrite = TRUE,
                 t_srs = "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                 s_srs = "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs",
                 ot="Int16",
                 tr=c(0.009107388, 0.009107388), # 0.009107388 is the resolution from the old series
                 wo = "INIT_DEST = NO_DATA",
                 co = c("COMPRESS=LZW","PREDICTOR=2"),
                 r = "near")
        
        # delete pre-warp file
        unlink(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"))
      }
      
      # proceed only if crop file doesn't exist and crop is enabled
      if (is_crop_enable && !file.exists(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))) {
        
        # crop the file by polygon/raster
        if (!is.na(crop_polygon)) {
          crop(x = mask(x=raster(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif")), mask=CROP_RASTER),
               y = CROP_POLYGON,
               filename = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"),
               overwrite = TRUE,
               format="GTiff",
               datatype = "INT2S",
               options = c("COMPRESS=LZW","PREDICTOR=2")
          )
        } else {
          # crop the file by extent
          crop(x = raster(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif")),
               y = crop_ext,
               filename = paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"),
               overwrite = TRUE,
               format="GTiff",
               datatype = "INT2S",
               options = c("COMPRESS=LZW","PREDICTOR=2"))
        }
        
        # delete pre-crop file
        unlink(paste0(mosaic_output_dir,mosaic_base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"))
      }
      
    }
  }
  
}

# finish cluster
stopCluster(cl)

# message
print("Processing finished.")

# check file size
sum(file.info(list.files(mosaic_input_dir, pattern="crop", full.names = TRUE))$size)/(1024*1024*1024)  # 37.29926