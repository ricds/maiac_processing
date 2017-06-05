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


# CONFIG ------------------------------------------------------------------


# maiac input folder
input_dir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/3_MAIAC_Ricardo/"

# folder to put the processed files
output_dir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/4_MAIAC_Ricardo_Bambu/"

# base filename of the processed file, e.g. if set base_filename = "maiac_ricardo", the processed files will be something like "maiac_ricardo_2000064_band1_latlon_crop.tif"
base_filename = "maiac_bamboo"

# tiles to mosaic
# South America = c("h01v02", "h01v01", "h00v01", "h00v02", "h01v00", "h01v03", "h02v00", "h02v01", "h02v02", "h02v03", "h03v00", "h03v01", "h03v02", "h03v03", "h03v04", "h03v05", "h04v00", "h04v01", "h04v02", "h04v03", "h00v00", "h00v03", "h01v04", "h01v05", "h01v06", "h02v04", "h02v05")
# Amazonia = c("h00v01", "h00v02", "h01v00", "h01v01", "h01v02", "h01v03", "h02v00", "h02v01", "h02v02", "h02v03", "h03v00", "h03v01", "h03v02")
tiles_to_mosaic = c("h01v01", "h01v02")

# crop extent information in lat/lon coordinates (min_lon, max_lon, min_lat, max_lat), if is_crop_enable is set to TRUE the script will crop the file to crop_ext extent
crop_ext = extent(-75, -66, -13, -6)
is_crop_enable = TRUE  # set to TRUE or FALSE

# number of cores to use in parallel processing
no_cores = 4



# PROCESS -----------------------------------------------------------------

# read composite vector from the functions folder
composite_vec = read.csv(paste0(paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/"),"composite_vec_16.csv"), header=F)

# band names
band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")

# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# process in parallel
f=foreach(i = 1:364, .packages=c("raster","gdalUtils","rgdal"), .errorhandling="remove") %dopar% {
  # list files and filter for composite_vec i
  file_list = list.files(input_dir, pattern=as.character(composite_vec[i,]), full.names=TRUE)
  
  # filter for tiles
  file_list = grep(file_list, pattern = paste(tiles_to_mosaic, collapse="|"), value=T)
  
  # process each band
  for (j in 1:9) {
    # proceed only if mosaic, latlon and crop file doesn't exist
    if (all(!file.exists(c(paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))))) {
      # mosaic
      mosaic_rasters(gdalfile = grep(file_list, pattern = band_names[j], value=T),
                     dst_dataset = paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),
                     overwrite=TRUE,
                     output_Raster = FALSE,
                     ot="Int16",
                     co = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"))
    }
    
    # proceed only if latlon and crop file doesn't exist
    if (all(!file.exists(c(paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))))) {
      # reproject from sinusoidal to latlon
      gdalwarp(srcfile = paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),
               dstfile = paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),
               overwrite = TRUE,
               t_srs = "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
               s_srs = "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs",
               ot="Int16",
               tr=c(0.009107388, 0.009107388), # 0.009107388 is the resolution from the old series
               wo = "INIT_DEST = NO_DATA",
               co = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"),
               r = "near")
      
      # delete pre-warp file
      unlink(paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"))
    }
    
    # proceed only if crop file doesn't exist and crop is enabled
    if (is_crop_enable && !file.exists(paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))) {
      # crop the file
      crop(x = raster(paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif")),
           y = crop_ext,
           filename = paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"),
           overwrite = TRUE,
           options = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"))
      
      # delete pre-crop file
      unlink(paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"))
    }
    
  }
}

# finish cluster
stopCluster(cl)

# message
print("Processing finished.")
