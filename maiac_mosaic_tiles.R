##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to merge MAIAC composites in a mosaic
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-05-26
##################################################
## General processing workflow:
## 1) mosaic
## 2) reproject
## 3) crop (optional)
##################################################

# packages
library("rgdal")
library("gdalUtils")
library("raster")
library("zoo") # na.aggregate()
library("foreach")
library("doParallel")

# clear all
rm(list = ls())

# number of cores
no_cores = 4

# bamboo shapefile fname
fname_BambooShp = "D:/1_Dataset/3_Perimeter/tabocal_milton-bianchini_2005__lat-long_wgs84/tabocal_milton-bianchini_2005__lat-long_wgs84.shp";

# maiac input folder
maiac_tiles_input = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/3_MAIAC_Ricardo/"

# folder to put the bamboo files
bamboo_mosaic_output = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/4_MAIAC_Ricardo_Bambu/"

# read composite vector
composite_vec = read.csv(paste0(maiac_tiles_input,"composite_vec_16.csv"), header=F)

# band names
band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")

# base filename of the product
base_filename = "maiac_bamboo"

# tiles to mosaic
tiles_to_mosaic = c("h01v01", "h01v02")

# crop extent info
crop_ext = extent(-75, -66, -13, -6)
is_crop_enable = TRUE

# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# process in parallel
f=foreach(i = 1:364, .packages=c("raster","gdalUtils","rgdal"), .errorhandling="remove") %dopar% {
  # list files and filter for composite_vec i
  file_list = list.files(maiac_tiles_input, pattern=as.character(composite_vec[i,]), full.names=TRUE)
  
  # filter for tiles
  file_list = grep(file_list, pattern = paste(tiles_to_mosaic, collapse="|"), value=T)
  
  # process each band
  for (j in 1:9) {
    # proceed only if mosaic, latlon and crop file doesn't exist
    if (all(!file.exists(c(paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))))) {
      # mosaic
      mosaic_rasters(gdalfile = grep(file_list, pattern = band_names[j], value=T),
                     dst_dataset = paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),
                     overwrite=TRUE,
                     output_Raster = FALSE,
                     ot="Int16",
                     co = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"))
    }
    
    # proceed only if latlon and crop file doesn't exist
    if (all(!file.exists(c(paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))))) {
      # reproject from sinusoidal to latlon
      gdalwarp(srcfile = paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"),
               dstfile = paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"),
               overwrite = TRUE,
               t_srs = "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
               s_srs = "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs",
               ot="Int16",
               tr=c(0.009107388, 0.009107388), # 0.009107388 is the resolution from the old series
               wo = "INIT_DEST = NO_DATA",
               co = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"),
               r = "near")
      
      # delete pre-warp file
      unlink(paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],".tif"))
    }
    
    # proceed only if crop file doesn't exist
    if (is_crop_enable && !file.exists(paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"))) {
      # crop the file
      crop(x = raster(paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif")),
           y = crop_ext,
           filename = paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif"),
           overwrite = TRUE)
      
      # delete pre-crop file
      unlink(paste0(bamboo_mosaic_output,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif"))
    }
    
  }
}

# finish cluster
stopCluster(cl)

# message
print("Processing finished.")
