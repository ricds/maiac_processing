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
## Requirement: setup the config_mosaic_vi.txt file
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
library(compiler)  #install.packages("compiler")

# define if EC2 or WS
machine = "WS"


# PROCESS -----------------------------------------------------------------

# get the folder where the script R functions and config file are placed
functions_dir = paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")

# load config.txt file that should be in the same directory of the scripts
source(paste0(functions_dir, "config_mosaic_vi.txt"))

# load functions
source(paste0(functions_dir, "maiac_processing_functions.R"))

# create view geometry string with first letter in Uppercase - the files in S3 are as this
view_geometry_upper = paste0(toupper(substr(view_geometry, 1,1)), substr(view_geometry, 2, nchar(view_geometry)))

# read composite vector from the functions folder
composite_vec = data.frame(createCompDates(composite_no, end_year = end_year))

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

# set spatial resolution
if (product_res == 1000) {
  spat_res = c(0.009107388,0.009107388) # 1000
} else {
  spat_res = c(0.004504505,0.004504505) # 500
}


# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)


# process in parallel
i=1
f=foreach(i = 1:dim(composite_vec)[1], .packages=c("raster","gdalUtils","rgdal"), .errorhandling="remove") %dopar% {

  # define year and month
  year = substr(composite_vec[i,], 1,4)
  month = substr(composite_vec[i,], 6,7)
  
  # process each band
  j=1
  for (j in 1:length(band_names)) {
    # define output fname
    output1 = paste0(mosaic_output_dir,mosaic_base_filename,"_",view_geometry, "_", composite_vec[i,],"_",band_names[j],".tif")
    output2 = paste0(mosaic_output_dir,mosaic_base_filename,"_",view_geometry, "_",composite_vec[i,],"_",band_names[j],"_latlon.tif")
    output3 = paste0(mosaic_output_dir,mosaic_base_filename,"_",view_geometry, "_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif")
    output4 = paste0(mosaic_output_dir,mosaic_base_filename,"_",view_geometry, "_",composite_vec[i,],"_",band_names[j],"_latlon_crop_mask.tif")
    
    # create file list
    file_list = paste0(s3_dir, "tiled/", view_geometry, "/", year, "/SR_",view_geometry_upper,"_month_MCD19A1_FilterQA.",tiles_to_mosaic,".", year, "_", month, ".",band_names[j],".tif")
    
    # create vrt for the file list
    vrt = vrt_imgs(file_list, gdalbuildvrt)
    
    # proceed only if mosaic, latlon and crop file doesn't exist
    if (all(!file.exists(c(output1, output2, output3)))) {
      # mosaic
      mosaic_rasters(gdalfile = vrt,
                     dst_dataset = output1,
                     overwrite = TRUE,
                     output_Raster = FALSE,
                     ot = "Int16",
                     co = c("COMPRESS=LZW","PREDICTOR=2"))
    }
    
    # proceed only if latlon and crop file doesn't exist
    if (all(!file.exists(c(output2, output3)))) {
      
      # set source crs
      source_srs = "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
      # if it is from the MCD (new product) - we have to adjust this - we check it by the original resolution equal to 926.6254
      if (all(res(stack(output1))[1] != c(1000,500))) {
        source_srs = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
      }
      
      if (!is_crop_enable) {
        # reproject from sinusoidal to latlon
        gdalwarp(srcfile = output1,
                 dstfile = output2,
                 overwrite = TRUE,
                 #t_srs = "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                 t_srs = "+proj=longlat +datum=WGS84 +no_defs",
                 s_srs = source_srs,
                 ot="Int16",
                 tr=spat_res, # 0.009107388 is the resolution from the old series
                 wo = "INIT_DEST = NO_DATA",
                 co = c("COMPRESS=LZW","PREDICTOR=2"),
                 te = c(-113.9209, -61.15665, -2.073097, 14.38913),
                 r = "near")
      } else {
        gdalwarp(srcfile = output1,
                 dstfile = output3,
                 overwrite = TRUE,
                 t_srs = "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
                 s_srs = source_srs,
                 ot="Int16",
                 tr=spat_res, # 0.009107388 is the resolution from the old series
                 te = c(extent(crop_ext)[1],extent(crop_ext)[3],extent(crop_ext)[2],extent(crop_ext)[4]),
                 wo = "INIT_DEST = NO_DATA",
                 co = c("COMPRESS=LZW","PREDICTOR=2"),
                 r = "near")
      }
      
      # delete pre-warp file
      unlink(output1)
    }
    
    # proceed only if mask file doesn't exist and is enabled
    if (is_mask_enable && all(!file.exists(output3))) {
      
      # define file name
      mask(x = raster(output3),
           mask = MASK_POLYGON,
           filename = output4,
           overwrite = TRUE,
           format="GTiff",
           datatype = "INT2S",
           options = c("COMPRESS=LZW","PREDICTOR=2")
      )
      
      # delete pre-mask file
      unlink(output3)
    }
    
    # remove vrt
    file.remove(vrt)
    
  }
  
}

# finish cluster
stopCluster(cl)

# message
print("Processing finished.")


# check file size
#sum(file.info(list.files(mosaic_output_dir, pattern="latlon", full.names = TRUE))$size)/(1024*1024*1024)
#sum(file.info(list.files(mosaic_output_dir, pattern="crop", full.names = TRUE))$size)/(1024*1024*1024)