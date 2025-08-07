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
machine = "EC2"


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
    output1 = paste0(mosaic_output_dir, mosaic_base_filename,"_",view_geometry, "_",composite_vec[i,],"_",band_names[j],".tif")
    output2 = paste0(mosaic_output_dir, mosaic_base_filename,"_",view_geometry, "_",composite_vec[i,],"_",band_names[j],"_latlon.tif")
    output3 = paste0(mosaic_output_dir, mosaic_base_filename,"_",view_geometry, "_",composite_vec[i,],"_",band_names[j],"_latlon_crop.tif")
    output4 = paste0(mosaic_output_dir, mosaic_base_filename,"_",view_geometry, "_",composite_vec[i,],"_",band_names[j],"_latlon_crop_mask.tif")
    
    # define output filename in s3
    input_file = output2
    output_file_s3 = paste0(s3_dir, "mosaic/", view_geometry, "/", basename(input_file))
    
    # skip ?
    if (S3_file_exists(output_file_s3)) {
      next
    }
    
    # create file list
    #file_list = paste0(s3_dir, "tiled/", view_geometry, "/", year, "/SR_",view_geometry_upper,"_month_MCD19A1_FilterQA.",tiles_to_mosaic,".", year, "_", month, ".",band_names[j],".tif")
    file_list = paste0(s3_dir, "tiled/", view_geometry, "/", year, "/SR_",view_geometry_upper,"_month_MAIACTerraAqua_",tiles_to_mosaic,"_", year, "_", month, "_",band_names[j],".tif")
    
    # download files
    tmp_dir = paste0(tempdir(), "/")
    file_list_local = paste0(tmp_dir, basename(file_list))
    try(S3_copy_single(file_list, file_list_local, S3_profile))
    
    # list files in disk
    file_list_local = normalizePath(list.files(tmp_dir, full.names=T, pattern=".tif$"))
    
    # skip if no files
    if (length(file_list_local) == 0) next
    
    # create vrt for the file list
    vrt = vrt_imgs(file_list_local, gdalbuildvrt)
    
    # proceed only if mosaic, latlon and crop file doesn't exist
    if (all(!file.exists(c(output1, output2, output3)))) {
      # mosaic
      mosaic_rasters(gdalfile = vrt,
                     dst_dataset = output1,
                     overwrite = TRUE,
                     output_Raster = FALSE,
                     ot = "Int16",
                     a_nodata = "32767",
                     co = c("COMPRESS=LZW","PREDICTOR=2","TILED=YES"))
    }
    
    # proceed only if latlon and crop file doesn't exist
    if (all(!file.exists(c(output2, output3)))) {
      
      # set source crs
      #source_srs = "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
      source_srs = "+proj=sinu +lon_0=-70 +R=6371007.181 +units=m +no_defs"
      # # if it is from the MCD (new product) - we have to adjust this - we check it by the original resolution equal to 926.6254
      # if (all(res(stack(output1))[1] != c(1000,500))) {
      #   source_srs = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
      # }
      
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
                 co = c("COMPRESS=LZW","PREDICTOR=2","TILED=YES"),
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
                 co = c("COMPRESS=LZW","PREDICTOR=2","TILED=YES"),
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
    
    # upload file to s3
    try(S3_copy_single(input_file, output_file_s3, S3_profile))
    file.remove(output2)
    
  }
  
  #
  print(paste("i=",i))
  
}

# finish cluster
stopCluster(cl)

# # upload files to s3
# input_files = list.files(mosaic_output_dir, full.names=T, pattern=".tif")
# output_files = paste0(s3_dir, "mosaic/", view_geometry, "/", basename(input_files))
# try(S3_copy_single_parallel(input_files, output_files, S3_profile))
# file.remove(input_files)

# message
print("Processing finished.")


# check file size
#sum(file.info(list.files(mosaic_output_dir, pattern="latlon", full.names = TRUE))$size)/(1024*1024*1024)
#sum(file.info(list.files(mosaic_output_dir, pattern="crop", full.names = TRUE))$size)/(1024*1024*1024)



# check mosaic files ------------------------------------------------------

if (FALSE) {
  
  ## noted that some mosaics have very low file size, need to chekc if they are like that because of fault in the mosaic or what is happening
  
  # list files and get their file size
  file_list = s3_list_bucket("s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/anisotropy/", RETRIEVE_ONLY_KEY = FALSE)
  print(length(file_list[[1]]))
  file_size = file_list$Size/(1024*1024)
  idx = file_size < 4
  file_filtered = file_list$Key[idx]
  print(file_filtered)
  if (length(file_filtered) > 0) S3_remove(file_filtered)
  
  file_list = s3_list_bucket("s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/backscat/", RETRIEVE_ONLY_KEY = FALSE)
  print(length(file_list[[1]]))
  #(sort(file_list$Size)/(1024*1024))[1:100]
  file_size = file_list$Size/(1024*1024)
  idx = file_size < 4
  file_filtered = file_list$Key[idx]
  print(file_filtered)
  if (length(file_filtered) > 0) S3_remove(file_filtered)
  
  file_list = s3_list_bucket("s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/forwardscat/", RETRIEVE_ONLY_KEY = FALSE)
  print(length(file_list[[1]]))
  #(sort(file_list$Size)/(1024*1024))[1:100]
  file_size = file_list$Size/(1024*1024)
  idx = file_size < 4
  file_filtered = file_list$Key[idx]
  print(file_filtered)
  if (length(file_filtered) > 0) S3_remove(file_filtered)
  
  file_list = s3_list_bucket("s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/mosaic/nadir/", RETRIEVE_ONLY_KEY = FALSE)
  print(length(file_list[[1]]))
  #(sort(file_list$Size)/(1024*1024))[1:100]
  file_size = file_list$Size/(1024*1024)
  idx = file_size < 4
  file_filtered = file_list$Key[idx]
  print(file_filtered)
  if (length(file_filtered) > 0) S3_remove(file_filtered)
  
  
  ## issue with the newly created files
  
}

# create vrt --------------------------------------------------------------


if (FALSE) {
  
  #
  #view_geometry = "nadir"
  view_geometry = "anisotropy"
  
  #
  band_names = c("evi", "ndvi", "gcc", "band1","band2","band3","band4","band5","band6","band7","band8","no_samples")
  
  # list mosaic files
  mosaic_dir_s3 = paste0(s3_dir, "mosaic/", view_geometry, "/")
  file_list = s3_list_bucket(mosaic_dir_s3)
  
  # define output directory
  vrt_output_dir = paste0(s3_dir, "mosaic/", view_geometry, "_vrt/")
  
  # process each band
  j=1
  for (j in 1:length(band_names)) {
    
    # filter for band
    file_list_j = grep(band_names[j], file_list, value=T)
    
    # create vrt
    vrt_fname = vrt_imgs(s3_to_vsis(file_list_j), gdalbuildvrt, add_cmd = "-separate -vrtnodata 32767 -srcnodata 32767")
    
    # create stats for faster opening
    system.time({system(paste("gdalinfo", vrt_fname, "-approx_stats"), ignore.stdout = TRUE, ignore.stderr = TRUE)})

    # upload vrt
    s3_vrt = paste0(vrt_output_dir, view_geometry, "_", band_names[j], "_stats.vrt")
    S3_copy_single(vrt_fname, s3_vrt, "ctrees")
    
  }
  
  
}


if (FALSE) {
  
  #
  y=-11.55404249
  x=-53.67970051
  gdallocationinfo_run = paste(
    "gdallocationinfo",
    "-geoloc",
    "-valonly",
    vrt_fname,
    x,
    y
  )
  a = system(gdallocationinfo_run, intern = TRUE)
  a = as.numeric(a)
  plot(a, type="l")
  
}