# script to generate a mask for amazonia in raster format using eva&huber shapefile
# the code is not optimized, have to change some directories to make it work

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
input_dir = "/mnt/DATADRIVE2/ricardo_data/_MAIAC/MAIAC_ProcessedTiles/"

# folder to put the processed files
output_dir = "/mnt/DATADRIVE2/ricardo_data/MAIAC_AMZ/"

# base filename of the processed file, e.g. if set base_filename = "maiac_ricardo", the processed files will be something like "maiac_ricardo_2000064_band1_latlon_crop.tif"
base_filename = "maiac_amz_nadir"

# tiles to mosaic
# South America, tiles_to_mosaic = c("h00v01", "h00v02", "h01v00", "h01v01", "h01v02", "h01v03", "h02v00", "h02v01", "h02v02", "h02v03", "h03v00", "h03v01", "h03v02", "h03v03", "h03v04", "h04v01", "h04v02", "h04v03", "h00v00", "h01v04", "h01v05", "h01v06", "h02v04", "h02v05")
# Amazonia, tiles_to_mosaic = c("h00v01", "h00v02", "h01v00", "h01v01", "h01v02", "h01v03", "h02v00", "h02v01", "h02v02", "h02v03", "h03v00", "h03v01", "h03v02")
tiles_to_mosaic = c("h00v01", "h00v02", "h01v00", "h01v01", "h01v02", "h01v03", "h02v00", "h02v01", "h02v02", "h02v03", "h03v00", "h03v01", "h03v02")

# crop extent information in lat/lon coordinates (min_lon, max_lon, min_lat, max_lat), if is_crop_enable is set to TRUE the script will crop the file to crop_ext extent
# Amazonia Eva Huber, crop_ext = extent(-80, -45, -21, 9), real extent is -79.62473, -44.35324, -20.47028, 8.63236  (xmin, xmax, ymin, ymax)
# Bamboo southwest amazon, crop_ext =  extent(-75, -66, -13, -6)
crop_ext = extent(-80, -45, -21, 9)
is_crop_enable = TRUE  # set to TRUE or FALSE

# number of cores to use in parallel processing
no_cores = 30


# PROCESS -----------------------------------------------------------------

# read composite vector from the functions folder
composite_vec = read.csv(paste0(paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/"),"composite_vec_16.csv"), header=F)

# band names
band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")

# Initiate cluster
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# select one maiac data to use as reference for the mask
i=364
j=1

# list files and filter for composite_vec i
file_list = list.files(input_dir, pattern=as.character(composite_vec[i,]), full.names=TRUE)

# filter for tiles
file_list = grep(file_list, pattern = paste(tiles_to_mosaic, collapse="|"), value=T)

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

# read amazonia eva & huber shapefile
library(rgdal)
AMZ = readOGR("/mnt/DATADRIVE2/ricardo_data/maiac_processing/AmazoniaLimits.shp")

# rasterize
amz_r = rasterize(x = AMZ,
                  y = raster(paste0(output_dir,base_filename,"_",composite_vec[i,],"_",band_names[j],"_latlon.tif")),
                  filename = "/mnt/DATADRIVE2/ricardo_data/maiac_processing/amazonia_raster.tif",
                  overwrite=T
)

# make it 0 or 1
amz_r[amz_r!=0]=1

# write it again now 0 and 1
writeRaster(amz_r, filename = "/mnt/DATADRIVE2/ricardo_data/maiac_processing/amazonia_raster.tif",
            overwrite = TRUE,
            format="GTiff",
            datatype = "INT2S",
            options = c("COMPRESS=LZW","PREDICTOR=2"))
