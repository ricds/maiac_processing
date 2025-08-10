##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: calculate vegetation indices NDVI and EVI using the maiac composites
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-06-27
## Notes: get files from "mosaic_output_dir" from "config.txt", calc VI and save them in the same directory
##################################################

# clean environment
rm(list = ls())

# load required libraries
library(raster)  #install.packages("raster")
library(foreach)  #install.packages("foreach") # click yes if asked
library(doParallel)  #install.packages("doParallel")
library(rstudioapi)  #install.packages("rstudioapi")
library(compiler)  #install.packages("compiler")
#library(itertools)  #install.packages("itertools")

# get the folder where the script R functions and config file are placed
functions_dir = paste0(dirname(rstudioapi::getActiveDocumentContext()$path),"/")

# load config.txt file that should be in the same directory of the scripts
machine = "EC2"
source(paste0(functions_dir, "config_mosaic_vi.txt"))

# load functions
source(paste0(functions_dir, "maiac_processing_functions.R"))


# functions ----------------------------------------------------------------

# function to calc evi
f_evi = function(NIR, RED, BLUE) {
  C1 = 6
  C2 = 7.5
  L = 1
  G = 2.5
  EVI = G*{NIR-RED}/{NIR+{C1*RED}-{C2*BLUE}+L}
  EVI*10000
}
ff_evi = cmpfun(f_evi)

# function to calc ndvi
f_ndvi = function(NIR, RED) {
  NDVI = {NIR-RED}/{NIR+RED}
  NDVI*10000
}
ff_ndvi = cmpfun(f_ndvi)

# function to calc gcc
f_gcc = function(RED, GREEN, BLUE) {
  GCC = {GREEN}/{RED+GREEN+BLUE}
  GCC*10000
}
ff_gcc = cmpfun(f_gcc)

# function to apply the 10000 scaling
scale_data = function(input_fname, output_fname) {
  gdal_calc_run = paste("gdal_calc.py",
                        paste0("--calc \" A / 10000 \" "), 
                        "-A", input_fname,
                        "--format GTiff",
                        "--type Float32",
                        "--overwrite",
                        "--co COMPRESS=DEFLATE",
                        "--NoDataValue 32767",
                        "--quiet",
                        "--outfile", output_fname)
  system(gdal_calc_run)
}


# run ---------------------------------------------------------------------

# read composite vector from the functions folder
composite_vec = data.frame(createCompDates(composite_no, end_year = end_year))

# input directory
s3_input_dir = paste0(s3_dir, "mosaic/", view_geometry, "/")
s3_input_dir_list = s3_list_bucket(s3_input_dir)

#
no_cores = 8

# Initiate cluster - Only use parallel if the mosaic is not huge like the whole south america
cl = parallel::makeCluster(no_cores)
registerDoParallel(cl)

# process
i=1
#for (i in 1:dim(composite_vec)[1]) {
f=foreach(i = 1:dim(composite_vec)[1], .packages=c("raster"), .errorhandling="remove") %dopar% { # for parallel
  
  if (TRUE) {
    
    # check if files exist for given composite
    file_list = grep(as.character(composite_vec[i,]), s3_input_dir_list, value=T)
    
    # if there are files with the given composite name
    if (length(file_list)!=0) {
      
      # define output filenames
      ndvi_fname = paste0(dirname(grep(file_list, pattern="band1", value=T)), "/", mosaic_base_filename, "_", view_geometry, "_", composite_vec[i,], "_ndvi_latlon.tif")
      evi_fname = paste0(dirname(grep(file_list, pattern="band1", value=T)), "/", mosaic_base_filename, "_", view_geometry, "_", composite_vec[i,], "_evi_latlon.tif")
      gcc_fname = paste0(dirname(grep(file_list, pattern="band1", value=T)), "/", mosaic_base_filename, "_", view_geometry, "_", composite_vec[i,], "_gcc_latlon.tif")
      
      # skip if output already exists
      NDVI_EXISTS = S3_file_exists(ndvi_fname) 
      EVI_EXISTS = S3_file_exists(evi_fname) 
      GCC_EXISTS = S3_file_exists(gcc_fname) 
      if (NDVI_EXISTS & EVI_EXISTS & GCC_EXISTS) next
      
      # convert s3 to vsis
      file_list = s3_to_vsis(file_list)
      
      # # get bands
      # maiac_band1 = raster(grep(file_list, pattern="band1", value=T))/10000
      # maiac_band2 = raster(grep(file_list, pattern="band2", value=T))/10000
      # maiac_band3 = raster(grep(file_list, pattern="band3", value=T))/10000
      # maiac_band4 = raster(grep(file_list, pattern="band4", value=T))/10000
      # 
      # # apply function
      # if (!NDVI_EXISTS) {
      #   ndvi_tmp = paste0(tempfile(), ".tif")
      #   writeRaster(x=overlay(maiac_band2, maiac_band1, fun=ff_ndvi),
      #               filename = ndvi_tmp,
      #               overwrite = TRUE, format = "GTiff",
      #               datatype = "INT2S",
      #               options = c("COMPRESS=LZW","PREDICTOR=2"))
      # }
      # 
      # if (!EVI_EXISTS) {
      #   evi_tmp = paste0(tempfile(), ".tif")
      #   writeRaster(x=overlay(maiac_band2, maiac_band1, maiac_band3, fun=ff_evi),
      #               filename = evi_tmp,
      #               overwrite = TRUE, format = "GTiff",
      #               datatype = "INT2S",
      #               options = c("COMPRESS=LZW","PREDICTOR=2"))
      # }
      # 
      # 
      # if (!GCC_EXISTS) {
      #   gcc_tmp = paste0(tempfile(), ".tif")
      #   writeRaster(x=overlay(maiac_band1, maiac_band4, maiac_band3, fun=ff_gcc),
      #               filename = gcc_tmp,
      #               overwrite = TRUE, format = "GTiff",
      #               datatype = "INT2S",
      #               options = c("COMPRESS=LZW","PREDICTOR=2"))
      # }
      
      # get bands
      maiac_band1 = grep(file_list, pattern="band1", value=T)
      maiac_band2 = grep(file_list, pattern="band2", value=T)
      maiac_band3 = grep(file_list, pattern="band3", value=T)
      maiac_band4 = grep(file_list, pattern="band4", value=T)
      
      # check length
      if (length(maiac_band1) != 1 & length(maiac_band2) != 1 & length(maiac_band3) != 1 & length(maiac_band4) != 1) next
      
      # prepare data
      maiac_band1_tmp = paste0(tempfile(), ".tif")
      maiac_band2_tmp = paste0(tempfile(), ".tif")
      maiac_band3_tmp = paste0(tempfile(), ".tif")
      maiac_band4_tmp = paste0(tempfile(), ".tif")
      scale_data(maiac_band1, maiac_band1_tmp)
      scale_data(maiac_band2, maiac_band2_tmp)
      scale_data(maiac_band3, maiac_band3_tmp)
      scale_data(maiac_band4, maiac_band4_tmp)
      
      
      # calc ndvi
      if (!NDVI_EXISTS) {
        # NDVI = {NIR-RED}/{NIR+RED}
        # NDVI*10000
        ndvi_tmp = paste0(tempfile(), ".tif")
        gdal_calc_run = paste("gdal_calc.py",
                              paste0("--calc \" ((A-B)/(A+B))*10000 \" "), 
                              "-A", maiac_band2_tmp,
                              "-B", maiac_band1_tmp,
                              "--format GTiff",
                              "--type Int16",
                              "--overwrite",
                              "--co COMPRESS=DEFLATE",
                              "--quiet",
                              "--outfile", ndvi_tmp)
        system(gdal_calc_run)
      }
      
      # calc ndvi
      if (!EVI_EXISTS) {
        # C1 = 6
        # C2 = 7.5
        # L = 1
        # G = 2.5
        # EVI = G*{NIR-RED}/{NIR+{C1*RED}-{C2*BLUE}+L}
        # EVI*10000
        evi_tmp = paste0(tempfile(), ".tif")
        gdal_calc_run = paste("gdal_calc.py",
                              paste0("--calc \" (2.5*(A-B)/(A+(6*B)-(7.5*C)+1))*10000 \" "), 
                              "-A", maiac_band2_tmp, #nir
                              "-B", maiac_band1_tmp, #red
                              "-C", maiac_band3_tmp, #blue
                              "--format GTiff",
                              "--type Int16",
                              "--overwrite",
                              "--co COMPRESS=DEFLATE",
                              "--quiet",
                              "--outfile", evi_tmp)
        system(gdal_calc_run)
      }
      
      # calc ndvi
      if (!GCC_EXISTS) {
        # GCC = {GREEN}/{RED+GREEN+BLUE}
        # GCC*10000
        gcc_tmp = paste0(tempfile(), ".tif")
        gdal_calc_run = paste("gdal_calc.py",
                              paste0("--calc \" (B/(A+B+C))*10000 \" "), 
                              "-A", maiac_band1_tmp,
                              "-B", maiac_band4_tmp,
                              "-C", maiac_band3_tmp,
                              "--format GTiff",
                              "--type Int16",
                              "--overwrite",
                              "--co COMPRESS=DEFLATE",
                              "--quiet",
                              "--outfile", gcc_tmp)
        system(gdal_calc_run)
      }
      
      # upload results
      if (!NDVI_EXISTS) S3_copy_single(ndvi_tmp, ndvi_fname, "ctrees")
      if (!EVI_EXISTS) S3_copy_single(evi_tmp, evi_fname, "ctrees")
      if (!GCC_EXISTS) S3_copy_single(gcc_tmp, gcc_fname, "ctrees")
      
      # clean files
      try(file.remove(ndvi_tmp, evi_tmp, gcc_tmp, maiac_band1_tmp, maiac_band2_tmp, maiac_band3_tmp, maiac_band4_tmp))
      
    }
    
    
    # progress monitor
    print(paste(i,dim(composite_vec)[1]))
    
    # clear
    gc()
    removeTmpFiles(h=0)
    
  }
  
}

# finish cluster
stopCluster(cl)

# message
print("Processing finished.")

# check file size
#sum(file.info(list.files(mosaic_output_dir, pattern="ndvi", full.names = TRUE))$size)/(1024*1024*1024)
#sum(file.info(list.files(mosaic_output_dir, pattern="evi", full.names = TRUE))$size)/(1024*1024*1024)