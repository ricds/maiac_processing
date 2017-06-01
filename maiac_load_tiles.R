##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: load MAIAC files like a time series
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-05-30
##################################################
## 
##################################################

# packages

# clear all
rm(list = ls())

# input directory
input_dir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/4_MAIAC_Ricardo_Bambu/"

# band names
band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")

# list the files by band
file_list_bands = lapply(band_names, function(x) list.files(input_dir, full.names = TRUE, pattern = x))

# load time series
band2 = stack(file_list_bands[[2]])

# set coordinate to extract
coord = matrix(c(-70,-10), nrow = 1, ncol = 2, byrow = T)

# extract values from test coordinates
coord_ts = extract(x=band2, y=coord)

# plot test coordinates time series
plot(as.numeric(coord_ts), type="l")
