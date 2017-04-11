##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to provide a sequence of processing using functions from maiac_processing_functions.R
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-02-09
## Notes: 
##################################################
## Current processing workflow:
## TODO: improve this workflow listing... it's not updated
## 1) get all filenames from a given 8-day period for all available tiles to south america
## 2) filter product names by only the product tiles/dates that have RTLS tiles
## 3) convert MAIACTBRF and MAIACRTLS hdfs to geotif (gdal_translate)
## 4) load all tif files needed
## 5) apply brf normalization to nadir for each date using the 8-day RTLS parameters file, and the eq. from MAIAC documentation: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
## 6) load QA layers and create a mask for each date excluding bad pixels (cloud, etc.)
## 7) apply mask to the brfn of each date
## 8) calc the median of each pixel using the remaining (best) values
## 9) (work in progress) mosaic the resulting median tiles (mosaic_rasters, also from gdal)
## 10) (work in progress) reproject the resulting 8-day composite (gdalwarp) and save as geotif
## 11) clean temporary files
## (repeat to all 8-day composites in the available time series)
## 12) (to do) merge each 8-day composites tif into one tif containing all time series, one big geotif for each band
## obs: the step 3 and 8 is where I'm working on at this momment, the rest is somewhat ready.
##
## Additional notes:
## sdsName = c("sur_refl", "Sigma_BRFn", "Snow_Fraction", "Snow_Grain_Diameter", "Snow_Fit", "Status_QA", "sur_refl_500m", "cosSZA", "cosVZA", "RelAZ", "Scattering_Angle", "Glint_Angle", "SAZ", "VAZ", "Fv", "Fg")
## sdsDataType = c("INT16", "INT16", "INT16", "INT16", "INT16", "UINT16","INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "INT16", "FLOAT32", "FLOAT32")
## sdsParametersName = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
## http://www.ctahr.hawaii.edu/grem/mod13ug/sect0005.html, http://glcf.umd.edu/data/modis/
##################################################

# libraries used on the script
library(raster)  #install.packages("raster")
library(gdalUtils)  #install.packages("gdalUtils")
library(rgdal)  #install.packages("rgdal")
library(foreach)  #install.packages("foreach") # click yes if asked
library(RCurl)  #install.packages("RCurl")
library(doParallel)  #install.packages("doParallel")


# CONFIG ------------------------------------------------------------------

# working directory, the one the functions are in
workDir = "D:/2_Projects/1_Author/4_MAIAC_process/"

# output directory, the one to export the processed tiles
outputDir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/MAIAC_ProcessedTiles/"

# preview directory, the one to to export preview images "png"
previewDir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/MAIAC_PreviewTiles/"

# nan tiles directory, the one where nan tiles are stored, to use in case of non-existant RTLS file for processing
nanTilesDir = "D:/1_Dataset/1_MODIS/1_MCD43A4_SurfRef16/MAIAC_NanTiles/"

# inputDir, the one where the raw files are
inputDir = "D:/h00v01/"

# tile to process
# TODO: one per time is working, more than one not sure
tile = c("h00v01")

# url to download maiac files for south america, in case of needed
maiacFtpUrl = "ftp://maiac@dataportal.nccs.nasa.gov/DataRelease/SouthAmerica/"

# process date range
# TODO: apply this somehow
processFrom = "04-08-2016"
processTo = "11-08-2016"

# product name MAIACTBRF, MAIACABRF, MAIACRTLS
product = c("MAIACTBRF","MAIACABRF")
parameters = "MAIACRTLS"

# enables filtering by Quality Assessment bits (adjacent clouds and stuff), may reduce number of available pixels
enableQAFiltering = FALSE

# enables filtering by Extreme Angles > 80, may reduce number of available pixels
enableExtremeAnglesFiltering = TRUE




# SEQUENCE FOR SEPARATE TILE PROCESSING -------------------------------------------------------------------


# define the output base filename
compositeFilename = "SR_Nadir_8day"
if (length(product)==1) compositeFilename = paste0(compositeFilename,"_",product) else compositeFilename = paste0(compositeFilename,"_MAIACTerraAqua")
if (enableQAFiltering & enableExtremeAnglesFiltering) compositeFilename = paste0(compositeFilename,"_FilterQA-EA") else
  if (enableQAFiltering) compositeFilename = paste0(compositeFilename,"_FilterQA") else
    if (enableExtremeAnglesFiltering) compositeFilename = paste0(compositeFilename,"_FilterEA")

# create matrix of days on each 8-day composite
eightDayMatrix = matrix(sprintf("%03d",1:360),ncol=8,byrow=T)

# create preview directory if it doesnt exist
dir.create(file.path(previewDir), showWarnings = FALSE)

# create nan rasters for every tile in case it is needed
createNanTiles(inputDir, outputDir, tile)

# measure time
start.time <- Sys.time()

#parallel method

# Calculate the number of cores minus 1
no_cores <- detectCores()-1

# Initiate cluster
cl<-makeCluster(no_cores)
registerDoParallel(cl)
#clusterExport(cl, varlist=c("singleHDF2TIF", "inputDir","outputDir"))
clusterEvalQ(cl, library(raster)) # pra passar packages pros workers
clusterEvalQ(cl, library(gdalUtils)) # pra passar packages pros workers
clusterEvalQ(cl, library(rgdal)) # pra passar packages pros workers
clusterEvalQ(cl, library(RCurl)) # pra passar packages pros workers

# Loop year
for (k in 1:17) {
  #k=14
  year = 1999+k
  
  # send variables to cluster
  clusterExport(cl, varlist=as.vector(c(lsf.str(), ls())))
  
  # Loop 8-day composite
  # parallel
  foreach(j = 1:dim(eightDayMatrix)[1]) %dopar% {
  #for (j in 1:dim(eightDayMatrix)[1]) {
    #j=28
    
    # get the day vector
    day = eightDayMatrix[j,]
    
    # check if 8-day tile composite processed file already exist, otherwise just skip to next
    if (checkProcessedTileComposite(compositeFilename, tile, year, day, outputDir))
      return(0)
      #next
    
    # set temporary directory
    tmpDir = paste0("tmp",year,day[8],"/")
    
    # create temporary directory
    dir.create(file.path(outputDir, tmpDir), showWarnings = FALSE)
    
    # get filenames of each 8-day product and parameters files
    fnameProduct = getFilenames(product, inputDir, tile, year, day)
    fnameParameters = getFilenames(parameters, inputDir, tile, year, day)
    
    # test if fnameProduct is different than 0
    if (length(fnameProduct)==0) {
      unlink(file.path(outputDir, tmpDir), recursive=TRUE)
      return(0)
    }
    
    # filter product names by only the product tiles/dates that have RTLS tiles
    fnameProduct = filterProductTilesbyRTLSTiles(fnameProduct, fnameParameters, outputDir)
    
    # convert the files from hdf to tif, <NO PARALELL>
    # time no parallel: 57 min (366 files)
    # time parallel: estimado 60% do tempo 34 min (366 files), deu 33% -> 19 min (366 files)
    convertHDF2TIF(fnameProduct, inputDir, outputDir, tmpDir, maiacFtpUrl)
    convertHDF2TIF(fnameParameters, inputDir, outputDir, tmpDir, maiacFtpUrl)
    
    # adjust filenames to remove the folder
    if (nchar(fnameProduct[1]) == 37) {
      fnameProduct = substr(fnameProduct,6,37)
      fnameParameters = substr(fnameParameters,6,37)
    }
    
    # check if the RTLS exists for this tile, if false return a empty (nan) raster as the processed i tile and go to the next iteration
    if (checkRTLSTile(tile, fnameParameters, inputDir, outputDir, tmpDir)) {
      unlink(file.path(outputDir, tmpDir), recursive=TRUE)
      return(0)
      #next
    }
    
    # load files needed for BRF normalization
    brfBrick = loadFiles(fnameProduct, outputDir, tmpDir, "sur_refl")
    brfFv = loadFiles(fnameProduct, outputDir, tmpDir, "Fv")
    brfFg = loadFiles(fnameProduct, outputDir, tmpDir, "Fg")
    rtlsKiso = loadFiles(fnameParameters, outputDir, tmpDir, "Kiso")
    rtlsKvol = loadFiles(fnameParameters, outputDir, tmpDir, "Kvol")
    rtlsKgeo = loadFiles(fnameParameters, outputDir, tmpDir, "Kgeo")
    
    # filter bad values or fill values
    # Fv and Fg: -99999 is a fill value, should be removed
    brfFv = filterBadValues(brfFv, equal=-99999)
    brfFg = filterBadValues(brfFg, equal=-99999)
    #brfBrick = filterBadValues(brfBrick, min=0, max=1) # not needed i think, if you filter the final result it's ok
    
    # convert BRF to Nadir
    nadirBRFBrick = convertBRFNadir(brfBrick, brfFv, brfFg, rtlsKiso, rtlsKvol, rtlsKgeo)
    rm(list = c("brfBrick", "brfFv", "brfFg", "rtlsKiso", "rtlsKvol", "rtlsKgeo"))
    
    # create and apply QA filter mask following a set of rules in function filterQuality()
    # remove pixels such as: possibly cloud, cloud adjacent, cloud shadow, etc.
    if (enableQAFiltering) {
      qaBrick = loadFiles(fnameProduct, outputDir, tmpDir, "Status_QA")
      qaMask = createBatchQAMask(qaBrick)
      nadirBRFBrick = applyListMask(nadirBRFBrick, qaMask)
      rm(list = c("qaBrick","qaMask"))
    }
    
    # create and apply a mask based on extreme sun angles >80
    if (enableExtremeAnglesFiltering) {
      nadirBRFBrick = filterEA(nadirBRFBrick, fnameProduct, outputDir, tmpDir)
    }
    
    # put the bands together so its easier to calc the median
    nadirBRFBrickPerBand = reorderBrickPerBand(nadirBRFBrick)
    rm(list = c("nadirBRFBrick"))
    
    # create median value BRF 8-day composite from the BRF brick and masked QA brick
    medianBRF = calcMedianBRF8day(nadirBRFBrickPerBand)
    rm(list = c("nadirBRFBrickPerBand"))
    
    # plot brfnadir to file to verify it later
    png(filename=paste0(previewDir,"fig_",compositeFilename,"_",year,day[8],".png"), type="cairo", units="cm", width=15, height=15, pointsize=10, res=300)
    par(oma=c(4,4,4,4))
    plot(medianBRF)
    dev.off()
    
    # save the tile median in the composite directory
    saveProcessedTileComposite(medianBRF, outputDir, compositeFilename, tile, year, day)
    rm(list = c("medianBRF"))
    
    # delete temporary directory
    unlink(file.path(outputDir, tmpDir), recursive=TRUE)

    # End Loop 8-day composite
    return(1)
  }
  
  
  # End of loop year
}

# stop cluster
stopCluster(cl)

# message
print("All tile composites processed. Good job!")

# measure time
end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken

# TO DO: MAKE CODE TO MOSAIC ALL TILES AT ONCE
#
# # list and load tiles
# processedList = list.files(paste0(outputDir,tmpDir), pattern = "Processed")
# 
# # mosaic the tiles
# medianBRFMosaic = mosaicTilesAndSave(processedList, outputDir, tmpDir, year, day)
# 
# # plot brfnadir to file to verify it later
# png(filename=paste0(previewDir,"fig_",compositeFilename,"_",year,day[8],".png"), type="cairo", units="cm", width=15, height=15, pointsize=10, res=300)
# par(oma=c(4,4,4,4))
# plot(medianBRFMosaic)
# dev.off()




