##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to provide functions for maiac_processing.R
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-02-09
## Notes: 
##################################################

# function to check for a processed composite, if it does exist just go to the next iteration
checkProcessedComposite = function(compositeFilename, year, day, outputDir) {
  if (file.exists(paste0(outputDir,compositeFilename,"_",year,day[8],".tif"))) {
    # message
    print(paste0("Composite ",paste0(compositeFilename,"_",year,day[8],".tif")," is already processed, going to the next iteration."))
    
    # go to the next iteration
    return(TRUE)
  }
  return(FALSE)
}

# function to check for a processed tile composite, if it does exist just go to the next iteration
checkProcessedTileComposite = function(compositeFilename, tile, year, day, outputDir) {
  if (file.exists(paste0(outputDir,compositeFilename,".",tile,".",year,day[8],".tif"))) {
    # message
    print(paste0("Tile composite ",paste0(compositeFilename,".",tile,".",year,day[8],".tif")," is already processed, going to the next iteration."))
    
    # go to the next iteration
    return(TRUE)
  }
  return(FALSE)
}

# function to create nan tiles, to use in case the time series dont have data for one tile on a given date
createNanTiles = function(inputDir,outputDir,tile) {
  # create temporary directory
  dir.create(file.path(outputDir, "tmpnanfiles/"), showWarnings = FALSE)
  
  for (i in 1:length(tile)) {
    # check if nan tile i already exists
    if (!file.exists(paste0("NanTiles/nantile.",tile[i],".tif"))) {
      fname = paste0("MAIACLatlon",".",tile[i],".hdf")
      
      # convert
      gdal_translate(paste0(inputDir,fname), dst_dataset = paste0(outputDir,"tmpnanfiles/",fname,".tif"), verbose=F, sds=TRUE)
      
      # open and assign nan
      r = raster(paste0(outputDir,"tmpnanfiles/",fname,"_1.tif"))
      r[]=NaN
      
      # save
      writeRaster(r, filename=paste0("NanTiles/","nantile.",tile[i],".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    }
  }
  
  # delete tmp
  unlink(file.path(outputDir, "tmpnanfiles/"), recursive=TRUE)
}

# function to get filenames of each 8-day product or parameters files from a product "x", from a input directory "inputDir", of a respective "tile", "year" and day vector "day"
getFilenames = function(x, inputDir, tile, year, day) {
  combinations = expand.grid(x,paste0(".",tile,"."),year,sprintf("%03s",day))
  combinations = paste0(combinations$Var1, combinations$Var2, combinations$Var3, combinations$Var4)
  tmp = list.files(inputDir, pattern=paste(combinations,collapse="|"), recursive=T)
  return(tmp)
}

# function to check for existing tiles in RTLS file list and filter them from the BRF products file list
filterProductTilesbyRTLSTiles = function(x, y, outputDir) {
  #x=fnameProduct
  #y=fnameParameters
  
  # check which tiles exist in RTLS
  existingTiles = substr(y,11,16)
  
  # check which tiles doesn't exist in RTLS but exist in product
  missingTiles = unique(substr(x[-grep(paste0(existingTiles, collapse="|"),x)],11,16))
  
  # report the missing RTLS tiles
  if (length(missingTiles) > 0) {
    missingDay = substr(y[[1]],22,24)
    missingYear = substr(y[[1]],18,21)
    line = paste(missingYear, missingDay, missingTiles, sep=",")
    write(line,file=paste0(outputDir,"missingRTLStiles.txt"),append=TRUE)
  }
  
  # return products that have RTLS tiles
  return(x[grep(paste0(existingTiles, collapse="|"),x)])
}

# function to download a missing file while is processing
downloadMissingFile = function(x, inputDir, maiacFtpUrl) {
  #x = x[i]
  # example file: "MAIACABRF.h01v01.20132171720.hdf"
  # example: ftp://maiac@dataportal.nccs.nasa.gov/DataRelease/SouthAmerica/h00v00/2000/MAIACRTLS.h00v00.2000096.hdf
  
  # message
  print(paste0("Trying to download the missing file: ",x))
  
  # file url
  tile = substr(x,11,16)
  year = substr(x,18,21)
  fileUrl = paste0(maiacFtpUrl,tile,"/",year,"/",x)
  
  # download the file
  tmp = try(getBinaryURL(fileUrl, verbose = F, username="maiac"))
  
  # download loop!
  w=0
  while(w <= 15 && class(tmp) == "try-error") {
    print(paste0("getBinaryURL error, trying again in ",10+w," seconds... try number ",w+1))
    Sys.sleep(10+w)
    tmp = try(getBinaryURL(fileUrl, verbose = F, username="maiac"))
    closeAllConnections()
    w=w+1
  }
  
  # save the file on the disk
  if (class(tmp) == "try-error") {
    print(paste0("Could not download the missing file: ",x))
  } else {
    print(paste0("Download sucess: ",x))
    writeBin(tmp, con=paste0(inputDir,x))
  }
}

# function to convert "x" files .HDF to .TIF from an input directory "inputDir" to a temporary output directory "outputDir"/tmp
convertHDF2TIF = function(x, inputDir, outputDir, tmpDir, maiacFtpUrl) {
  #x = fnameProduct
  for(i in 1:length(x)) {
    #i=1
    # adjust output filename in case the product name has folder in the beggining
    x1 = x[i]
    if (nchar(x1) == 37)
      x1 = substr(x1,6,37)
    
    # check if x[i] converted tif file exists
    # if it does, just throw some message
    # if it doesnt, try to convert, if it works nice, if it doesnt try to download the tile and process it
    if (!file.exists(paste0(outputDir,tmpDir,x1,"_01.tif")) & !file.exists(paste0(outputDir,tmpDir,x1,"_1.tif"))) {
      # message
      print(paste0("Converting HDF to TIF file ",i," from ",length(x)," -> ",x[i]))
      
      # sds=T converts all files, while sd_index is only 1 file
      # dont need to specify data type for each SDS, it detects by itself
      gdal_translate(paste0(inputDir,x[i]), dst_dataset = paste0(outputDir,tmpDir,x1,".tif"), verbose=F, sds=TRUE)
      
      # check if file exists after converting
      if (!file.exists(paste0(outputDir,tmpDir,x1,"_01.tif")) & !file.exists(paste0(outputDir,tmpDir,x1,"_1.tif"))) {
        # if it doesn't, it means that the HDF is corrupted and a download is needed
        w=0
        while((!file.exists(paste0(outputDir,tmpDir,x1,"_01.tif")) & !file.exists(paste0(outputDir,tmpDir,x1,"_1.tif"))) & w<5) {
          # message
          print(paste0("Error while converting file ",i," from ",length(x)," -> ",x[i]))
          
          # download the missing file
          downloadMissingFile(x1,inputDir, maiacFtpUrl)
          
          # try to convert again
          gdal_translate(paste0(inputDir,x1), dst_dataset = paste0(outputDir,tmpDir,x1,".tif"), verbose=F, sds=TRUE)
          
          # count
          w=w+1 
        }
        
        # check if the file was extracted
        if (file.exists(paste0(outputDir,tmpDir,x1,"_01.tif")) & file.exists(paste0(outputDir,tmpDir,x1,"_1.tif"))) {
          print(paste0("File ",x1," was downloaded and extracted with sucess. Error avoided (i hope), oh yeah!"))
        }
        
      }
      
    } else {
      print(paste0("File ",i," from ",length(x)," is already converted to tif -> ",x1))
    }
  }
  
  # # parallel method
  # require(foreach)
  # require(doParallel)
  # 
  # # Calculate the number of cores
  # no_cores <- detectCores()
  # 
  # # Initiate cluster
  # cl<-makeCluster(no_cores)
  # registerDoParallel(cl)
  # clusterExport(cl, varlist=c("singleHDF2TIF", "inputDir","outputDir"))
  # clusterEvalQ(cl, library(gdalUtils)) # pra passar packages pros workers
  # 
  # foreach(i=1:length(x)) %dopar% {
  #   singleHDF2TIF(x[i],inputDir,outputDir,tmpDir)
  # }
  # 
  # stopCluster(cl)
  
}

# function to check for a processed tile, if it does exist just go to the next iteration
checkProcessedTile = function(x, inputDir, outputDir, tmpDir) {
  #x=tile[i]
  if (file.exists(paste0(outputDir,tmpDir,"Processed.",x,".tif"))) {
    # message
    print(paste0("Tile ",x," is already processed, going to the next iteration."))
    
    # go to the next iteration
    return(TRUE)
  }
  return(FALSE)
}

# function to check for RTLS tile, and if it doesn't exist, create a "processed tile" with nan values
checkRTLSTile = function(x, y, inputDir, outputDir, tmpDir) {
  #x=tile[i]
  #y=fnameParameters
  
  # check if x exist in y
  existingTiles = substr(y,11,16)
  idx = grep(x,existingTiles)
  
  # if RTLS tile does not exist return the nan tile and go to the next iteration
  if (length(idx)==0) {
    # open the nan tile
    r = stack(paste0(inputDir,"nantile.",x,".tif"))
    
    # save the nan tile as the processed tile
    writeRaster(r, filename=paste0(outputDir,tmpDir,"Processed.",tile[i],".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    
    # message
    print(paste0("Tile ",tile[i]," did not have a RTLS file, so a nan tile was created, going to the next iteration."))
    
    # go to the next iteration
    return(TRUE)
  }
  return(FALSE)
}

# function to load files of a specific type from the temporary output folder "outputDir"/tmp
loadFiles = function(x, outputDir, tmpDir, type) {
  # name and order vector of the subdatasets
  sdsName = c("sur_refl", "Sigma_BRFn", "Snow_Fraction", "Snow_Grain_Diameter", "Snow_Fit", "Status_QA", "sur_refl_500m", "cosSZA", "cosVZA", "RelAZ", "Scattering_Angle", "Glint_Angle", "SAZ", "VAZ", "Fv", "Fg")
  sdsParametersName = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
  
  # identify the type
  typeNumber = sprintf("%02d", grep(paste0("^", type, "$"), sdsName))
  if (length(typeNumber)==0)
    typeNumber = sprintf("%01d", grep(paste0("^", type, "$"), sdsParametersName))
  if (length(typeNumber)==0) {
    stop(paste0("Can't find file type to open: ",type))
  }
  
  # list of bricks
  y = list()
  
  # loop though files and load the files
  for(i in 1:length(x)) {
    y[[i]] = brick(paste0(outputDir,tmpDir,x[i],"_",typeNumber,".tif"))
  }
  
  # return
  return(y)
}

# function to filter bad values (outside 0 and 1)
filterBadValues = function(x, minArg, maxArg, equal) {
  # load package
  #require(snow)
  
  filterEqual = function(x) {
    x[x==equal]=NA
    return(x)
  }
  
  filterMinMax = function(x) {
    x[x<minArg | x>maxArg]=NA
    return(x)
  }
  
  if (hasArg(equal)) {
    myFun =  filterEqual
  } else if (hasArg(minArg) & hasArg(maxArg)) {
    myFun =  filterMinMax  
  } else {
    print("Couldn't find min, max or equal argument, returning the variable without filtering.")
    return(x)
  }
  
  # apply the function
  # for function method
  if (is.list(x)) {
    for (i in 1:length(x)) {
      x[[i]] = myFun(x[[i]])
    }
  } else
    x = myFun(x)
  #
  
  
  # if (method=="lapply") {
  #   x=lapply(x,FUN = filterBad)
  #   y=lapply(x,FUN = filterBad)
  # }
  # 
  # if (method=="for") {
  #   # no parallel
  #   # for each date
  #   for (i in 1:length(x)) {
  #     x[[i]][x[[i]]>1 | x[[i]]<0] = NA
  #   }
  # }
  
  return(x)
}

# function to convert brf to brfn
# to do: arquivo vira float depois da covnersao, transformar em outro formato? (ex. int2s)
# transf para int2s parece que ferra os valores
# band values are calculated ok, tested calculating one band separatedely and compared to the batch convert
convertBRFNadir = function(BRF, FV, FG, kL, kV, kG) {
  # brfBrick (12 bandas por data, 1km)
  # brfFv (1 por data, 5km)
  # brfFg (1 por data, 5km)
  # rtlsKiso (8 bandas, 1km)
  # rtlsKvol (8 bandas, 1km)
  # rtlsKgeo (8 bandas, 1km)
  
  # BRF = brfBrick
  # FV = brfFv
  # FG = brfFg
  # kL = rtlsKiso
  # kV = rtlsKvol
  # kG = rtlsKgeo
  
  # get RTLS information for the given tile of product
  tileVec = vector()
  for (i in 1:length(kL)) {
    tileVec[i] = substr(names(kL[[i]])[[1]],11,16)
  }
  
  # list to put the results
  BRFn = list()
  
  # for each date
  #i=1
  for (i in 1:length(BRF)) {
    # message
    print(paste0("normalizing brf iteration ",i," from ",length(BRF)))
    
    # set parameters, interpolate the 5km to 1 km by nearest neighbor
    BRFi = subset(BRF[[i]],1:8)
    FVi = disaggregate(FV[[i]], fact=c(5,5))
    FGi = disaggregate(FG[[i]], fact=c(5,5))
    
    # identify which tile is the given i data and set the parameters to the tile
    idx = substr(names(FGi),11,16)
    kLi = kL[[grep(idx,tileVec)]]
    kVi = kV[[grep(idx,tileVec)]]
    kGi = kG[[grep(idx,tileVec)]]
    
    # calculate brf nadir
    # original do documento: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
    BRFn[[i]] = BRFi * (kLi - (0.04578*kVi) - (1.10003*kGi))/(kLi + (FVi*kVi) + (FGi*kGi))
  }
  
  # constrain the BRFn between 0 and 1 - possible results
  BRFn = filterBadValues(BRFn, min=0, max=1)
  
  return(BRFn)
  
}

# compute quality based on occuring QA on the raster
# https://stevemosher.wordpress.com/2012/12/05/modis-qc-bits/
# https://lpdaac.usgs.gov/sites/default/files/public/modis/docs/MODIS_LP_QA_Tutorial-1b.pdf
# dataspec.doc
computeQuality= function(x) {
  # interpreting all possible QA
  qaDF <- data.frame(Integer_Value = x,
                     surface = NA, #12-15
                     reserved = NA, #10-11
                     algoinit = NA, #9
                     aotlevel = NA, #8
                     adjacency = NA, #5-7
                     landcover = NA, #3-4
                     cloud = NA #0-2
  )
  
  for(i in 1:length(x)){
    # Convert value to bit
    AsInt <- as.integer(intToBits(qaDF$Integer_Value[i])[1:16])
    
    # Flip the vector
    AsInt = AsInt[16:1]
    
    # Bit12to15, surface change mask
    qaDF[i,2] = paste0(AsInt[1:4], collapse="")
    
    # Bit10-11, reserved
    qaDF[i,3] = paste0(AsInt[5:6], collapse="")
    
    # Bit9, algorithm initialize status
    qaDF[i,4] = paste0(AsInt[7], collapse="")
    
    # Bit8, aot level
    qaDF[i,5] = paste0(AsInt[8], collapse="")
    
    # Bit5to7, adjacency mask
    qaDF[i,6] = paste0(AsInt[9:11], collapse="")
    
    # Bit3to4, land water snow/ice mask
    qaDF[i,7] = paste0(AsInt[12:13], collapse="")
    
    # Bit0to2, cloud mask
    qaDF[i,8] = paste0(AsInt[14:16], collapse="")
  }
  
  return(qaDF)
}

# filter quality table, the things to filter out the image
filterQuality= function(x) {
  #x = qaDF
  
  # aotlevel5
  filterList = c(5, "1") # AOT is high (> 0.6) or undefined
  
  # filter adjacency6
  filterList = rbind(filterList,c(6, "001")) # Adjacent to cloud
  filterList = rbind(filterList,c(6, "010")) # Surrounded  by more than 8 cloudy pixels
  filterList = rbind(filterList,c(6, "011")) # Single cloudy pixel
  
  # filter cloud8
  filterList = rbind(filterList,c(8, "000")) # 000 ---  Undefined
  filterList = rbind(filterList,c(8, "010")) # 010 --- Possibly Cloudy (detected by AOT filter)
  filterList = rbind(filterList,c(8, "011")) # 011 --- Cloudy  (detected by cloud mask algorithm)
  filterList = rbind(filterList,c(8, "101")) # 101 -- - Cloud Shadow
  #filterList = rbind(filterList,c(8, "110")) # 110 --- hot spot of fire
  filterList = rbind(filterList,c(8, "111")) # 111 --- Water Sediments
  
  # remove filterList from the quality data frame (x)
  for (i in 1:dim(filterList)[1]) {
    if (dim(x)[1] > 0) {
      idx = which(x[as.numeric(filterList[i,1])]==filterList[i,2])
      if (length(idx) > 0)
        x = x[-idx,]
    }
  }
  
  return(x)
}

# function to create a single qa mask based on a qa raster
createSingleQAMask = function(x) {
  #x = qaBrick[[5]]
  
  # get unique qa values
  uniqueQA = as.numeric(unique(x))
  
  # remove nan's
  uniqueQA = uniqueQA[!is.na(uniqueQA)]
  
  if (length(uniqueQA) > 0) {
    # compute qa dataframe with all possible quality combinations in the image
    qaDF = computeQuality(uniqueQA)
    
    # filter the qa dataframe with a set of determined rules excluding the bad ones
    qaDFFiltered = filterQuality(qaDF)
    
    # create the mask using the remaining QA, rest of values become NaN
    if (dim(qaDFFiltered)[1] > 0)
      x = subs(x, data.frame(id=qaDFFiltered$Integer_Value, v=rep(1,length(qaDFFiltered$Integer_Value))))
    else
      x[]=NaN
    
  } else {
    x[]=NaN
  }
  
  # return
  return(x)
}

# function to create a qa mask based on a qa raster brick "x"
createBatchQAMask = function(x) {
  # initiate list
  y = list()
  
  # loop throught the brick
  for (i in 1:length(x)) {
    # message
    print(paste0("Creating qa mask file ",i," from ",length(x)))
    
    # add to the brick
    y[[i]] = createSingleQAMask(x[[i]])
  }
  
  # return
  return(y)
}

# function to apply the QA mask over a raster brick of the same size
applyListMask = function(x, y) {
  # initiate list
  z = list()
  
  # loop throught the "x" brick
  for (i in 1:length(x)) {
    # message
    print(paste0("Applying mask in file ",i," from ",length(x)))
    
    # apply mask
    z[[i]] = mask(x[[i]], y[[i]])
  }
  
  # return
  return(z)
}

# function to create extreme azimutal angle >80 mask and apply to BRF
filterEA = function(x, fnameProduct, outputDir, tmpDir) {
  #cosSZA = loadFiles(fnameProduct, outputDir, tmpDir, "cosSZA")
  #cosSZA = filterBadValues(cosSZA, min=-10000, max=10000)
  #acosSZA = lapply(cosSZA,FUN = function(x) calc(x, fun = acos))
  SAZ = loadFiles(fnameProduct, outputDir, tmpDir, "SAZ")
  SAZ = filterBadValues(SAZ, min=-10000, max=80)
  
  # function to create the mask
  notna2one = function(x) {
    x[!is.na(x)]=1
    return(x)
  }
  
  # create the mask
  SAZMask = lapply(SAZ, FUN = notna2one)
  
  # interpolate the mask
  SAZMask = lapply(SAZMask, FUN = disaggregate, fact=c(5,5))
  
  # apply mask
  x = applyListMask(x, SAZMask)
  
  # return
  return(x)
  
}

# function to reorder the brick list per band instead of per date
reorderBrickPerBand = function(x) {
  # new list
  y = list()
  
  # loop through bands
  for (j in 1:nlayers(x[[1]])) {
    # message
    print(paste0("Re-ordering brick per band ",j," from ",nlayers(x[[1]])))
    
    # create brick
    y[[j]] = brick()
    
    # loop through dates filling the y
    for (i in 1:length(x)) {
      y[[j]] = addLayer(y[[j]],x[[i]][[j]])
    }
  }
  
  # return
  return(y)
}

# function to merge the data from 8-day time span into one composite file using the median value
calcMedianBRF8day = function(x) {
  #x=nadirBRFBrickMaskedPerBand
  
  # function to calculate median and return the nearest value to the median
  # this prevents the algorithm calculating the mean of "two median values" if you dont have one single median, example:
  # values = c(0.0338, 0.0172, 0.0368, NA, 0.0230, NA, NA, NA, NA, NA, NA)
  # median equlals to 0.0284, a value that doesn't exist, it's a mean of 0.0338 and 0.0230
  # now, the difference between values and median = c(0.0054, 0.0112, 0.0084, NA, 0.0054, NA, NA, NA, NA, NA, NA)
  # an existing median can be either 0.0338 or 0.023, but i choose arbitrarily to get the first value it find in the vector
  medianIndex = function(x) which.min(abs(x - median(x, na.rm=T)))
  
  # function to calc the median "m" of a raster brick and return the index "idx" of the raster corresponding to that median value, also get how many observations that pixel had
  medianAndN = function(x) {
    value = as.numeric(x)
    if (sum(is.na(value))==length(value)) {
      #c(NA,NA,NA) # if 3 outputs
      c(NA,NA)  # if 2 outputs
    } else {
      idx = medianIndex(value)
      m = value[idx]
      n = sum(!is.na(value))
      
      # return the data (m), index of image take was taken (idx), and number of good pixels (n)
      #c(m, idx, n)
      c(m, n)
    }
  }
  
  # list to put the results
  y = list()
  
  # load package
  #require(snow)
  
  # for each band
  for (i in 1:length(x)) {
    # message
    print(paste0("Calculating median per band ",i," from ",length(x)))
    
    # calc median
    # beginCluster(8)
    # y[[i]] = clusterR(x[[i]], calc, args=list(fun=medianAndN), export=c("medianIndex"))
    # endCluster()
    y[[i]] = calc(x[[i]], fun=medianAndN)
  }
  
  # only bands
  y=brick(c(lapply(y,FUN=subset, subset=1),y[[1]][[2]]))
  names(y)=c("band1","band2","band3","band4","band5","band6","band7","band8","samples")
  
  return(y)
}

# function to write the processed file to disk while applying a factor of 10000 to bands 1-8 to reduce disk space usage
saveProcessedTileComposite = function(medianBRF, outputDir, compositeFilename, tile, year, day) {
  # factors for each band
  factors = c(10000,10000,10000,10000,10000,10000,10000,10000,1)
  
  # apply factors
  b=brick(lapply(c(1:9),FUN=function(x) round(unstack(medianBRF)[[x]]*factors[x],0)))
  
  # write to file
  writeRaster(b, filename=paste0(outputDir,compositeFilename,".",tile,".",year, day[8],".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
  #writeRaster(medianBRF, filename=paste0(outputDir,tmpDir,"Processed.",tile,".tif"), format="GTiff", overwrite=TRUE)
  
  # message
  print(paste0("Tile composite was saved: ",compositeFilename,".",tile,".",year, day[8],".tif"))
}

# function to write the processed file to disk while applying a factor of 10000 to bands 1-8 to reduce disk space usage
saveProcessedTile = function(medianBRF, outputDir, tmpDir, tile) {
  # factors for each band
  factors = c(10000,10000,10000,10000,10000,10000,10000,10000,1)
  
  # apply factors
  b=brick(lapply(c(1:9),FUN=function(x) round(unstack(medianBRF)[[x]]*factors[x],0)))
  
  # write to file
  writeRaster(b, filename=paste0(outputDir,tmpDir,"Processed.",tile,".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
  #writeRaster(medianBRF, filename=paste0(outputDir,tmpDir,"Processed.",tile,".tif"), format="GTiff", overwrite=TRUE)
  
  # message
  print(paste0("Tile was saved: ",tile))
}

# function to mosaic given tiles from a list of bricks "x"
mosaicTilesAndSave = function(x, outputDir, tmpDir, year, day) {
  #x = processedList
  
  # message
  print("Mosaicking the tiles...")
  
  # mosaic and save files
  m=mosaic_rasters(paste0(outputDir,tmpDir,x), dst_dataset = paste0(outputDir,compositeFilename,"_",year,day[8],".tif"), verbose=F, output_Raster = TRUE, ot="Int16", co = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"))
  
  # return
  return(m)
}

# function to reproject composites
reprojectComposite = function(x, outputDir, tmpDir, year, day, outputRaster) {
  
  # South America region has Sinusoid projection, with central longitude 58W.
  # projecao lat/lon: "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
  # projecao original do maiac: "+proj=sinu +lon_0=-3323155211.758775 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
  # projecao ajustada do maiac com longitude central da SouthAmerica -58W (sugestão Yujie Wang): "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
  gdalwarp(paste0(outputDir,compositeFilename,"_",year,day[8],".tif"),
           dstfile = paste0(outputDir,compositeFilename,"_LatLon_",year,day[8],".tif"),
           t_srs = "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0",
           s_srs = "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs",
           ot="Int16",
           tr=c(0.009107388, 0.009107388), # 0.009107388 is the resolution from the old series
           wo = "INIT_DEST = NO_DATA",
           co = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"),
           r = "near",
           verbose=F,
           overwrite = T)
  
}
