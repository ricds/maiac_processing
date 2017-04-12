##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to provide functions for maiac_processing.R
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-02-09
## Notes: https://www.r-bloggers.com/r-style-guide/, http://adv-r.had.co.nz/Style.html, https://google.github.io/styleguide/Rguide.xml
##################################################

# function to check if given composite exists in output_dir, if it does return TRUE, otherwise return FALSE
IsCompositeProcessed = function(composite_fname, year, day, output_dir) {
  # set escape variable default
  result = FALSE
  
  # check if composite exists
  if (file.exists(paste0(output_dir,composite_fname,"_",year,day[8],".tif"))) {
    # message
    print(paste0("Composite ",paste0(composite_fname,"_",year,day[8],".tif")," is already processed, going to the next iteration."))
    
    # go to the next iteration
    result = TRUE
  }
  
  # return
  return(result)
}

# function to check for a processed tile composite, if it does exist just go to the next iteration
IsTileCompositeProcessed = function(composite_fname, tile, year, day, output_dir) {
  # set escape variable default
  result = FALSE
  
  # check if tile composite exists
  if (file.exists(paste0(output_dir,composite_fname,".",tile,".",year,day[8],".tif"))) {
    # message
    print(paste0("Tile composite ",paste0(composite_fname,".",tile,".",year,day[8],".tif")," is already processed, going to the next iteration."))
    
    # go to the next iteration
    result = TRUE
  }
  return(result)
}

# function to check for a processed tile, if it does return TRUE, otherwise return FALSE
isTileProcessed = function(tile, input_dir, output_dir, tmp_dir) {
  # set escape variable default
  result = FALSE
  
  #x=tile[i]
  if (file.exists(paste0(output_dir,tmp_dir,"Processed.",x,".tif"))) {
    # message
    print(paste0("Tile ",x," is already processed, going to the next iteration."))
    
    # go to the next iteration
    result = TRUE
  }
  return(result)
}

# function to check if RTLS tile is available, if it does return TRUE, if it doesn't exist, create a "processed tile" with nan values
IsDataAvailable = function(fname, tile, year, day, nan_tiles_dir, output_dir, obs) {
  # set escape variable default
  result = FALSE
  
  # if the variable is empty
  if (length(fname)==0) {
    # log the bad file
    line = paste(obs, year, day[8], tile, sep=",")
    write(line, file=paste0(output_dir,"missing_files.txt"), append=TRUE)
    
    # load a brick of 9 nan bands (equivalent to band1-8 and number of pixels)
    b = brick(lapply(c(1:9), FUN=function(x) raster(paste0(nan_tiles_dir,"nantile.",tile,".tif"))))

    # save the nan tile as the processed tile
    writeRaster(b, filename=paste0(output_dir, composite_fname, ".", tile, ".", year, day[8], ".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    
    # message
    #print(paste0("Tile ",tile," did not have a RTLS file, so a nan tile was used instead, going to the next iteration."))
    print(paste0("Couldn't find ", fname, " for tile ", tile, ", year ", year, ", and day ", day[8],". Going to next iteration..."))
    
    # go to the next iteration
    result = TRUE
  }
  return(result)
}

# function to create nan tiles, to use in case the time series dont have data for one tile on a given date
CreateNanTiles = function(input_dir, latlon_tiles_dir, nan_tiles_dir, tile) {
  # check if nan tile already exists
  if (!file.exists(paste0(nan_tiles_dir,"nantile",".",tile,".tif"))) {
    # create tmpnanfiles directory
    dir.create(file.path(nan_tiles_dir, "tmpnanfiles/"), showWarnings = FALSE)
    
    # set latlon fname
    fname = paste0("MAIACLatlon",".",tile,".hdf")
    
    # convert
    gdal_translate(paste0(latlon_tiles_dir,fname), dst_dataset = paste0(nan_tiles_dir,"tmpnanfiles/",fname,".tif"), verbose=F, sds=TRUE)
    
    # open and assign nan
    r = raster(paste0(nan_tiles_dir,"tmpnanfiles/",fname,"_1.tif"))
    r[]=NaN
    
    # save
    writeRaster(r, filename=paste0(nan_tiles_dir,"nantile.",tile,".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    
    # delete tmpnanfiles directory
    unlink(file.path(nan_tiles_dir, "tmpnanfiles/"), recursive=TRUE)
  }
}

# function to get filenames of each 8-day product or parameters files from a product "x", from a input directory "input_dir", of a respective "tile", "year" and day vector "day"
GetFilenameVec = function(product, input_dir, tile, year, day) {
  # create combinatios of product, tile, year and day
  combinations = expand.grid(product, paste0(".",tile,"."), year, sprintf("%03s",day))
  
  # merge the combinations
  combinations = paste0(combinations$Var1, combinations$Var2, combinations$Var3, combinations$Var4)
  
  # prepare an outut
  result = list.files(input_dir, pattern=paste(combinations,collapse="|"), recursive=T)
  
  # return the output
  return(result)
}

# function to check for existing RTLS files and filter the BRF products by it, and report the missing RTLS
# TODO: remove this function? this function seems to be obsolete now that we process by tile
FilterProductTilesbyRTLSTiles = function(product_fname, parameter_fname, output_dir) {
  #x=product_fname
  #y=parameter_fname
  
  # check which tiles exist in RTLS
  existing_tiles = substr(parameter_fname, 11, 16)
  
  # find the string MAIAC inside the product string
  pos = gregexpr('MAIAC', parameter_fname)[[1]]
  if (any(pos) > 0) {
    existing_tiles = substr(parameter_fname,pos[length(pos)]+10,pos[length(pos)]+15)
  } else {
    stop(paste0("Problem in FilterProductTilesbyRTLSTiles function, can't find MAIAC in the fname."))
  } 

  # check which tiles doesn't exist in RTLS but exist in product
  missing_tiles = unique(substr(product_fname[-grep(paste0(existing_tiles, collapse="|"), product_fname)],11,16))
  
  # report the missing RTLS tiles
  if (length(missing_tiles) > 0) {
    missing_day = substr(parameter_fname[[1]],22,24)
    missing_year = substr(parameter_fname[[1]],18,21)
    line = paste(missing_year, missing_day, missing_tiles, sep=",")
    write(line,file=paste0(output_dir,"missingRTLStiles.txt"),append=TRUE)
  }
  
  # return products that have RTLS tiles
  return(product_fname[grep(paste0(existing_tiles, collapse="|"),product_fname)])
}

# function to download a missing file while is processing
DownloadMissingFile = function(fname, directory, maiac_ftp_url) {
  #fname = x[i]
  # example file: "MAIACABRF.h01v01.20132171720.hdf"
  # example: ftp://maiac@dataportal.nccs.nasa.gov/DataRelease/SouthAmerica/h00v00/2000/MAIACRTLS.h00v00.2000096.hdf
  
  # message
  print(paste0("Trying to download the missing file: ",fname))
  
  # file url
  tile = substr(fname,11,16)
  year = substr(fname,18,21)
  file_url = paste0(maiac_ftp_url,tile,"/",year,"/",fname)
  
  # download the file
  tmp_file = try(getBinaryURL(file_url, verbose = F, username="maiac"))
  
  # download loop!
  w=0
  while(w <= 15 && class(tmp_file) == "try-error") {
    print(paste0("getBinaryURL error, trying again in ",10+w," seconds... try number ",w+1))
    Sys.sleep(10+w)
    tmp_file = try(getBinaryURL(file_url, verbose = F, username="maiac"))
    closeAllConnections()
    w=w+1
  }
  
  # save the file on the disk
  if (class(tmp_file) == "try-error") {
    print(paste0("Could not download the missing file: ",fname))
  } else {
    print(paste0("Download sucess: ",fname))
    writeBin(tmp_file, con=paste0(directory,fname))
  }
}

# remove no_char_eliminar digits from the beggining of product_string in case number of letters of product_string is equal to no_char_limiar
# TODO: remove? probably obsolete
RemoveDirectoryFromFilenameVec = function(product_string) {
  # find the string MAIAC inside the product string
  pos = gregexpr('MAIAC', product_string[1])[[1]]
  
  # if the algorithm find a match
  if (any(pos) > 0) {
    # trim the string by the last MAIAC match
    product_string = substr(product_string,pos[length(pos)],nchar(product_string[1]))
  }
  
  return(product_string)
}

# function to convert "x" files .HDF to .TIF from an input directory "input_dir" to a temporary output directory "output_dir"/tmp
ConvertHDF2TIF = function(x, input_dir, output_dir, tmp_dir, maiac_ftp_url) {
  #x = product_fname
  for(i in 1:length(x)) {
    #i=1
    
    # adjust output filename in case the product name has folder in the beggining
    #x1 = x[i]
    #x1 = RemoveDirectoryFromFilenameVec(x[i])
    x1 = basename(x[i])

    # check if x[i] converted tif file exists
    # if it does, just throw some message
    # if it doesnt, try to convert, if it works nice, if it doesnt try to download the tile and process it
    if (!file.exists(paste0(output_dir,tmp_dir,x1,"_01.tif")) & !file.exists(paste0(output_dir,tmp_dir,x1,"_1.tif"))) {
      # message
      print(paste0("Converting HDF to TIF file ",i," from ",length(x)," -> ",x1))
      
      # sds=T converts all files, while sd_index is only 1 file
      # dont need to specify data type for each SDS, it detects by itself
      gdal_translate(paste0(input_dir,x[i]), dst_dataset = paste0(output_dir,tmp_dir,x1,".tif"), verbose=F, sds=TRUE)
      
      # check if file exists after converting
      if (!file.exists(paste0(output_dir,tmp_dir,x1,"_01.tif")) & !file.exists(paste0(output_dir,tmp_dir,x1,"_1.tif"))) {
        # if it doesn't, it means that the HDF is corrupted and a download is needed
        w=0
        while((!file.exists(paste0(output_dir,tmp_dir,x1,"_01.tif")) & !file.exists(paste0(output_dir,tmp_dir,x1,"_1.tif"))) & w<5) {
          # message
          print(paste0("Error while converting file ",i," from ",length(x)," -> ",x1))
          
          # download the missing file
          DownloadMissingFile(x1, paste0(input_dir,dirname(x[i])), maiac_ftp_url)
          
          # try to convert again
          gdal_translate(paste0(input_dir,x1), dst_dataset = paste0(output_dir,tmp_dir,x1,".tif"), verbose=F, sds=TRUE)
          
          # count
          w=w+1 
        }
        
        # check if the file was extracted
        if (file.exists(paste0(output_dir,tmp_dir,x1,"_01.tif")) & file.exists(paste0(output_dir,tmp_dir,x1,"_1.tif"))) {
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
  # clusterExport(cl, varlist=c("singleHDF2TIF", "input_dir","output_dir"))
  # clusterEvalQ(cl, library(gdalUtils)) # pra passar packages pros workers
  # 
  # foreach(i=1:length(x)) %dopar% {
  #   singleHDF2TIF(x[i],input_dir,output_dir,tmp_dir)
  # }
  # 
  # stopCluster(cl)
  
}



# function to load files of a specific type from the temporary output folder "output_dir"/tmp
LoadMAIACFiles = function(raster_filename, output_dir, tmp_dir, type) {
  # name and order vector of the subdatasets
  science_dataset_names = c("sur_refl", "Sigma_BRFn", "Snow_Fraction", "Snow_Grain_Diameter", "Snow_Fit", "Status_QA", "sur_refl_500m", "cosSZA", "cosVZA", "RelAZ", "Scattering_Angle", "Glint_Angle", "SAZ", "VAZ", "Fv", "Fg")
  science_dataset_parameter_names = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
  
  # identify the type
  type_number = sprintf("%02d", grep(paste0("^", type, "$"), science_dataset_names))
  if (length(type_number)==0)
    type_number = sprintf("%01d", grep(paste0("^", type, "$"), science_dataset_parameter_names))
  if (length(type_number)==0) {
    stop(paste0("Can't find file type to open: ",type))
  }
  
  # list of bricks
  raster_brick = list()
  
  # loop though files and load the files
  for(i in 1:length(raster_filename)) {
    raster_brick[[i]] = brick(paste0(output_dir,tmp_dir,raster_filename[i],"_",type_number,".tif"))
  }
  
  # return
  return(raster_brick)
}

# function to filter bad values of a "x" variable between min and max, or equal something
FilterBadValues = function(x, minArg, maxArg, equal) {
  
  # filter function equal
  FilterEqual = function(x) {
    x[x==equal]=NA
    return(x)
  }
  
  # filter function min/max
  FilterMinMax = function(x) {
    x[x<minArg | x>maxArg]=NA
    return(x)
  }
  
  # choose function based on argument passed to the function
  if (hasArg(equal)) {
    MyFun =  FilterEqual
  } else if (hasArg(minArg) & hasArg(maxArg)) {
    MyFun =  FilterMinMax  
  } else {
    print("Couldn't find min, max or equal argument, returning the variable without filtering.")
    return(x)
  }
  
  # apply the function
  # for function method
  if (is.list(x)) {
    for (i in 1:length(x)) {
      x[[i]] = MyFun(x[[i]])
    }
  } else
    x = MyFun(x)

  # return value
  return(x)
}

# function to convert brf to brfn
# to do: arquivo vira float depois da covnersao, transformar em outro formato? (ex. int2s)
# transf para int2s parece que ferra os valores
# band values are calculated ok, tested calculating one band separatedely and compared to the batch convert
ConvertBRFNadir = function(BRF, FV, FG, kL, kV, kG) {
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
  tile_vec = vector()
  for (i in 1:length(kL)) {
    tile_vec[i] = substr(names(kL[[i]])[[1]],11,16)
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
    kLi = kL[[grep(idx,tile_vec)]]
    kVi = kV[[grep(idx,tile_vec)]]
    kGi = kG[[grep(idx,tile_vec)]]
    
    # calculate brf nadir
    # original do documento: (BRFn = BRF * (kL - 0.04578*kV - 1.10003*kG)/( kL + FV*kV + FG*kG))
    BRFn[[i]] = BRFi * (kLi - (0.04578*kVi) - (1.10003*kGi))/(kLi + (FVi*kVi) + (FGi*kGi))
  }
  
  # constrain the BRFn between 0 and 1 - possible results
  BRFn = FilterBadValues(BRFn, min=0, max=1)
  
  # return
  return(BRFn)
}

# compute quality based on occuring QA on the raster
# https://stevemosher.wordpress.com/2012/12/05/modis-qc-bits/
# https://lpdaac.usgs.gov/sites/default/files/public/modis/docs/MODIS_LP_QA_Tutorial-1b.pdf
# dataspec.doc
ComputeQuality = function(x) {
  # interpreting all possible QA
  qa_dataframe <- data.frame(Integer_Value = x,
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
    qa_as_int = as.integer(intToBits(qa_dataframe$Integer_Value[i])[1:16])
    
    # Flip the vector
    qa_as_int = qa_as_int[16:1]
    
    # Bit12to15, surface change mask
    qa_dataframe[i,2] = paste0(qa_as_int[1:4], collapse="")
    
    # Bit10-11, reserved
    qa_dataframe[i,3] = paste0(qa_as_int[5:6], collapse="")
    
    # Bit9, algorithm initialize status
    qa_dataframe[i,4] = paste0(qa_as_int[7], collapse="")
    
    # Bit8, aot level
    qa_dataframe[i,5] = paste0(qa_as_int[8], collapse="")
    
    # Bit5to7, adjacency mask
    qa_dataframe[i,6] = paste0(qa_as_int[9:11], collapse="")
    
    # Bit3to4, land water snow/ice mask
    qa_dataframe[i,7] = paste0(qa_as_int[12:13], collapse="")
    
    # Bit0to2, cloud mask
    qa_dataframe[i,8] = paste0(qa_as_int[14:16], collapse="")
  }
  
  # return
  return(qa_dataframe)
}

# filter quality table, the things to filter out the image
FilterQuality= function(qa_dataframe) {
  #qa_dataframe = qaDF
  
  # aotlevel5
  filter_list = c(5, "1") # AOT is high (> 0.6) or undefined
  
  # filter adjacency6
  filter_list = rbind(filter_list,c(6, "001")) # Adjacent to cloud
  filter_list = rbind(filter_list,c(6, "010")) # Surrounded  by more than 8 cloudy pixels
  filter_list = rbind(filter_list,c(6, "011")) # Single cloudy pixel
  
  # filter cloud8
  filter_list = rbind(filter_list,c(8, "000")) # 000 ---  Undefined
  filter_list = rbind(filter_list,c(8, "010")) # 010 --- Possibly Cloudy (detected by AOT filter)
  filter_list = rbind(filter_list,c(8, "011")) # 011 --- Cloudy  (detected by cloud mask algorithm)
  filter_list = rbind(filter_list,c(8, "101")) # 101 -- - Cloud Shadow
  #filter_list = rbind(filter_list,c(8, "110")) # 110 --- hot spot of fire
  filter_list = rbind(filter_list,c(8, "111")) # 111 --- Water Sediments
  
  # remove filter_list from the quality data frame (qa_dataframe)
  for (i in 1:dim(filter_list)[1]) {
    if (dim(qa_dataframe)[1] > 0) {
      idx = which(qa_dataframe[as.numeric(filter_list[i,1])]==filter_list[i,2])
      if (length(idx) > 0)
        qa_dataframe = qa_dataframe[-idx,]
    }
  }
  
  return(qa_dataframe)
}

# function to create a single qa mask based on a qa raster
CreateSingleQAMask = function(raster_file) {
  #x = qaBrick[[5]]
  
  # get unique qa values
  uniqueQA = as.numeric(unique(raster_file))
  
  # remove nan's
  uniqueQA = uniqueQA[!is.na(uniqueQA)]
  
  if (length(uniqueQA) > 0) {
    # compute qa dataframe with all possible quality combinations in the image
    qaDF = computeQuality(uniqueQA)
    
    # filter the qa dataframe with a set of determined rules excluding the bad ones
    qaDFFiltered = filterQuality(qaDF)
    
    # create the mask using the remaining QA, rest of values become NaN
    if (dim(qaDFFiltered)[1] > 0)
      raster_file = subs(raster_file, data.frame(id=qaDFFiltered$Integer_Value, v=rep(1,length(qaDFFiltered$Integer_Value))))
    else
      raster_file[]=NaN
    
  } else {
    raster_file[]=NaN
  }
  
  # return
  return(raster_file)
}

# function to create a qa mask based on a qa raster brick "raster_brick"
CreateQAMask = function(raster_brick) {
  # initiate list
  mask_brick = list()
  
  # loop throught the brick
  for (i in 1:length(raster_brick)) {
    # message
    print(paste0("Creating qa mask file ",i," from ",length(raster_brick)))
    
    # add to the brick
    mask_brick[[i]] = CreateSingleQAMask(raster_brick[[i]])
  }
  
  # return
  return(mask_brick)
}

# function to apply the QA mask over a raster brick of the same size
ApplyMaskOnBrick = function(raster_brick, mask_brick) {
  # initiate list
  masked_raster_brick = list()
  
  # loop throught the "raster_brick" brick
  for (i in 1:length(raster_brick)) {
    # message
    print(paste0("Applying mask in file ",i," from ",length(raster_brick)))
    
    # apply mask
    masked_raster_brick[[i]] = mask(raster_brick[[i]], mask_brick[[i]])
  }
  
  # return
  return(masked_raster_brick)
}

# function to create extreme azimutal angle >80 mask and apply to BRF
FilterEA = function(raster_brick, product_fname, output_dir, tmp_dir) {
  #cosSZA = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "cosSZA")
  #cosSZA = filterBadValues(cosSZA, min=-10000, max=10000)
  #acosSZA = lapply(cosSZA,FUN = function(raster_brick) calc(raster_brick, fun = acos))
  SAZ = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "SAZ")
  SAZ = FilterBadValues(SAZ, min=-10000, max=80)
  
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
  raster_brick = ApplyMaskOnBrick(raster_brick, SAZMask)
  
  # return
  return(raster_brick)
  
}

# function to reorder the brick list per band instead of per date
ReorderBrickPerBand = function(raster_brick) {
  # new list
  y = list()
  
  # loop through bands
  for (j in 1:nlayers(raster_brick[[1]])) {
    # message
    print(paste0("Re-ordering brick per band ",j," from ",nlayers(raster_brick[[1]])))
    
    # create brick
    y[[j]] = brick()
    
    # loop through dates filling the y
    for (i in 1:length(raster_brick)) {
      y[[j]] = addLayer(y[[j]],raster_brick[[i]][[j]])
    }
  }
  
  # return
  return(y)
}

# function to merge the data from 8-day time span into one composite file using the median value
CalcMedianBRF8day = function(raster_brick_per_band) {
  #raster_brick_per_band=nadirBRFBrickMaskedPerBand
  
  # function to calculate median and return the nearest value to the median
  # this prevents the algorithm calculating the mean of "two median values" if you dont have one single median, example:
  # values = c(0.0338, 0.0172, 0.0368, NA, 0.0230, NA, NA, NA, NA, NA, NA)
  # median equlals to 0.0284, a value that doesn't exist, it's a mean of 0.0338 and 0.0230
  # now, the difference between values and median = c(0.0054, 0.0112, 0.0084, NA, 0.0054, NA, NA, NA, NA, NA, NA)
  # an existing median can be either 0.0338 or 0.023, but i choose arbitrarily to get the first value it find in the vector
  medianIndex = function(x) which.min(abs(x - median(x, na.rm=T)))
  
  # function to calc the median "m" of a raster brick and return the index "idx" of the raster corresponding to that median value, also get how many observations that pixel had
  CalcMedianAndN = function(x) {
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
  median_raster_brick_per_band = list()
  
  # load package
  #require(snow)
  
  # for each band
  for (i in 1:length(raster_brick_per_band)) {
    # message
    print(paste0("Calculating median per band ",i," from ",length(raster_brick_per_band)))
    
    # calc median
    # beginCluster(8)
    # median_raster_brick_per_band[[i]] = clusterR(raster_brick_per_band[[i]], calc, args=list(fun=medianAndN), export=c("medianIndex"))
    # endCluster()
    median_raster_brick_per_band[[i]] = calc(raster_brick_per_band[[i]], fun=CalcMedianAndN)
  }
  
  # only bands
  median_raster_brick_per_band=brick(c(lapply(median_raster_brick_per_band,FUN=subset, subset=1),median_raster_brick_per_band[[1]][[2]]))
  names(median_raster_brick_per_band)=c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")
  
  return(median_raster_brick_per_band)
}

# function to write the processed file to disk while applying a factor of 10000 to bands 1-8 to reduce disk space usage
SaveProcessedTileComposite = function(medianBRF, output_dir, composite_fname, tile, year, day) {
  # factors for each band
  #factors = c(10000,10000,10000,10000,10000,10000,10000,10000,1)
  factors = c(10000,10000,10000,10000,10000,10000,10000,10000,10000)
  
  # apply factors
  b = brick(lapply(c(1:9),FUN=function(x) round(unstack(medianBRF)[[x]]*factors[x],0)))
  
  # write to file
  writeRaster(b, filename=paste0(output_dir,composite_fname,".",tile,".",year, day[8],".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
  #writeRaster(medianBRF, filename=paste0(output_dir,tmp_dir,"Processed.",tile,".tif"), format="GTiff", overwrite=TRUE)
  
  # message
  print(paste0("Tile composite was saved: ",composite_fname,".",tile,".",year, day[8],".tif"))
}

# function to write the processed file to disk while applying a factor of 10000 to bands 1-8 to reduce disk space usage
SaveProcessedTile = function(medianBRF, output_dir, tmp_dir, tile) {
  # factors for each band
  factors = c(10000,10000,10000,10000,10000,10000,10000,10000,1)
  
  # apply factors
  b = brick(lapply(c(1:9),FUN=function(x) round(unstack(medianBRF)[[x]]*factors[x],0)))
  
  # write to file
  writeRaster(b, filename=paste0(output_dir, "Processed.", tile, ".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
  #writeRaster(medianBRF, filename=paste0(output_dir,tmp_dir,"Processed.",tile,".tif"), format="GTiff", overwrite=TRUE)
  
  # message
  print(paste0("Tile was saved: ",tile))
}

# function to mosaic given tiles from a list of bricks "x"
MosaicTilesAndSave = function(x, output_dir, tmp_dir, year, day) {
  #x = processedList
  
  # message
  print("Mosaicking the tiles...")
  
  # mosaic and save files
  m=mosaic_rasters(paste0(output_dir,tmp_dir,x), dst_dataset = paste0(output_dir,composite_fname,"_",year,day[8],".tif"), verbose=F, output_Raster = TRUE, ot="Int16", co = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"))
  
  # return
  return(m)
}

# function to reproject composites
ReprojectComposite = function(x, output_dir, tmp_dir, year, day, output_raster) {
  
  # South America region has Sinusoid projection, with central longitude 58W.
  # projecao lat/lon: "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
  # projecao original do maiac: "+proj=sinu +lon_0=-3323155211.758775 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
  # projecao ajustada do maiac com longitude central da SouthAmerica -58W (sugestão Yujie Wang): "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
  gdalwarp(paste0(output_dir,composite_fname,"_",year,day[8],".tif"),
           dstfile = paste0(output_dir,composite_fname,"_LatLon_",year,day[8],".tif"),
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
