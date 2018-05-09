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
  if (file.exists(paste0(output_dir,composite_fname,"_",year,day[length(day)],".tif"))) {
    # message
    print(paste0(Sys.time(), ": Composite ",paste0(composite_fname,"_",year,day[length(day)])," is already processed, going to the next iteration."))
    
    # go to the next iteration
    result = TRUE
  }
  
  # return
  return(result)
}

# function to check for a processed tile composite, if it does exist just go to the next iteration
IsTileCompositeProcessed = function(composite_fname, tile, year, day, output_dir, overwrite_files) {
  # set escape variable default
  result = FALSE
  
  # name of the bands
  band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")
  
  # if it is not a manual run, proceed to test if files exist, otherwise just skip testing and process
  if (!overwrite_files) {
    
    # define the composite number or name
    if (composite_no == "month") {
      composite_num = paste0("_",format(as.Date(paste0(day[length(day)],"-", year), "%j-%Y"), "%m"))
    } else {
      composite_num = day[length(day)]
    }
    
    # check if tile composite exists, check just band 1 because the rest is supposed to be there aswell
    if (file.exists(paste0(output_dir,composite_fname,".",tile,".",year, composite_num,".",band_names[1],".tif"))) {
      # message
      print(paste0(Sys.time(), ": Tile composite ",paste0(composite_fname,".",tile,".",year,composite_num)," is already processed, going to the next iteration."))
      
      # go to the next iteration
      result = TRUE
    }
  }
  
  return(result)
}

# function to check for a processed tile, if it does return TRUE, otherwise return FALSE
isTileProcessed = function(tile, input_dir, output_dir, tmp_dir) {
  # set escape variable default
  result = FALSE
  
  #x=tile[i]
  if (file.exists(paste0(tmp_dir,"Processed.",tile,".tif"))) {
    # message
    print(paste0(Sys.time(), ": Tile ",tile," is already processed, going to the next iteration."))
    
    # go to the next iteration
    result = TRUE
  }
  return(result)
}

# function to check if RTLS tile is available, if it does return TRUE, if it doesn't exist, create a "processed tile" with nan values
IsDataAvailable = function(type, tile, year, day, nan_tiles_dir, output_dir, obs, maiac_ftp_url, composite_fname, downloaded_files_dir, composite_no) {
  # set escape variable default
  result = TRUE
  
  # retrieve data available
  if (obs=="rtls") {
    fname = basename(GetFilenameVec(type, input_dir, downloaded_files_dir, tile, year, day, offset_days = 24))
  } else {
    fname = basename(GetFilenameVec(type, input_dir, downloaded_files_dir, tile, year, day, offset_days = 0))
  }
  
  # if its brf
  if (length(fname)==0 & obs == "brf") {
    
    # log the bad file
    line = paste(obs, year, day[length(day)], tile, sep=",")
    write(line, file=paste0(output_dir,"missing_files.txt"), append=TRUE)
    
    # load a brick of 9 nan bands (equivalent to band1-8 and number of pixels)
    b = brick(lapply(c(1:9), FUN=function(x) raster(paste0(nan_tiles_dir,"nantile.",tile,".tif"))))
    
    # save the nan tile as the processed tile
    #writeRaster(b, filename=paste0(output_dir, composite_fname, ".", tile, ".", year, day[length(day)], ".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    SaveProcessedTileComposite(b, output_dir, composite_fname, tile, year, day, composite_no)
    
    # message
    print(paste0(Sys.time(), ": Couldn't find ", obs ," tile ", tile, ", year ", year, ", and day ", day[length(day)],". Going to next iteration..."))
    
    # go to the next iteration
    result = FALSE
  }
  
  # if no rtls
  if (length(fname)==0 & obs == "rtls") {
    # there was a code here to try and download the missing files, but it was tricky
    
    # log the bad file
    line = paste(obs, year, day[length(day)], tile, sep=",")
    write(line, file=paste0(output_dir,"missing_files.txt"), append=TRUE)
    
    
    # load a brick of 9 nan bands (equivalent to band1-8 and number of pixels)
    b = brick(lapply(c(1:9), FUN=function(x) raster(paste0(nan_tiles_dir,"nantile.",tile,".tif"))))
    
    # save the nan tile as the processed tile
    #writeRaster(b, filename=paste0(output_dir, composite_fname, ".", tile, ".", year, day[length(day)], ".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    SaveProcessedTileComposite(b, output_dir, composite_fname, tile, year, day, composite_no)
    
    # message
    print(paste0(Sys.time(), ": Couldn't find ", obs ," tile ", tile, ", year ", year, ", and day ", day[length(day)],". Going to next iteration..."))
    
    # go to the next iteration
    result = FALSE
  }
  
  # return
  return(result)
}

# function to create nan tiles, to use in case the time series dont have data for one tile on a given date
CreateNanTiles = function(tile, nan_tiles_dir, latlon_tiles_dir) {
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
  } else {
    r = raster(paste0(nan_tiles_dir,"nantile.",tile,".tif"))
  }
  return(r)
}

# function to get filenames of each 8-day product or parameters files from a product "x", from a input directory "input_dir", of a respective "tile", "year" and day vector "day"
GetFilenameVec = function(type, input_dir, downloaded_files_dir, tile, year, day, offset_days) {
  # initiate some things
  result = c()
  continue_processing=TRUE
  offset_i = 0
  day2 = day
  
  # loop to find rtls and increase offset on each iteration
  while(length(result)==0 & continue_processing) {
    # create combinatios of product, tile, year and day
    combinations = expand.grid(type, paste0(".",tile,"."), year, sprintf("%03s",day2))
    
    # merge the combinations
    combinations = paste0(combinations$Var1, combinations$Var2, combinations$Var3, combinations$Var4)
    
    # get files from input_dir
    result = list.files(input_dir, pattern=paste(combinations,collapse="|"), recursive = TRUE, full.names = TRUE)
    
    # get files from downloaded_files_dir
    result2 = list.files(downloaded_files_dir, pattern=paste(combinations,collapse="|"), recursive = TRUE, full.names = TRUE)
    
    # merge both, downloaded first so it has priority when filtering for duplicates
    result = c(result2, result)
    
    # find and remove duplicates
    if (length(result2)>0)
      result = result[-c(which(duplicated(basename(result))))]
    
    # if there is results, just finish processing
    if (length(result)>0) {
      continue_processing=FALSE
    } else {
      # if no results and its rtls try to increase offset
      if (type=="MAIACRTLS") {
        # test if the composite rtls is available and remove the rest
        if (offset_days>0 & any(offset_days == c(8,16,24))) {
          # raise offset in 8
          offset_i = offset_i + 8
          
          # get all possible rtls days
          rtls_days = seq(8,360,8)
          
          # find which rtls files should exist
          day_match = match(as.numeric(day), rtls_days)
          
          # get its index
          day_idx = day_match[!is.na(day_match)]
          
          # get its value
          rtls_match = rtls_days[day_idx]
          
          # list rtls files with -24 to +24 days around the composite date
          day2 = sprintf("%03d", seq(min(rtls_match)-offset_i, max(rtls_match)+offset_i, 1))
        }
      } else { # if its brf just finish processing
        continue_processing=FALSE
      }
    }
  }
  
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
    # message
    stop(paste0(Sys.time(), ": Problem in FilterProductTilesbyRTLSTiles function, can't find MAIAC in the fname."))
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
DownloadMissingFile = function(fname, directory, maiac_ftp_url, output_dir) {
  #fname = x[i]
  # example file: "MAIACABRF.h01v01.20132171720.hdf"
  # example: ftp://maiac@dataportal.nccs.nasa.gov/DataRelease/SouthAmerica/h00v00/2000/MAIACRTLS.h00v00.2000096.hdf
  
  # message
  print(paste0(Sys.time(), ": Trying to download the missing file: ",fname))
  
  # file url
  tile = substr(fname,11,16)
  year = substr(fname,18,21)
  file_url = paste0(maiac_ftp_url,tile,"/",year,"/",fname)
  
  # download the file
  tmp_file = try(getBinaryURL(file_url, verbose = F, username="maiac"))
  
  # download loop!
  w=0
  while(w <= 15 && class(tmp_file) == "try-error") {
    print(paste0(Sys.time(), ": getBinaryURL error, trying again in ",10+w," seconds... try number ",w+1))
    Sys.sleep(10+w)
    tmp_file = try(getBinaryURL(file_url, verbose = F, username="maiac"))
    closeAllConnections()
    w=w+1
  }
  
  # save the file on the disk
  if (class(tmp_file) == "try-error") {
    print(paste0(Sys.time(), ": Could not download the missing file: ",fname))
    
    # log
    line = fname
    write(line, file=paste0(output_dir,"download_fail.txt"), append=TRUE)
  } else {
    dir.create(file.path(directory), showWarnings = FALSE)
    writeBin(tmp_file, con=paste0(directory,fname))
    print(paste0(Sys.time(), ": Download sucess: ",fname))
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

# function to convert BRF and RTLS files from .HDF to .TIF from an input directory "input_dir" to a temporary output directory "output_dir"/tmp
ConvertHDF2TIF = function(x, y, input_dir, output_dir, tmp_dir, maiac_ftp_url, no_cores, log_fname, is_ea_filter, is_qa_filter, downloaded_files_dir, download_enabled, process_dir) {
  # message
  print(paste0(Sys.time(), ": Converting HDFs to TIF in parallel..."))
  
  # measure time
  t1 = mytic()
  
  # create sds_to_retrieve list
  sds_to_retrieve_brf = c("01","15","16")
  if (is_ea_filter)
    sds_to_retrieve_brf = c(sds_to_retrieve_brf,"13")
  if (is_qa_filter)
    sds_to_retrieve_brf = c(sds_to_retrieve_brf,"06")
  sds_to_retrieve_rtls = c("1","2","3")
  sds_to_retrieve_mat = rbind(matrix(sds_to_retrieve_brf,length(x),3, byrow=T),matrix(sds_to_retrieve_rtls,length(y),3, byrow=T))
  
  # create sds_suffix and prefix
  sds_preffix = "HDF4_EOS:EOS_GRID:"
  sds_suffix_brf = c(":grid1km:sur_refl",":grid1km:Sigma_BRFn",":grid1km:Snow_Fraction",":grid1km:Snow_Grain_Diameter",":grid1km:Snow_Fit",":grid1km:Status_QA",":grid500m:sur_refl_500m",":grid5km:cosSZA",":grid5km:cosVZA",":grid5km:RelAZ",":grid5km:Scattering_Angle",":grid5km:Glint_Angle",":grid5km:SAZ",":grid5km:VAZ",":grid5km:Fv",":grid5km:Fg")
  sds_suffix_rtls = c(":grid1km:Kiso",":grid1km:Kvol",":grid1km:Kgeo",":grid1km:sur_albedo",":UpdateDay")
  
  # merge x and y
  x = c(x,y)
  
  # Initiate cluster
  cl = parallel::makeCluster(min(length(x), no_cores))
  registerDoParallel(cl)
  objects_to_export = c("x", "input_dir", "output_dir", "tmp_dir", "maiac_ftp_url", "DownloadMissingFile", "sds_to_retrieve_mat", "sds_preffix", "sds_suffix_brf","sds_suffix_rtls")
  
  # loop through the files
  foreach(i = 1:length(x), .packages=c("raster","gdalUtils","rgdal","RCurl"), .export=objects_to_export, .errorhandling="remove") %dopar% {
    # adjust output filename in case the product name has folder in the beggining
    x1 = basename(x[i])
    
    # check if x[i] converted tif file exists
    # if it does, just throw some message
    # if it doesnt, try to convert, if it works nice just go on, if it doesnt try to download the tile and process it again
    if (any(!file.exists(paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,],".tif")))) {
      # message
      print(paste0(Sys.time(), ": Converting HDF to TIF file ",i," from ",length(x)," -> ",x1))
      
      # get the sds list
      #sds_list = get_subdatasets(paste0(input_dir,x[i]))  # slooooow
      #sds_list = paste0(sds_preffix,paste0(input_dir,x[i]),sds_suffix)  # fast!
      if (sds_to_retrieve_mat[i,1] == "01") {
        sds_list = paste0(sds_preffix,x[i],sds_suffix_brf)  # fast!
      } else
        sds_list = paste0(sds_preffix,x[i],sds_suffix_rtls)  # fast!
      
      # retrieve one sub-data set each time
      for (j in 1:length(sds_to_retrieve_mat[i,])) { #sprintf("%02d",sds_to_retrieve_mat[i,][j])
        if (any(as.numeric(sds_to_retrieve_mat[i,][j]) == c(15,16))) {
          gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=F, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]), a_nodata=-99999)
        } else
          gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=F, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]))
      }
      
      # check if file exists after converting
      # if it doesn't, it can mean two things: (1) converting was somehow interrupted -> convert again, or (2) HDF is corrupted -> download again
      try_count = 0
      while (any(!file.exists(paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,],".tif"))) & try_count < 3) {
        
        # option 1, problem in conversion
        # solution: try to convert it again, and test for all files, test this 2 times
        if (any(file.exists(paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,],".tif"))) & try_count < 2) {
          # try to convert again
          for (j in 1:length(sds_to_retrieve_mat[i,])) { #sprintf("%02d",sds_to_retrieve_mat[i,][j])
            if (any(as.numeric(sds_to_retrieve_mat[i,][j]) == c(15,16))) {
              gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=F, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]), a_nodata=-99999)
            } else
              gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=F, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]))
          }
          
          # counter
          try_count = try_count + 1
          
          # go back to the while statement to check if all files exist
          next
        }
        
        # after two tries converting again, the problem might be a corrupted HDF
        # option 2, corrupted HDF
        # solution: download again, test download for 5 times, NASA website sometimes doesn't respond
        try_count_download = 0
        while (any(!file.exists(paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,],".tif"))) & try_count_download < 5 & download_enabled) {
          # message
          print(paste0(Sys.time(), ": Error while converting file ",i," from ",length(x)," -> ",x1))
          
          # download the missing file and place the file in the input folder
          #DownloadMissingFile(x1, paste0(input_dir, dirname(x[i]),"/"), maiac_ftp_url, output_dir)
          
          # download the missing file and place the file in a download directory
          DownloadMissingFile(x1, downloaded_files_dir, maiac_ftp_url, output_dir)
          
          # change the sds list
          if (sds_to_retrieve_mat[i,1] == "01") {
            sds_list = paste0(sds_preffix,paste0(downloaded_files_dir,x1),sds_suffix_brf)  # fast!
          } else
            sds_list = paste0(sds_preffix,paste0(downloaded_files_dir,x1),sds_suffix_rtls)  # fast!
          
          # try to convert again
          for (j in 1:length(sds_to_retrieve_mat[i,])) { #sprintf("%02d",sds_to_retrieve_mat[i,][j])
            if (any(as.numeric(sds_to_retrieve_mat[i,][j]) == c(15,16))) {
              gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=F, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]), a_nodata=-99999)
            } else
              gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=F, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]))
          }
          
          # count
          try_count_download = try_count_download + 1 
        }
        
        # check if the file was extracted
        if (all(file.exists(paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,],".tif")))) {
          print(paste0(Sys.time(), ": File ",x1," was downloaded and extracted with sucess. Error avoided (i hope), oh yeah!"))
        }
        
      }
      
    } else {
      print(paste0(Sys.time(), ": File ",i," from ",length(x)," is already converted to tif -> ",x1))
    }
    
    # file does not exist... report in a .txt
    if (any(!file.exists(paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,],".tif")))) {
      print(paste0(Sys.time(), ": ERROR on file ",i," from ",length(x),", could not convert hdf2tif -> ",x1))
      write(x1, file=paste0(process_dir,"hdf2tif_convert_fail.txt"), append=TRUE)
    }
    
  }
  
  # finish cluster
  stopCluster(cl)
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Converting HDFs to TIF in parallel finished in ", t2))
  
}

ConvertHDF2TIF = cmpfun(ConvertHDF2TIF)

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
    # message
    stop(paste0(Sys.time(), ": Can't find file type to open: ",type))
  }
  
  # list of bricks
  raster_brick = list()
  
  # loop though files and load the files
  for(i in 1:length(raster_filename)) {
    raster_brick[[i]] = brick(paste0(tmp_dir,raster_filename[i],"_",type_number,".tif"))
  }
  
  # return
  return(raster_brick)
}

# function to filter bad values of a "x" variable
FilterValEqualToNA = function(x, equal) {
  
  # filter function equal
  FilterEqual = function(x, equal) {
    x[x==equal]=NA
    return(x)
  }
  
  # apply the function
  # for function method
  if (is.list(x)) {
    for (i in 1:length(x)) {
      x[[i]] = FilterEqual(x[[i]], equal)
    }
  } else
    x = FilterEqual(x, equal)
  
  # return value
  return(x)
}

# function to filter bad values of a "x" variable
FilterValOutRangeToNA = function(x, minArg, maxArg) {
  
  # filter function min/max
  FilterMinMax = function(x, minArg, maxArg) {
    x[x<minArg | x>maxArg]=NA
    return(x)
  }
  
  # apply the function
  # for function method
  if (is.list(x)) {
    y = list()
    for (i in 1:length(x)) {
      y[[i]] = FilterMinMax(x[[i]], minArg, maxArg)
    }
  } else
    y = FilterMinMax(x, minArg, maxArg)
  
  # return value
  return(y)
}

# function to convert brf to brfn
# to do: arquivo vira float depois da covnersao, transformar em outro formato? (ex. int2s)
# transf para int2s parece que ferra os valores
# band values are calculated ok, tested calculating one band separatedely and compared to the batch convert
ConvertBRFNadir = function(BRF, FV, FG, kL, kV, kG, tile, year, output_dir, no_cores, log_fname, view_geometry) {
  # BRF = brf_reflectance  # (12 bandas por data, 1km)
  # FV = brf_fv  # (1 por data, 5km)
  # FG = brf_fg  # (1 por data, 5km)
  # kL = rtls_kiso  # (8 bandas, 1km)
  # kV = rtls_kvol  # (8 bandas, 1km)
  # kG = rtls_kgeo  # (8 bandas, 1km)
  
  # message
  print(paste0(Sys.time(), ": Normalizing brf in parallel..."))
  
  # measure time
  t1 = mytic()
  
  # retrieve RTLS day
  rtls_day_vec = vector()
  for (i in 1:length(kL)) {
    rtls_day_vec[i] = as.numeric(substr(names(kL[[i]])[[1]],22,24))
  }
  
  # function to normalize
  ff_nadir = function(BRFi, kLi, kVi, kGi, FVi, FGi) {
    #return(BRFi * (kLi - (0.04578*kVi) - (1.10003*kGi))/(kLi + (FVi*kVi) + (FGi*kGi)))
    a = BRFi * {kLi - {0.04578*kVi} - {1.10003*kGi}}/{kLi + {FVi*kVi} + {FGi*kGi}}
    a[a<0 | a>1]=NA
    a
  }
  
  # function to normalize backscat
  ff_backscat = function(BRFi, kLi, kVi, kGi, FVi, FGi) {
    # For AZ=180, kernels are: backward
    # Fg = 0.017440045;    Fv=0.22930469;
    a = BRFi * {kLi + {0.22930469*kVi} + {0.017440045*kGi}}/{kLi + {FVi*kVi} + {FGi*kGi}}
    a[a<0 | a>1]=NA
    a
  }
  
  # function to normalize forwardscat
  ff_forwardscat = function(BRFi, kLi, kVi, kGi, FVi, FGi) {
    #For AZ=0, kernels are: forward
    #Fg = -1.6218740;    Fv=-0.12029795;
    a = BRFi * {kLi - {0.12029795*kVi} - {1.6218740*kGi}}/{kLi + {FVi*kVi} + {FGi*kGi}}
    a[a<0 | a>1]=NA
    a
  }
  
  # choose function
  if (view_geometry == "nadir")
    ff = cmpfun(ff_nadir)
  if (view_geometry == "backscat")
    ff = cmpfun(ff_backscat)
  if (view_geometry == "forwardscat")
    ff = cmpfun(ff_forwardscat)
  
  # Initiate cluster
  #cl = parallel::makeCluster(no_cores, outfile=log_fname)
  cl = parallel::makeCluster(min(length(BRF), no_cores))
  registerDoParallel(cl)
  objects_to_export = c("BRF", "FV", "FG", "kL", "kV", "kG", "tile", "year", "rtls_day_vec", "ff", "FilterValOutRangeToNA")
  
  # for each date
  BRFn = foreach(i = 1:length(BRF), .packages=c("raster"), .export=objects_to_export, .errorhandling="remove", .inorder = FALSE) %dopar% {
    # message
    print(paste0(Sys.time(), ": Normalizing brf iteration ",i," from ",length(BRF)))
    
    # set parameters, interpolate the 5km to 1 km by nearest neighbor
    FVi = disaggregate(FV[[i]], fact=c(5,5))
    
    # check if FVi is available
    if (is.na(minValue(FVi)) & is.na(maxValue(FVi)))
      return(0)
    
    # identify which tile is the given i data and set the parameters to the tile
    img_day = as.numeric(substr(names(FVi),22,24))
    
    # get rtls days that are lower than image day, and that are near the image day until 8 days (the aggreagated rtls), get always the lowest rlts day (index 1)
    idx = which(img_day <= rtls_day_vec & abs(rtls_day_vec - img_day) <= 8)[1]
    
    # if there is no rtls available, get the closest one and log it
    if (is.na(idx)) {
      idx = which(min(abs(img_day - rtls_day_vec)) == abs(img_day - rtls_day_vec))
      line = paste0("Tile: ", tile,", Year: ", year,", Image day: ", img_day,", RTLS day: ", rtls_day_vec[idx])
      write(line,file=paste0(output_dir, "processed_closest_rtls.txt"), append=TRUE)
    }
    
    # calculate brf nadir
    c(overlay(subset(BRF[[i]],1:8), kL[[idx]], kV[[idx]], kG[[idx]], FVi, FGi = disaggregate(FG[[i]], fact=c(5,5)), fun=ff))
  }
  
  # finish cluster
  stopCluster(cl)
  
  # unlist the results to get a correct output
  BRFn = unlist(BRFn)
  
  # remove the layers without data from BRFn
  idx_vec = vector()
  for (i in 1:length(BRFn)) {
    if (typeof(BRFn[[i]])=="double") {
      idx_vec = c(idx_vec,i)
    } else {
      if (all(is.na(minValue(BRFn[[i]]))) & all(is.na(maxValue(BRFn[[i]]))))
        idx_vec = c(idx_vec,i)
    }
  }
  if (length(idx_vec)>0)
    BRFn = BRFn[-c(idx_vec)]
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Normalizing brf in parallel finished in ", t2))
  
  # return
  return(BRFn)
}

ConvertBRFNadir = cmpfun(ConvertBRFNadir)

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
    print(paste0(Sys.time(), ": Creating qa mask file ",i," from ",length(raster_brick)))
    
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
    print(paste0(Sys.time(), ": Applying mask in file ",i," from ",length(raster_brick)))
    
    # apply mask
    masked_raster_brick[[i]] = mask(raster_brick[[i]], mask_brick[[i]])
  }
  
  # return
  return(masked_raster_brick)
}

# function to create extreme azimutal angle >80 mask and apply to BRF
FilterEA = function(raster_brick, product_fname, output_dir, tmp_dir) {
  #cosSZA = LoadMAIACFiles(product_fname, tmp_dir, "cosSZA")
  #cosSZA = FilterValOutRangeToNA(cosSZA, -10000, 10000)
  #acosSZA = lapply(cosSZA,FUN = function(raster_brick) calc(raster_brick, fun = acos))
  SAZ = LoadMAIACFiles(product_fname, tmp_dir, "SAZ")
  SAZ = FilterValOutRangeToNA(SAZ, -10000, 80)
  
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
ReorderBrickPerBand = function(raster_brick, output_dir, tmp_dir) {
  #raster_brick = nadir_brf_reflectance
  
  # if object is bigger than 5gb, we have to export files to disk and read it again
  if ((object.size(raster_brick)/(1024*1024*1024)) >= 5) {
    # message
    print(paste0(Sys.time(), ": Saving normalized brf to disk - because object is too big to hold in memory..."))
    
    # measure time
    t1 = mytic()
    
    # create dir for normalized brf
    norm_dir = paste0(tmp_dir,"norm_brf\\")
    dir.create(file.path(norm_dir), showWarnings = FALSE, recursive=T)
    
    # vec size
    n_samples = length(raster_brick)
    
    # export normalized brf
    i=1
    for (i in 1:n_samples) {
      writeRaster(raster_brick[[i]], filename = paste0(norm_dir, "norm_brf_",i,".tif"), overwrite=T, bylayer=T)
      #print(i)
    }
    rm("raster_brick")
    gc()
    
    # re-read files from disk
    raster_brick = list()
    i=1
    for (i in 1:n_samples) {
      raster_brick[[i]] = stack(paste0(norm_dir, "norm_brf_",i,"_",1:8,".tif"))
      #print(i)
    }
    
    # measure time
    t2 = mytoc(t1)
    
    # message
    print(paste0(Sys.time(), ": Saving normalized brf to disk finished in ", t2))
    
  }
  
  # new list
  y = list()
  
  # loop through bands
  for (j in 1:nlayers(raster_brick[[1]])) {
    # message
    print(paste0(Sys.time(), ": Re-ordering brick per band ",j," from ",nlayers(raster_brick[[1]])))
    
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

ReorderBrickPerBand = cmpfun(ReorderBrickPerBand)

# funcao de mediana em C++
# cppFunction("
#   double median2(std::vector<double> x){
#             double median;
#             size_t size = x.size();
#             sort(x.begin(), x.end());
#             if (size  % 2 == 0){
#             median = (x[size / 2 - 1] + x[size / 2]) / 2.0;
#             }
#             else {
#             median = x[size / 2];
#             }
#             return median;
#             }")

# function to merge the data from 8-day time span into one composite file using the median value
CalcMedianBRF = function(raster_brick_per_band, no_cores, log_fname, output_dir, tmp_dir) {
  #raster_brick_per_band = nadir_brf_reflectance_per_band
  
  # message
  print(paste0(Sys.time(), ": Calculating median..."))
  
  # measure time
  t1 = mytic()
  
  # function to calculate median and number of pixels
  CalcMedianAndN = function(x) {
    value = x[!is.na(x)]
    l_value = length(value)
    if (l_value == 0) {
      c(NA,NA)
    } else {
      c(median2(value), l_value)
    }
  }
  
  # compile (maybe it's faster)
  CalcMedianAndN = cmpfun(CalcMedianAndN)
  
  # rename the first layer so we can identify the bands inside the foreach/iterator
  for (i in 1:8)
    names(raster_brick_per_band[[i]])[1] = i
  
  # create the iterator object with the raster brick
  myit = iter(obj = raster_brick_per_band)
  
  # Initiate cluster
  #cl = parallel::makeCluster(min(8, no_cores), outfile=log_fname)
  cl = parallel::makeCluster(min(8, no_cores))
  registerDoParallel(cl)
  objects_to_export = c("CalcMedianAndN", "output_dir", "tmp_dir")
  
  # for each band, iterate through myit so each band is passed to one core
  foreach(i = myit, .packages=c("raster","median2rcpp"), .export=objects_to_export, .errorhandling="remove") %dopar% {
    # message
    #print(paste0(Sys.time(), ": Calculating median per band ",i," from ",length(raster_brick_per_band)))
    
    # calc median, if i == 1 save the number of pixels, otherwise just save the values
    if (names(i)[1]=="X1") {
      writeRaster(round(calc(i, fun=CalcMedianAndN)*10000,0), filename=paste0(tmp_dir, "Band_",names(i)[1],".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
    } else
      writeRaster(round(calc(i, fun=CalcMedianAndN)[[1]]*10000,0), filename=paste0(tmp_dir, "Band_",names(i)[1],".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
    
    # return 0 to the foreach
    c(0)
  }
  
  # finish cluster
  stopCluster(cl)
  
  # open the rasters and make a brick
  b1 = brick(paste0(tmp_dir, "Band_X",1,".tif"))
  median_raster_brick_per_band = stack(c(b1[[1]],paste0(tmp_dir, "Band_X",c(2:8),".tif"),b1[[2]]))
  
  # put a name to the bands
  names(median_raster_brick_per_band)=c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Calculating median finished in ", t2))
  
  # return
  return(median_raster_brick_per_band)
}

CalcMedianBRF = cmpfun(CalcMedianBRF)

# function to write the processed file to disk, don't need to apply factors anymore, because we're already applying it on median
SaveProcessedTileComposite = function(medianBRF, output_dir, composite_fname, tile, year, day, composite_no) {
  # factors for each band
  #factors = c(10000,10000,10000,10000,10000,10000,10000,10000,10000)
  
  # apply factors
  #b = brick(lapply(c(1:9),FUN=function(x) round(unstack(medianBRF)[[x]]*factors[x],0)))
  
  # test if number of layers is equal to 1, which means that is a nantile and gotta repeat it 9 times
  if (dim(medianBRF)[3] == 1) {
    medianBRF = brick(medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF)
  }
  
  # name of the bands
  band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")
  
  # define the composite number or name
  if (composite_no == "month") {
    composite_num = paste0("_",format(as.Date(paste0(day[length(day)],"-", year), "%j-%Y"), "%m"))
  } else {
    composite_num = day[length(day)]
  }
  
  # write to file
  for (i in 1:9) {
    writeRaster(medianBRF[[i]], filename=paste0(output_dir,composite_fname,".",tile,".",year, composite_num,".",band_names[i],".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
  }
  
  # message
  print(paste0(Sys.time(), ": Tile composite was saved: ",composite_fname,".",tile,".",year, composite_num))
}

# function to write the processed file to disk while applying a factor of 10000 to bands 1-8 to reduce disk space usage
SaveProcessedTile = function(medianBRF, output_dir, tmp_dir, tile) {
  # factors for each band
  factors = c(10000,10000,10000,10000,10000,10000,10000,10000,1)
  
  # apply factors
  b = brick(lapply(c(1:9),FUN=function(x) round(unstack(medianBRF)[[x]]*factors[x],0)))
  
  # write to file
  writeRaster(b, filename=paste0(output_dir, "Processed.", tile, ".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
  #writeRaster(medianBRF, filename=paste0(tmp_dir,"Processed.",tile,".tif"), format="GTiff", overwrite=TRUE)
  
  # message
  print(paste0(Sys.time(), ": Tile was saved: ",tile))
}

# function to mosaic given tiles from a list of bricks "x"
MosaicTilesAndSave = function(x, output_dir, tmp_dir, year, day) {
  #x = processedList
  
  # message
  print("Mosaicking the tiles...")
  
  # mosaic and save files
  m=mosaic_rasters(paste0(tmp_dir,x), dst_dataset = paste0(output_dir,composite_fname,"_",year,day[length(day)],".tif"), verbose=F, output_Raster = TRUE, ot="Int16", co = c("COMPRESS=DEFLATE","PREDICTOR=2","ZLEVEL=3"))
  
  # return
  return(m)
}

# function to reproject composites
ReprojectComposite = function(x, output_dir, tmp_dir, year, day, output_raster) {
  
  # South America region has Sinusoid projection, with central longitude 58W.
  # projecao lat/lon: "+init=epsg:4326 +proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
  # projecao original do maiac: "+proj=sinu +lon_0=-3323155211.758775 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
  # projecao ajustada do maiac com longitude central da SouthAmerica -58W (sugestão Yujie Wang): "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
  gdalwarp(paste0(output_dir,composite_fname,"_",year,day[length(day)],".tif"),
           dstfile = paste0(output_dir,composite_fname,"_LatLon_",year,day[length(day)],".tif"),
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

# function to create the day matrix
CreateDayMatrix = function(composite_no = 8) {
  if (is.numeric(composite_no)) {
    # create matrix of days on each composite
    day_mat = matrix(sprintf("%03d",1:(360 - 360%%composite_no)), ncol=composite_no, byrow=T)
  } else {
    day_mat=NA
  }
  
  # return
  return(day_mat)
}

# function to create composite name given parameters
CreateCompositeName = function(composite_no, product, is_qa_filter, is_ea_filter, view_geometry = "nadir") {
  # choose function
  if (view_geometry == "nadir")
    view_geometry_str = "Nadir"
  if (view_geometry == "backscat")
    view_geometry_str = "Backscat"
  if (view_geometry == "forwardscat")
    view_geometry_str = "Forwardscat"
  
  # base name
  if (composite_no == "month") {
    composite_fname = paste0("SR_",view_geometry_str,"_", composite_no)
  } else {
    composite_fname = paste0("SR_",view_geometry_str,"_", composite_no ,"day")
  }
  
  # which product
  if (length(product)==1) {
    composite_fname = paste0(composite_fname,"_",product)
  } else {
    composite_fname = paste0(composite_fname,"_MAIACTerraAqua")
  }
  
  # which filter
  if (is_qa_filter & is_ea_filter) {
    composite_fname = paste0(composite_fname,"_FilterQA-EA")
  } else {
    if (is_qa_filter) {
      composite_fname = paste0(composite_fname,"_FilterQA")
    } else {
      if (is_ea_filter) {
        composite_fname = paste0(composite_fname,"_FilterEA")
      }
    }
  }
  
  #return
  return(composite_fname)
}

# function to create loop mat, filtering the start and end dates from loop_mat, depending on composite_no
CreateLoopMat = function(day_mat, composite_no, input_dir_vec, tile_vec, manual_run) {

  # check if this is a manual run and create the loop_mat with the specific configuration
  if (any(manual_run != FALSE)) {
    # if its not a monthly composite, adjust values, for monthly its already ok
    if (composite_no != "month") {
      for (i in 1:dim(manual_run)[1])
        manual_run[i,1] = which(sprintf("%03d", as.numeric(manual_run[i,1])) == day_mat, arr.ind = TRUE)[1]
    }
    
    loop_mat = manual_run
    
  } else { # or create a loop matrix containing all time series
    # check if its a month composite
    if (composite_no == "month") {
      # create the loop mat excluding the start and end index
      mat1 = expand.grid(c(3:12), 2000)
      mat2 = expand.grid(c(1:12), c(2001:2016))
    } else { # if its a fixed number of days composite
      # find the lines in day_mat of first and last composite
      # begin of time series = 64 2000
      # end of time series = 352 2016
      idx_begin = which(sprintf("%03d", 64) == day_mat, arr.ind = TRUE)[1]
      #idx_end = which(sprintf("%03d", 352) == day_mat, arr.ind = TRUE)[1]
      
      # create the loop mat excluding the start and end index
      mat1 = expand.grid(c(idx_begin:dim(day_mat)[1]), 2000)
      mat2 = expand.grid(c(1:dim(day_mat)[1]), c(2001:2016))
      #mat3 = expand.grid(c(1:idx_end), 2016)
    }
    
    # merge mat columns
    mat1 = cbind(mat1$Var1, mat1$Var2)
    mat2 = cbind(mat2$Var1, mat2$Var2)
    #mat3 = cbind(mat3$Var1, mat3$Var2)
    
    # merge mats
    day_year = rbind(mat1, mat2)#, mat3)
    
    # initiate the variable that will contain both day, year and dir, tile
    loop_mat = c()
    
    # populate the matrix
    for (i in 1:length(input_dir_vec)) {
      # create repetitions
      dir_rep = rep(input_dir_vec[i],dim(day_year)[1])
      tile_rep = rep(tile_vec[i],dim(day_year)[1])
      
      # bind the matrices
      loop_mat = rbind(loop_mat,cbind(day_year,dir_rep,tile_rep))
    }
  }
  
  # return
  return(loop_mat)
}

mytic = function() {
  return(Sys.time())
}

mytoc = function(mt) {
  end.time <- Sys.time()
  time.taken <- paste0(round(as.numeric(difftime(end.time,mt,units = "mins")),6), " min")
  return(time.taken)
}

# function to return the date from year (x) and day of year (y), or yeardoy (x)
YearDoy2Date = function(x, y) {
  if (nchar(x)==7) { # if yeardoy is 7 digit ex. 2000224
    return(as.Date(as.numeric(substr(x,5,7)), origin = paste0(as.numeric(substr(x,1,4))-1,"-12-31")))
  } else { # if yeardoy is separated in two variables ex. x=2000, y=224
    return(as.Date(y, origin = paste0(x-1,"-12-31")))
  }
}
