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
IsDataAvailable = function(type, tile, year, day, nan_tiles_dir, output_dir, obs, composite_fname, composite_no, isMCD, product_res) {
  # set escape variable default
  result = TRUE
  
  # retrieve data available
  if (obs=="rtls") {
    fname = basename(GetFilenameVec(type, input_dir, tile, year, day, offset_days = 24))
  } else {
    fname = basename(GetFilenameVec(type, input_dir, tile, year, day, offset_days = 0))
  }
  
  # if nan tile does not exist, lets create one
  if (!file.exists(paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile.",tile,".tif"))) {
    CreateNanTileFromFile(input_name = GetFilenameVec(type, input_dir, tile, year, day, offset_days = 24)[1],
                          tile, nan_tiles_dir, isMCD, product_res)
  }
  
  # if its brf
  if (length(fname)==0 & obs == "brf") {
    
    # log the bad file
    line = paste(obs, year, day[length(day)], tile, sep=",")
    write(line, file=paste0(output_dir,"missing_files.txt"), append=TRUE)
    
    # load a brick of 9 nan bands (equivalent to band1-8 and number of pixels)
    #b = brick(lapply(c(1:9), FUN=function(x) raster(paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile.",tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"))))
    b = GetNanTile(tile, nan_tiles_dir, isMCD, product_res)
    
    # save the nan tile as the processed tile
    #writeRaster(b, filename=paste0(output_dir, composite_fname, ".", tile, ".", year, day[length(day)], ".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    SaveProcessedTileComposite(b, output_dir, composite_fname, tile, year, day, composite_no, product_res)
    
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
    #b = brick(lapply(c(1:9), FUN=function(x) raster(paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile.",tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"))))
    b = GetNanTile(tile, nan_tiles_dir, isMCD, product_res)
    
    # save the nan tile as the processed tile
    #writeRaster(b, filename=paste0(output_dir, composite_fname, ".", tile, ".", year, day[length(day)], ".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    SaveProcessedTileComposite(b, output_dir, composite_fname, tile, year, day, composite_no, product_res)
    
    # message
    print(paste0(Sys.time(), ": Couldn't find ", obs ," tile ", tile, ", year ", year, ", and day ", day[length(day)],". Going to next iteration..."))
    
    # go to the next iteration
    result = FALSE
  }
  
  # return
  return(result)
}

# function to create nan tiles from a hdf file, to use in case the time series dont have data for one tile on a given date
GetNanTile = function(tile, nan_tiles_dir, isMCD, product_res) {
  nan_tile = raster(paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile.",tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"))
  names(nan_tile) = "nantile"
  return(nan_tile)
}

# function to create nan tiles from a hdf file, to use in case the time series dont have data for one tile on a given date
CreateNanTileFromFile = function(input_name, tile, nan_tiles_dir, isMCD, product_res) {
  # check if nan tile already exists
  if (!file.exists(paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""), "nantile",".",tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"))) {
    # create tmpnanfiles directory
    dir.create(file.path(nan_tiles_dir, "tmpnanfiles/"), showWarnings = FALSE)
    
    # convert
    # get_subdatasets(input_name)
    # old maiac 500 = 7, MCD is 19-25
    gdal_translate(src_dataset = input_name,
                   dst_dataset = paste0(nan_tiles_dir,"tmpnanfiles/",ifelse(isMCD,"MCD19A1_",""),tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"),
                   verbose=F, #sds=TRUE,
                   sd_index = ifelse(product_res == 1000, 1, ifelse(!isMCD, 7, 19)))
    
    # open and assign nan
    r = raster(paste0(nan_tiles_dir,"tmpnanfiles/",ifelse(isMCD,"MCD19A1_",""),tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"))
    r[]=NaN
    
    # save
    writeRaster(r, filename=paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile.",tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    
    # delete tmpnanfiles directory
    unlink(file.path(nan_tiles_dir, "tmpnanfiles/"), recursive=TRUE)
  } else {
    r = raster(paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile.",tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"))
  }
  return(r)
}

# function to create nan tiles, to use in case the time series dont have data for one tile on a given date
# NOTE: this function is for the old MAIAC files pre-MCD19A1 - NOT USED ANYMORE
CreateNanTiles = function(tile, nan_tiles_dir, latlon_tiles_dir, isMCD) {
  # check if nan tile already exists
  if (!file.exists(paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile",".",tile,ifelse(product_res != 1000,paste0("_",product_res),""),".tif"))) {
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
    writeRaster(r, filename=paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile.",tile,".tif"), format="GTiff", datatype='INT2S', overwrite=TRUE)
    
    # delete tmpnanfiles directory
    unlink(file.path(nan_tiles_dir, "tmpnanfiles/"), recursive=TRUE)
  } else {
    r = raster(paste0(nan_tiles_dir,ifelse(isMCD,"MCD19A1_",""),"nantile.",tile,".tif"))
  }
  return(r)
}

# function to get filenames of each 8-day product or parameters files from a product "x", from a input directory "input_dir", of a respective "tile", "year" and day vector "day"
GetFilenameVec = function(type, input_dir, tile, year, day, offset_days) {
  # initiate some things
  result = c()
  continue_processing=TRUE
  offset_i = 0
  day2 = day
  
  # loop to find rtls and increase offset on each iteration
  while(length(result)==0 & continue_processing) {
    # create combinatios of product, tile, year and day
    if (any(type == "MCD19A1") | any(type == "MCD19A3D")) {
      combinations = expand.grid(type, paste0(".A", year, sprintf("%03s",day2),"."), tile)
    } else {
      combinations = expand.grid(type, paste0(".",tile,"."), year, sprintf("%03s",day2))
    }
    
    # merge the combinations
    combinations = paste0(combinations$Var1, combinations$Var2, combinations$Var3, combinations$Var4)
    
    # get files from input_dir
    result = list.files(input_dir, pattern=paste(combinations,collapse="|"), recursive = TRUE, full.names = TRUE)
    result = grep("*.hdf$", result, value=T)
    
    # if there is results, just finish processing
    if (length(result)>0) {
      continue_processing=FALSE
    } else {
      # if no results and its rtls try to increase offset
      if (type[1] == "MAIACRTLS" | type[1] == "MCD19A3") {
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
ConvertHDF2TIF = function(product_fname, parameter_fname, input_dir, output_dir, tmp_dir, no_cores, log_fname, is_ea_filter, is_qa_filter, process_dir, isMCD, product_res) {
  value = TRUE
  # message
  print(paste0(Sys.time(), ": Converting HDFs to TIF in parallel..."))
  
  # measure time
  t1 = mytic()
  
  # define which sds according to maiac product
  # to find the indices: get_subdatasets(product_fname[1])
  # get_subdatasets("E:\\Maiac_raw_data_2019\\MCD19A1.A2019001.h11v10.006.2019009051517.hdf")
  if (isMCD) {
    if (product_res == 1000) {
      brf_num = 1:8
    } else {
      brf_num = 19:25
    }
    sds_to_retrieve_brf_num = c(brf_num,33,34)
    sds_to_retrieve_brf = sprintf("%02d",sds_to_retrieve_brf_num)
    if (is_ea_filter)
      sds_to_retrieve_brf = c(sds_to_retrieve_brf,"31")
    if (is_qa_filter)
      sds_to_retrieve_brf = c(sds_to_retrieve_brf,"17")
    sds_to_retrieve_rtls = c("1","2","3")
    
    # create sds_suffix and prefix
    sds_preffix = "HDF4_EOS:EOS_GRID:"
    #sds_suffix_brf = c(":grid1km:Sur_refl1",":grid1km:Sur_refl2",":grid1km:Sur_refl3",":grid1km:Sur_refl4",":grid1km:Sur_refl5",":grid1km:Sur_refl6",":grid1km:Sur_refl7",":grid1km:Sur_refl8",":grid1km:Sur_refl9",":grid1km:Sur_refl10",":grid1km:Sur_refl11",":grid1km:Sur_refl12",":grid1km:Sigma_BRFn1",":grid1km:Sigma_BRFn2",":grid1km:Snow_Fraction",":grid1km:Snow_Grain_Size",":grid1km:Snow_Fit",":grid1km:Status_QA",":grid500m:Sur_refl_500m1",":grid500m:Sur_refl_500m2",":grid500m:Sur_refl_500m3",":grid500m:Sur_refl_500m4",":grid500m:Sur_refl_500m5",":grid500m:Sur_refl_500m6",":grid500m:Sur_refl_500m7",":grid5km:cosSZA",":grid5km:cosVZA",":grid5km:RelAZ",":grid5km:Scattering_Angle",":grid5km:SAZ",":grid5km:VAZ",":grid5km:Glint_Angle",":grid5km:Fv",":grid5km:Fg") # v6 or prior (?)
    sds_suffix_brf = c(":grid1km:Sur_refl1",":grid1km:Sur_refl2",":grid1km:Sur_refl3",":grid1km:Sur_refl4",":grid1km:Sur_refl5",":grid1km:Sur_refl6",":grid1km:Sur_refl7",":grid1km:Sur_refl8",":grid1km:Sur_refl9",":grid1km:Sur_refl10",":grid1km:Sur_refl11",":grid1km:Sur_refl12",":grid1km:Sigma_BRFn1",":grid1km:Sigma_BRFn2",":grid1km:Snow_Fraction",":grid1km:Snow_Grain_Size",":grid1km:Status_QA",":grid1km:Snow_Fit",":grid500m:Sur_refl_500m1",":grid500m:Sur_refl_500m2",":grid500m:Sur_refl_500m3",":grid500m:Sur_refl_500m4",":grid500m:Sur_refl_500m5",":grid500m:Sur_refl_500m6",":grid500m:Sur_refl_500m7",":grid5km:cosSZA",":grid5km:cosVZA",":grid5km:RelAZ",":grid5km:Scattering_Angle",":grid5km:SAZ",":grid5km:VAZ",":grid5km:Glint_Angle",":grid5km:Fv",":grid5km:Fg") # v6.1
    sds_suffix_rtls = c(":grid1km:Kiso",":grid1km:Kvol",":grid1km:Kgeo",":grid1km:sur_albedo",":UpdateDay")
  } else {
    # SDS numbers
    brf_num = ifelse(product_res == 1000, "01", "07")
    sds_to_retrieve_brf = c(brf_num,"15","16")
    if (is_ea_filter)
      sds_to_retrieve_brf = c(sds_to_retrieve_brf,"13")
    if (is_qa_filter)
      sds_to_retrieve_brf = c(sds_to_retrieve_brf,"06")
    sds_to_retrieve_rtls = c("1","2","3")
    
    # create sds_suffix and prefix
    sds_preffix = "HDF4_EOS:EOS_GRID:"
    sds_suffix_brf = c(":grid1km:sur_refl",":grid1km:Sigma_BRFn",":grid1km:Snow_Fraction",":grid1km:Snow_Grain_Diameter",":grid1km:Snow_Fit",":grid1km:Status_QA",":grid500m:sur_refl_500m",":grid5km:cosSZA",":grid5km:cosVZA",":grid5km:RelAZ",":grid5km:Scattering_Angle",":grid5km:Glint_Angle",":grid5km:SAZ",":grid5km:VAZ",":grid5km:Fv",":grid5km:Fg")
    sds_suffix_rtls = c(":grid1km:Kiso",":grid1km:Kvol",":grid1km:Kgeo",":grid1km:sur_albedo",":UpdateDay")
  }
  
  # create sds_to_retrieve list
  sds_to_retrieve_mat = rbind.fill.matrix(matrix(sds_to_retrieve_brf,length(product_fname),length(sds_to_retrieve_brf), byrow=T),matrix(sds_to_retrieve_rtls,length(parameter_fname),length(sds_to_retrieve_rtls), byrow=T))
  
  # merge x and y
  x = c(product_fname,parameter_fname)
  
  # Initiate cluster
  cl = parallel::makeCluster(min(length(x), no_cores))
  registerDoParallel(cl)
  objects_to_export = c("x", "input_dir", "output_dir", "tmp_dir", "sds_to_retrieve_mat", "sds_preffix", "sds_suffix_brf","sds_suffix_rtls")
  
  # loop through the files
  f = foreach(i = 1:length(x), .packages=c("raster","gdalUtils","rgdal","RCurl"), .export=objects_to_export, .errorhandling="remove") %dopar% {
    #for(i in 1:length(x)) {
    value = TRUE
    # adjust output filename in case the product name has folder in the beggining
    x1 = basename(x[i])
    
    # check if x[i] converted tif file exists
    # if it does, just throw some message, otherwise try to convert again
    if (any(!file.exists(paste0(tmp_dir,x1,"_",na.omit(sds_to_retrieve_mat[i,]),".tif")))) {
      # message
      print(paste0(Sys.time(), ": Converting HDF to TIF file ",i," from ",length(x)," -> ",x1))
      
      # get the sds list
      #sds_list = get_subdatasets(paste0(x[i]))  # slooooow
      #sds_list = paste0(sds_preffix,paste0(input_dir,x[i]),sds_suffix)  # fast!
      # different than "1" means it is not the RTLS parameters
      if (sds_to_retrieve_mat[i,1] != "1") { 
        sds_list = paste0(sds_preffix,x[i],sds_suffix_brf)  # fast!
      } else {
        sds_list = paste0(sds_preffix,x[i],sds_suffix_rtls)  # fast!
      }
      
      # retrieve one sub-data set each time
      for (j in 1:length(na.omit(sds_to_retrieve_mat[i,]))) { #sprintf("%02d",sds_to_retrieve_mat[i,][j])
        if (any(as.numeric(sds_to_retrieve_mat[i,][j]) == c(15,16,33,34))) {
          gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=T, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]), a_nodata=-99999)
        } else
          gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=T, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]))
      }
      
      # check if file exists after converting
      # if it doesn't, it can mean two things: (1) converting was somehow interrupted -> convert again, or (2) HDF is corrupted -> download again
      try_count = 0
      while (any(!file.exists(paste0(tmp_dir,x1,"_",na.omit(sds_to_retrieve_mat[i,]),".tif"))) & try_count < 1) {
        
        # try to convert again
        for (j in 1:length(na.omit(sds_to_retrieve_mat[i,]))) { #sprintf("%02d",sds_to_retrieve_mat[i,][j])
          if (any(as.numeric(sds_to_retrieve_mat[i,][j]) == c(15,16,33,34))) {
            gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=F, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]), a_nodata=-99999)
          } else
            gdal_translate(sds_list[as.numeric(sds_to_retrieve_mat[i,][j])], dst_dataset = paste0(tmp_dir,x1,"_",sds_to_retrieve_mat[i,][j],".tif"), verbose=F, sdindex=as.numeric(sds_to_retrieve_mat[i,][j]))
        }
        
        # counter
        try_count = try_count + 1
        
        # check if the file was extracted
        if (all(file.exists(paste0(tmp_dir,x1,"_",na.omit(sds_to_retrieve_mat[i,]),".tif")))) {
          print(paste0(Sys.time(), ": File ",x1," was extracted with sucess. Error avoided (i hope), oh yeah!"))
          next
        }
        
      }
      
      # file does not exist... report in a .txt
      if (any(!file.exists(paste0(tmp_dir,x1,"_",na.omit(sds_to_retrieve_mat[i,]),".tif")))) {
        print(paste0(Sys.time(), ": ERROR on file ",i," from ",length(x),", could not convert hdf2tif -> ",x1))
        write(x1, file=paste0(process_dir,"hdf2tif_convert_fail.txt"), append=TRUE)
        value = FALSE
      }
      
    } else {
      print(paste0(Sys.time(), ": File ",i," from ",length(x)," is already converted to tif -> ",x1))
    }
    
    c(value)
  }
  
  # finish cluster
  stopCluster(cl)
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  if (all(f)) {
    print(paste0(Sys.time(), ": Converting HDFs to TIF in parallel finished in ", t2))
  } else {
    print(paste0(Sys.time(), ": Converting HDFs to TIF in parallel returned some ERRORS, finished in ", t2))
    value = FALSE
  }
  
  return(value)
}

ConvertHDF2TIF = cmpfun(ConvertHDF2TIF)

# function to load files of a specific type from the temporary output folder "output_dir"/tmp
LoadMAIACFiles = function(raster_filename, output_dir, tmp_dir, type, isMCD) {
  # list of bricks
  raster_brick = list()
  
  # adjust the 'type' variable for the brf according to resolution and product
  if (is.numeric(type)) {
    product_res = type
    
    if (product_res == 1000) {
      type = "sur_refl"
    } else {
      if (isMCD) {
        type = "Sur_refl_500m"
      } else {
        type = "sur_refl_500m"
      }
    }
  }
  
  # for the new MCD product
  if (isMCD) {
    # name and order vector of the subdatasets
    #science_dataset_names = c("sur_refl1","sur_refl2","sur_refl3","sur_refl4","sur_refl5","sur_refl6","sur_refl7","sur_refl8","sur_refl9","sur_refl10","sur_refl11","sur_refl12","Sigma_BRFn1","Sigma_BRFn2","Snow_Fraction","Snow_Grain_Size","Snow_Fit","Status_QA","Sur_refl_500m1","Sur_refl_500m2","Sur_refl_500m3","Sur_refl_500m4","Sur_refl_500m5","Sur_refl_500m6","Sur_refl_500m7","cosSZA","cosVZA","RelAZ","Scattering_Angle","SAZ","VAZ","Glint_Angle","Fv","Fg") # v6 or prior (?)
    science_dataset_names = c("sur_refl1","sur_refl2","sur_refl3","sur_refl4","sur_refl5","sur_refl6","sur_refl7","sur_refl8","sur_refl9","sur_refl10","sur_refl11","sur_refl12","Sigma_BRFn1","Sigma_BRFn2","Snow_Fraction","Snow_Grain_Size","Status_QA","Snow_Fit","Sur_refl_500m1","Sur_refl_500m2","Sur_refl_500m3","Sur_refl_500m4","Sur_refl_500m5","Sur_refl_500m6","Sur_refl_500m7","cosSZA","cosVZA","RelAZ","Scattering_Angle","VAZ","SAZ","Glint_Angle","Fv","Fg") # v6.1
    science_dataset_parameter_names = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
    
    # identify the type
    if (type == "sur_refl") {
      type_number = sprintf("%02d", 1:8)
    } else {
      if (type == "Sur_refl_500m") {
        type_number = sprintf("%02d", 19:25)
      } else {
        type_number = sprintf("%02d", grep(paste0("^", type, "$"), science_dataset_names))
        if (length(type_number)==0)
          type_number = sprintf("%01d", grep(paste0("^", type, "$"), science_dataset_parameter_names))
        if (length(type_number)==0) {
          # message
          stop(paste0(Sys.time(), ": Can't find file type to open: ",type))
        }
      }
    }
    
    # loop though files and load the files
    i=1
    for(i in 1:length(raster_filename)) {
      # RTLS
      if (any(type == c("Kiso", "Kvol", "Kgeo"))) {
        raster_brick[[i]] = brick(paste0(tmp_dir,raster_filename[i],"_",type_number,".tif"))
      } else { # BRF
        if (type == "sur_refl" | type == "Sur_refl_500m") {
          # open each band
          j=1
          band_list = list()
          n_bands = ifelse(type == "sur_refl", 8, 7)
          for (j in 1:n_bands) {
            band_list[[j]] = brick(paste0(tmp_dir,raster_filename[i],"_",type_number[j],".tif"))
          }
          # now we stack the bands according to the number of observations in this DOY
          obs_list = list()
          for (j in 1:nlayers(band_list[[1]])) {
            if (n_bands == 8) {
              obs_list[[j]] = stack(band_list[[1]][[j]], band_list[[2]][[j]], band_list[[3]][[j]], band_list[[4]][[j]], band_list[[5]][[j]], band_list[[6]][[j]], band_list[[7]][[j]], band_list[[8]][[j]])
            } else {
              obs_list[[j]] = stack(band_list[[1]][[j]], band_list[[2]][[j]], band_list[[3]][[j]], band_list[[4]][[j]], band_list[[5]][[j]], band_list[[6]][[j]], band_list[[7]][[j]])  
            }
          }
          for (j in 1:length(obs_list)) {
            raster_brick[[length(raster_brick)+1]] = obs_list[[j]]
          }
          
        } else {
          tmp = brick(paste0(tmp_dir,raster_filename[i],"_",type_number,".tif"))
          for (j in 1:nlayers(tmp)) {
            raster_brick[[length(raster_brick)+1]] = tmp[[j]]
          }
        }
      }
    }
    
  } else { # for the old MAIAC product
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
    
    # loop though files and load the files
    for(i in 1:length(raster_filename)) {
      raster_brick[[i]] = brick(paste0(tmp_dir,raster_filename[i],"_",type_number,".tif"))
    }
    
  }
  
  # return
  return(raster_brick)
}

# function to load files of a specific type from the temporary output folder "output_dir"/tmp
LoadMAIACFilesGDAL = function(raster_filename, output_dir, tmp_dir, type, isMCD, product_type) {
  # list of bricks
  raster_brick = list()
  
  # adjust the 'type' variable for the brf according to resolution and product
  if (is.numeric(type)) {
    product_res = type
    
    if (product_res == 1000) {
      type = "sur_refl"
    } else {
      if (isMCD) {
        type = "Sur_refl_500m"
      } else {
        type = "sur_refl_500m"
      }
    }
  }
  
  
  # name and order vector of the subdatasets
  #science_dataset_names = c("sur_refl1","sur_refl2","sur_refl3","sur_refl4","sur_refl5","sur_refl6","sur_refl7","sur_refl8","sur_refl9","sur_refl10","sur_refl11","sur_refl12","Sigma_BRFn1","Sigma_BRFn2","Snow_Fraction","Snow_Grain_Size","Snow_Fit","Status_QA","Sur_refl_500m1","Sur_refl_500m2","Sur_refl_500m3","Sur_refl_500m4","Sur_refl_500m5","Sur_refl_500m6","Sur_refl_500m7","cosSZA","cosVZA","RelAZ","Scattering_Angle","SAZ","VAZ","Glint_Angle","Fv","Fg") # v6 or prior (?)
  science_dataset_names = c("sur_refl1","sur_refl2","sur_refl3","sur_refl4","sur_refl5","sur_refl6","sur_refl7","sur_refl8","sur_refl9","sur_refl10","sur_refl11","sur_refl12","Sigma_BRFn1","Sigma_BRFn2","Snow_Fraction","Snow_Grain_Size","Status_QA","Snow_Fit","Sur_refl_500m1","Sur_refl_500m2","Sur_refl_500m3","Sur_refl_500m4","Sur_refl_500m5","Sur_refl_500m6","Sur_refl_500m7","cosSZA","cosVZA","RelAZ","Scattering_Angle","VAZ","SAZ","Glint_Angle","Fv","Fg") # v6.1
  science_dataset_parameter_names = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
  
  # identify the type
  if (type == "sur_refl") {
    type_number = sprintf("%02d", 1:8)
  } else {
    if (type == "Sur_refl_500m") {
      type_number = sprintf("%02d", 19:25)
    } else {
      type_number = sprintf("%02d", grep(paste0("^", type, "$"), science_dataset_names))
      if (length(type_number)==0) type_number = sprintf("%01d", grep(paste0("^", type, "$"), science_dataset_parameter_names))
      if (length(type_number)==0) {
        # message
        stop(paste0(Sys.time(), ": Can't find file type to open: ",type))
      }
    }
  }
  
  # empty vector to store results
  observations_fnames = c()
  
  # loop though files and load the files
  i=1
  for(i in 1:length(raster_filename)) {
    
    # change number of observations depending on the product
    if (product_type == "A1") {
      # number of observations in this raster
      n_obs = nlayers(brick(paste0(tmp_dir,raster_filename[i],"_",type_number[1],".tif")))  
    } else {
      n_obs = 1
    }
    
    # load band filenames
    j=1
    fname_list = c()
    for (j in 1:length(type_number)) {
      fname_list[j] = paste0(tmp_dir,raster_filename[i],"_",type_number[j],".tif")
    }
    
    # loop observations
    obs=1
    for (obs in 1:n_obs) {
      
      # save each band separately
      fname_tif_list = c()
      for (w in 1:length(fname_list)) {
        # get the bands
        fname_tif_list[w] = paste0(tempfile(),".tif")
        gdal_translate_run = paste("gdal_translate",
                                   "-of GTiff",
                                   ifelse(product_type == "A1", paste0("-b ",obs), ""),
                                   fname_list[w],
                                   fname_tif_list[w])
        system(gdal_translate_run)
      }
      
      # create vrt
      fname_vrt = vrt_imgs(fname_tif_list, "gdalbuildvrt", add_cmd = ifelse(product_type == "A1","-separate",""))
      
      # combine them back together into one geotiff
      tmp_output_fname = paste0(tempfile(),".tif")
      gdal_translate_run = paste("gdal_translate",
                                 "-of GTiff",
                                 fname_vrt,
                                 tmp_output_fname)
      system(gdal_translate_run)
      
      # remove temp files
      file.remove(fname_tif_list)
      file.remove(fname_vrt)
      
      # assign result to the vector
      observations_fnames[length(observations_fnames)+1] = tmp_output_fname
      
    } # end observations
    
  }
  
  # return
  return(observations_fnames)
}

# function to load files of a specific type from the temporary output folder "output_dir"/tmp
LoadMAIACFilesGDALParallel = function(raster_filename, output_dir, tmp_dir, type, isMCD, product_type, dateOnly=FALSE) {
  
  # adjust the 'type' variable for the brf according to resolution and product
  if (is.numeric(type)) {
    product_res = type
    
    if (product_res == 1000) {
      type = "sur_refl"
    } else {
      if (isMCD) {
        type = "Sur_refl_500m"
      } else {
        type = "sur_refl_500m"
      }
    }
  }
  
  
  # name and order vector of the subdatasets
  if (isMCD) {
    #science_dataset_names = c("sur_refl1","sur_refl2","sur_refl3","sur_refl4","sur_refl5","sur_refl6","sur_refl7","sur_refl8","sur_refl9","sur_refl10","sur_refl11","sur_refl12","Sigma_BRFn1","Sigma_BRFn2","Snow_Fraction","Snow_Grain_Size","Snow_Fit","Status_QA","Sur_refl_500m1","Sur_refl_500m2","Sur_refl_500m3","Sur_refl_500m4","Sur_refl_500m5","Sur_refl_500m6","Sur_refl_500m7","cosSZA","cosVZA","RelAZ","Scattering_Angle","SAZ","VAZ","Glint_Angle","Fv","Fg") # v6 or prior (?)
    science_dataset_names = c("sur_refl1","sur_refl2","sur_refl3","sur_refl4","sur_refl5","sur_refl6","sur_refl7","sur_refl8","sur_refl9","sur_refl10","sur_refl11","sur_refl12","Sigma_BRFn1","Sigma_BRFn2","Snow_Fraction","Snow_Grain_Size","Status_QA","Snow_Fit","Sur_refl_500m1","Sur_refl_500m2","Sur_refl_500m3","Sur_refl_500m4","Sur_refl_500m5","Sur_refl_500m6","Sur_refl_500m7","cosSZA","cosVZA","RelAZ","Scattering_Angle","VAZ","SAZ","Glint_Angle","Fv","Fg") # v6.1
    science_dataset_parameter_names = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
  } else {
    science_dataset_names = c(":grid1km:sur_refl",":grid1km:Sigma_BRFn",":grid1km:Snow_Fraction",":grid1km:Snow_Grain_Diameter",":grid1km:Snow_Fit",":grid1km:Status_QA",":grid500m:sur_refl_500m",":grid5km:cosSZA",":grid5km:cosVZA",":grid5km:RelAZ",":grid5km:Scattering_Angle",":grid5km:Glint_Angle",":grid5km:SAZ",":grid5km:VAZ",":grid5km:Fv",":grid5km:Fg")
    science_dataset_parameter_names = c(":grid1km:Kiso",":grid1km:Kvol",":grid1km:Kgeo",":grid1km:sur_albedo",":UpdateDay")
  }
  
  # identify the type
  if (isMCD) {
    if (type == "sur_refl") {
      type_number = sprintf("%02d", 1:8)
    } else {
      if (type == "Sur_refl_500m") {
        type_number = sprintf("%02d", 19:25)
      } else {
        type_number = sprintf("%02d", grep(paste0("^", type, "$"), science_dataset_names))
        if (length(type_number)==0) type_number = sprintf("%01d", grep(paste0("^", type, "$"), science_dataset_parameter_names))
        if (length(type_number)==0) {
          # message
          stop(paste0(Sys.time(), ": Can't find file type to open: ",type))
        }
      }
    }
  } else {
    # experimental maiac
    if (type == "sur_refl") {
      type_number = sprintf("%02d", 1)
    } else {
      type_number = 1:3
      type_number = sprintf("%02d", grep(paste0("^", type, "$"), science_dataset_names))
    }
    
  }
  
  # create vector of filenames for the temporary files
  if (TRUE) {
    # empty vector to store results
    observations_fnames = list()
    
    # loop though files and load the files
    i=1
    for (i in 1:length(raster_filename)) {
      
      # change number of observations depending on the product
      if (product_type == "A1") {
        # number of observations in this raster
        n_obs = nlayers(brick(paste0(tmp_dir,raster_filename[i],"_",type_number[1],".tif")))  
      } else {
        n_obs = 1
      }
      
      # create vec
      observations_fnames[[i]] = vector("character", n_obs)
      
      # create temp file
      for (obs in 1:n_obs) {
        # tmp filename
        tmp_output_fname = paste0(tempfile(),".tif")
        
        # assign result to the vector
        observations_fnames[[i]][obs] = tmp_output_fname
        
      }
    }
  } # end filename creation
  
  # get date
  if (dateOnly) {
    date_vec = c()
    for (k in 1:length(observations_fnames)) {
      tmp = length(observations_fnames[[k]])
      date_vec = c(date_vec, rep(k, tmp))
    }
    return(date_vec)
  }
  
  # loop though files and load the files
  i=1
  tif_organize = function(i) {
    
    # change number of observations depending on the product
    if (product_type == "A1") {
      # number of observations in this raster
      n_obs = nlayers(brick(paste0(tmp_dir,raster_filename[i],"_",type_number[1],".tif")))  
    } else {
      n_obs = 1
    }
    
    # load band filenames
    j=1
    fname_list = c()
    for (j in 1:length(type_number)) {
      fname_list[j] = paste0(tmp_dir,raster_filename[i],"_",type_number[j],".tif")
    }
    
    # loop observations
    obs=1
    for (obs in 1:n_obs) {
      
      # save each band separately
      fname_tif_list = c()
      w=1
      for (w in 1:length(fname_list)) {
        # get the bands
        fname_tif_list[w] = paste0(tempfile(),".tif")
        gdal_translate_run = paste("gdal_translate",
                                   "-of GTiff",
                                   "-q",
                                   ifelse(product_type == "A1", paste0("-b ",obs), ""),
                                   fname_list[w],
                                   fname_tif_list[w])
        system(gdal_translate_run)
      }
      
      # create vrt
      fname_vrt = vrt_imgs(fname_tif_list, "gdalbuildvrt", add_cmd = ifelse(product_type == "A1","-separate",""))
      
      # combine them back together into one geotiff
      #tmp_output_fname = paste0(tempfile(),".tif")
      tmp_output_fname = observations_fnames[[i]][obs]
      gdal_translate_run = paste("gdal_translate",
                                 "-of GTiff",
                                 "-q",
                                 fname_vrt,
                                 tmp_output_fname)
      system(gdal_translate_run)
      
      # remove temp files
      file.remove(fname_tif_list)
      file.remove(fname_vrt)
      
      # assign result to the vector
      #observations_fnames[length(observations_fnames)+1] = tmp_output_fname
      
    } # end observations
    
  }
  
  # run in parallel
  snowrun(fun = tif_organize,
          values = 1:length(raster_filename),
          no_cores = no_cores,
          var_export = c("raster_filename", "tmp_dir", "product_type", "type_number", "observations_fnames", "vrt_imgs"),
          pack_export = "raster")
  
  
  # unlist
  observations_fnames = unlist(observations_fnames)
  #file.exists(observations_fnames)
  #file.remove(observations_fnames)
  
  
  # return
  return(observations_fnames)
}

# function to load files of a specific type from the temporary output folder "output_dir"/tmp
LoadMAIACFilesGDALParallel_nonMCD = function(raster_filename, output_dir, tmp_dir, type, isMCD, product_type, dateOnly=FALSE) {
  
  # adjust the 'type' variable for the brf according to resolution and product
  if (is.numeric(type)) {
    product_res = type
    
    if (product_res == 1000) {
      type = "sur_refl"
    } else {
      if (isMCD) {
        type = "Sur_refl_500m"
      } else {
        type = "sur_refl_500m"
      }
    }
  }
  
  
  # name and order vector of the subdatasets
  if (isMCD) {
    #science_dataset_names = c("sur_refl1","sur_refl2","sur_refl3","sur_refl4","sur_refl5","sur_refl6","sur_refl7","sur_refl8","sur_refl9","sur_refl10","sur_refl11","sur_refl12","Sigma_BRFn1","Sigma_BRFn2","Snow_Fraction","Snow_Grain_Size","Snow_Fit","Status_QA","Sur_refl_500m1","Sur_refl_500m2","Sur_refl_500m3","Sur_refl_500m4","Sur_refl_500m5","Sur_refl_500m6","Sur_refl_500m7","cosSZA","cosVZA","RelAZ","Scattering_Angle","SAZ","VAZ","Glint_Angle","Fv","Fg") # v6 or prior (?)
    science_dataset_names = c("sur_refl1","sur_refl2","sur_refl3","sur_refl4","sur_refl5","sur_refl6","sur_refl7","sur_refl8","sur_refl9","sur_refl10","sur_refl11","sur_refl12","Sigma_BRFn1","Sigma_BRFn2","Snow_Fraction","Snow_Grain_Size","Status_QA","Snow_Fit","Sur_refl_500m1","Sur_refl_500m2","Sur_refl_500m3","Sur_refl_500m4","Sur_refl_500m5","Sur_refl_500m6","Sur_refl_500m7","cosSZA","cosVZA","RelAZ","Scattering_Angle","VAZ","SAZ","Glint_Angle","Fv","Fg") # v6.1
    science_dataset_parameter_names = c("Kiso", "Kvol", "Kgeo", "sur_albedo", "UpdateDay")
  } else {
    science_dataset_names = c(":grid1km:sur_refl",":grid1km:Sigma_BRFn",":grid1km:Snow_Fraction",":grid1km:Snow_Grain_Diameter",":grid1km:Snow_Fit",":grid1km:Status_QA",":grid500m:sur_refl_500m",":grid5km:cosSZA",":grid5km:cosVZA",":grid5km:RelAZ",":grid5km:Scattering_Angle",":grid5km:Glint_Angle",":grid5km:SAZ",":grid5km:VAZ",":grid5km:Fv",":grid5km:Fg")
    science_dataset_parameter_names = c(":grid1km:Kiso",":grid1km:Kvol",":grid1km:Kgeo",":grid1km:sur_albedo",":UpdateDay")
  }
  
  # identify the type
  if (isMCD) {
    if (type == "sur_refl") {
      type_number = sprintf("%02d", 1:8)
    } else {
      if (type == "Sur_refl_500m") {
        type_number = sprintf("%02d", 19:25)
      } else {
        type_number = sprintf("%02d", grep(paste0("^", type, "$"), science_dataset_names))
        if (length(type_number)==0) type_number = sprintf("%01d", grep(paste0("^", type, "$"), science_dataset_parameter_names))
        if (length(type_number)==0) {
          # message
          stop(paste0(Sys.time(), ": Can't find file type to open: ",type))
        }
      }
    }
  } else {
    # experimental maiac
    if (product_type == "A1") {
      if (type == "sur_refl") {
        type_number = sprintf("%02d", 1)
      } else {
        #type_number = sprintf("%02d", grep(paste0("^", type, "$"), science_dataset_names))
        type_number = sprintf("%02d", grep(paste0(type, "$"), science_dataset_names))
      }
    } else {
      type_number = sprintf("%01d", grep(paste0(type, "$"), science_dataset_parameter_names))
    }
    
  }
  
  ## for experimental maiac, the surface reflectance bands are already in one geotiff, we dont need to combine them
  ## we just need to list them
  
  # create vector of filenames for the temporary files
  if (!isMCD) {
    
    # empty vector to store results
    observations_fnames = c()
    
    # loop though files and load the files
    i=1
    for (i in 1:length(raster_filename)) {
      
      # get filename
      observations_fnames[i] = paste0(tmp_dir,raster_filename[i],"_",type_number,".tif")
      
    }
    
    return(observations_fnames)
    
  } # end filename creation
  
  # get date
  if (dateOnly) {
    date_vec = c()
    for (k in 1:length(observations_fnames)) {
      tmp = length(observations_fnames[[k]])
      date_vec = c(date_vec, rep(k, tmp))
    }
    return(date_vec)
  }
  
  # loop though files and load the files
  i=1
  tif_organize = function(i) {
    
    # change number of observations depending on the product
    if (product_type == "A1") {
      # number of observations in this raster
      n_obs = nlayers(brick(paste0(tmp_dir,raster_filename[i],"_",type_number[1],".tif")))  
    } else {
      n_obs = 1
    }
    
    # load band filenames
    j=1
    fname_list = c()
    for (j in 1:length(type_number)) {
      fname_list[j] = paste0(tmp_dir,raster_filename[i],"_",type_number[j],".tif")
    }
    
    # loop observations
    obs=1
    for (obs in 1:n_obs) {
      
      # save each band separately
      fname_tif_list = c()
      w=1
      for (w in 1:length(fname_list)) {
        # get the bands
        fname_tif_list[w] = paste0(tempfile(),".tif")
        gdal_translate_run = paste("gdal_translate",
                                   "-of GTiff",
                                   "-q",
                                   ifelse(product_type == "A1", paste0("-b ",obs), ""),
                                   fname_list[w],
                                   fname_tif_list[w])
        system(gdal_translate_run)
      }
      
      # create vrt
      fname_vrt = vrt_imgs(fname_tif_list, "gdalbuildvrt", add_cmd = ifelse(product_type == "A1","-separate",""))
      
      # combine them back together into one geotiff
      #tmp_output_fname = paste0(tempfile(),".tif")
      tmp_output_fname = observations_fnames[[i]][obs]
      gdal_translate_run = paste("gdal_translate",
                                 "-of GTiff",
                                 "-q",
                                 fname_vrt,
                                 tmp_output_fname)
      system(gdal_translate_run)
      
      # remove temp files
      file.remove(fname_tif_list)
      file.remove(fname_vrt)
      
      # assign result to the vector
      #observations_fnames[length(observations_fnames)+1] = tmp_output_fname
      
    } # end observations
    
  }
  
  # run in parallel
  snowrun(fun = tif_organize,
          values = 1:length(raster_filename),
          no_cores = no_cores,
          var_export = c("raster_filename", "tmp_dir", "product_type", "type_number", "observations_fnames", "vrt_imgs"),
          pack_export = "raster")
  
  
  # unlist
  observations_fnames = unlist(observations_fnames)
  #file.exists(observations_fnames)
  #file.remove(observations_fnames)
  
  
  # return
  return(observations_fnames)
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
ConvertBRFNadir = function(BRF, FV, FG, kL, kV, kG, tile, year, output_dir, no_cores, log_fname, view_geometry, isMCD, product_res, tmp_dir) {
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
  
  # fix FV names
  for (i in 1:length(FV)) {
    if (substr(names(FV[[i]]), 1,3) != "MCD") {
      names(FV[[i]]) = basename(FV[[i]]@file@name)
    }
  }
  
  # define RTLS day name location in the string
  if (isMCD) {
    rtls_day_str_begin = 14
    rtls_day_str_end = 16
  } else {
    rtls_day_str_begin = 22
    rtls_day_str_end = 24
  }
  
  # retrieve RTLS day
  rtls_day_vec = vector()
  for (i in 1:length(kL)) {
    rtls_day_vec[i] = as.numeric(substr(names(kL[[i]])[[1]],rtls_day_str_begin+1,rtls_day_str_end+1))
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
  objects_to_export = c("BRF", "FV", "FG", "kL", "kV", "kG", "tile", "year", "rtls_day_vec", "ff", "FilterValOutRangeToNA", "product_res")
  
  # for each date
  tmp_file_names = c()
  for (i in 1:length(BRF)) tmp_file_names[i] = paste0(tmp_dir, basename(tempfile()))
  BRFn = foreach(i = 1:length(BRF), .packages=c("raster"), .export=objects_to_export, .errorhandling="remove", .inorder = FALSE) %dopar% {
    # message
    print(paste0(Sys.time(), ": Normalizing brf iteration ",i," from ",length(BRF)))
    
    # set parameters, interpolate the 5km to 1 km by nearest neighbor
    dis_fac_5 = 5000 / product_res
    FVi = disaggregate(FV[[i]], fact=c(dis_fac_5,dis_fac_5)) # fac = 5 for 1km and 10 for 0.5 km
    
    # check if FVi is available
    if (is.na(minValue(FVi)) & is.na(maxValue(FVi)))
      return(0)
    
    # identify which tile is the given i data and set the parameters to the tile
    img_day = as.numeric(substr(names(FVi),rtls_day_str_begin,rtls_day_str_end))
    
    # get rtls days that are lower than image day, and that are near the image day until 8 days (the aggreagated rtls), get always the lowest rlts day (index 1)
    if (isMCD) {
      #idx = which(img_day >= rtls_day_vec & abs(rtls_day_vec - img_day) < 8)[1] # v6
      idx = which(img_day == rtls_day_vec) # v6.1
    } else {
      idx = which(img_day <= rtls_day_vec & abs(rtls_day_vec - img_day) <= 8)[1]
    }
    #print(idx)
    #print(img_day)
    #print(rtls_day_vec[idx])
    #i=i+1
    
    # if there is no rtls available, get the closest one and log it
    if (is.na(idx)) {
      idx = which(min(abs(img_day - rtls_day_vec)) == abs(img_day - rtls_day_vec))
      line = paste0("Tile: ", tile,", Year: ", year,", Image day: ", img_day,", RTLS day: ", rtls_day_vec[idx])
      write(line,file=paste0(output_dir, "processed_closest_rtls.txt"), append=TRUE)
    }
    
    # calculate brf nadir
    if (product_res == 1000) {
      c(overlay(subset(BRF[[i]],1:8), kL[[idx]], kV[[idx]], kG[[idx]], FVi, FGi = disaggregate(FG[[i]], fact=c(dis_fac_5,dis_fac_5)), fun=ff))
    } else {
      dis_fac_1 = 1000 / product_res
      band_subset = 1:7
      c(overlay(subset(BRF[[i]],band_subset), disaggregate(subset(kL[[idx]], band_subset), fact=c(dis_fac_1,dis_fac_1)), disaggregate(subset(kV[[idx]], band_subset), fact=c(dis_fac_1,dis_fac_1)), disaggregate(subset(kG[[idx]], band_subset), fact=c(dis_fac_1,dis_fac_1)), FVi, FGi = disaggregate(FG[[i]], fact=c(dis_fac_5,dis_fac_5)), fun=ff
                ,filename = tmp_file_names[i]
      ))
    }
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

# function to convert brf to brfn
# to do: arquivo vira float depois da covnersao, transformar em outro formato? (ex. int2s)
# transf para int2s parece que ferra os valores
# band values are calculated ok, tested calculating one band separatedely and compared to the batch convert
ConvertBRFNadirGDAL = function(BRF, FV, FG, kL, kV, kG, tile, year, output_dir, no_cores, log_fname, view_geometry, isMCD, product_res, tmp_dir) {
  # BRF = brf_reflectance  # (12 bandas por data, 1km)
  # FV = brf_fv  # (1 por data, 1km)
  # FG = brf_fg  # (1 por data, 1km)
  # kL = rtls_kiso  # (8 bandas, 1km)
  # kV = rtls_kvol  # (8 bandas, 1km)
  # kG = rtls_kgeo  # (8 bandas, 1km)
  
  # message
  print(paste0(Sys.time(), ": Normalizing brf in parallel..."))
  
  # measure time
  t1 = mytic()
  
  # create output filenames
  #output_file1=c()
  output_file1=list()
  output_file2=c()
  for (i in 1:length(BRF)) {
    #output_file1[i] = paste0(tempfile(),".tif")
    output_file1[[i]] = vector("character", 8)
    for (j in 1:8) output_file1[[i]][j] = paste0(tempfile(),".tif")
    output_file2[i] = paste0(tempfile(),".tif")
  }
  
  # choose function
  if (view_geometry == "nadir") {
    coeff_Fv = -0.0220060255
    coeff_Fg = -1.10681915
  }
  if (view_geometry == "backscat") {
    coeff_Fv = 0.361657202
    coeff_Fg = 0.0174400453
  }
  if (view_geometry == "forwardscat") {
    coeff_Fv = -0.108056836
    coeff_Fg = -1.62187397
  }
  
  # function to wrap parallel normalization
  normalization_wrapper = function(i) {
    
    # normalize
    j=1
    for (j in 1:8) {
      gdal_calc_run = paste("gdal_calc.py",
                            paste0("--calc \" A * (D + (", coeff_Fv, "*E) + (", coeff_Fg,"*F))/(D + (B*E) + (C*F)) \" "),
                            "-A", BRF[i],
                            "-B", FV[i],
                            "-C", FG[i],
                            "-D", kL[i],
                            "-E", kV[i],
                            "-F", kG[i],
                            #"--allBands A",
                            paste0("--A_band=",j),
                            paste0("--B_band=",1),
                            paste0("--C_band=",1),
                            paste0("--D_band=",j),
                            paste0("--E_band=",j),
                            paste0("--F_band=",j),
                            "--format GTiff",
                            #"--hideNoData",
                            "--type UInt16",
                            "--overwrite",
                            "--quiet",
                            "--outfile", output_file1[[i]][j])
      system(gdal_calc_run)
    }
    
    # stack the bands
    output_file1_vrt = vrt_imgs(output_file1[[i]], "gdalbuildvrt", add_cmd = "-separate")
    
    # filter data in the correct range, 0 to 10000
    gdal_calc_run = paste("gdal_calc.py",
                          paste0("--calc \" logical_and(A>=0, A<=10000)*A \" "),
                          #"-A", output_file1[i],
                          "-A", output_file1_vrt,
                          "--allBands A",
                          "--format GTiff",
                          "--overwrite",
                          "--quiet",
                          "--outfile", output_file2[i])
    system(gdal_calc_run)
    
    # remove temp filename
    file.remove(output_file1_vrt)
    file.remove(output_file1[[i]])
    
  }
  
  # debugging
  if (FALSE) {
    r = stack(output_file1[i])
    r
    plot(r[[2]])
    click(r)
    r = stack(output_file2[i])
    r
    plot(r[[2]])
    click(r)
  }
  
  # save files in parallel
  snowrun(fun = normalization_wrapper,
          values = 1:length(BRF),
          no_cores = no_cores,
          var_export = c("BRF", "FV", "FG", "kL", "kV", "kG", "coeff_Fv", "coeff_Fg", "output_file1", "output_file2", "isMCD", "vrt_imgs"),
          pack_export = NULL)
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Normalizing brf in parallel finished in ", t2))
  
  # return
  return(output_file2)
}

# compute quality based on occuring QA on the raster
# https://stevemosher.wordpress.com/2012/12/05/modis-qc-bits/
# https://lpdaac.usgs.gov/sites/default/files/public/modis/docs/MODIS_LP_QA_Tutorial-1b.pdf
# https://lpdaac.usgs.gov/documents/1500/MCD19_User_Guide_V61.pdf
# dataspec.doc
ComputeQuality = function(x) {
  # interpreting all possible QA
  qa_dataframe <- data.frame(Integer_Value = x,
                             surface = NA, #13-15
                             altitude = NA, #12
                             brfoversnow = NA, #11
                             aodtype = NA, #9-10
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
    
    # Bit13to15, surface change mask
    qa_dataframe[i,2] = paste0(qa_as_int[1:3], collapse="")
    
    # Bit12, altitude
    qa_dataframe[i,3] = paste0(qa_as_int[4], collapse="")
    
    # Bit11, brf over snow
    qa_dataframe[i,4] = paste0(qa_as_int[5], collapse="")
    
    # Bit9-10, aod type
    qa_dataframe[i,5] = paste0(qa_as_int[6:7], collapse="")
    
    # Bit8, aot level
    qa_dataframe[i,6] = paste0(qa_as_int[8], collapse="")
    
    # Bit5to7, adjacency mask
    qa_dataframe[i,7] = paste0(qa_as_int[9:11], collapse="")
    
    # Bit3to4, land water snow/ice mask
    qa_dataframe[i,8] = paste0(qa_as_int[12:13], collapse="")
    
    # Bit0to2, cloud mask
    qa_dataframe[i,9] = paste0(qa_as_int[14:16], collapse="")
  }
  
  # return
  return(qa_dataframe)
}

# filter quality table, the things to filter out the image
FilterQuality= function(qa_dataframe) {
  #qa_dataframe = qaDF
  
  # aotlevel6
  filter_list = c(6, "1") # AOT is high (> 0.6) or undefined
  
  # filter adjacency7
  filter_list = rbind(filter_list,c(7, "001")) # Adjacent to cloud
  filter_list = rbind(filter_list,c(7, "010")) # Surrounded  by more than 4 cloudy pixels
  filter_list = rbind(filter_list,c(7, "011")) # Single cloudy pixel
  
  # filter landcover8
  filter_list = rbind(filter_list,c(8, "10")) # 10--- Snow
  filter_list = rbind(filter_list,c(8, "11")) # 11 --- Ice
  
  # filter cloud9
  filter_list = rbind(filter_list,c(9, "000")) # 000 ---  Undefined
  filter_list = rbind(filter_list,c(9, "010")) # 010 --- Possibly Cloudy (detected by AOT filter)
  filter_list = rbind(filter_list,c(9, "011")) # 011 --- Cloudy  (detected by cloud mask algorithm)
  filter_list = rbind(filter_list,c(9, "101")) # 101 -- - Cloud Shadow
  #filter_list = rbind(filter_list,c(9, "110")) # 110 --- hot spot of fire
  filter_list = rbind(filter_list,c(9, "111")) # 111 --- Water Sediments
  
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
    qaDF = ComputeQuality(uniqueQA)
    
    # filter the qa dataframe with a set of determined rules excluding the bad ones
    qaDFFiltered = FilterQuality(qaDF)
    
    # create the mask using the remaining QA, rest of values become NaN
    if (dim(qaDFFiltered)[1] > 0) {
      raster_file = subs(raster_file, data.frame(id=qaDFFiltered$Integer_Value, v=rep(1,length(qaDFFiltered$Integer_Value))))
    } else {
      raster_file[]=NaN
    }
    
  } else {
    raster_file[]=NaN
  }
  
  # write the QA to a file
  tmp_file = paste0(tempfile(),".tif")
  writeRaster(raster_file, filename = tmp_file, overwrite=T)
  
  # return
  #return(raster_file)
  return(tmp_file)
}

# function to create a single qa mask based on a qa raster
CreateSingleQAMask_wrapper = function(i) {
  #x = qaBrick[[5]]
  
  # load file
  raster_file = raster(raster_brick[i])
  output_file_i = output_file[i]
  
  # get unique qa values
  uniqueQA = as.numeric(unique(raster_file))
  
  # remove nan's
  uniqueQA = uniqueQA[!is.na(uniqueQA)]
  
  if (length(uniqueQA) > 0) {
    # compute qa dataframe with all possible quality combinations in the image
    qaDF = ComputeQuality(uniqueQA)
    
    # filter the qa dataframe with a set of determined rules excluding the bad ones
    qaDFFiltered = FilterQuality(qaDF)
    
    # create the mask using the remaining QA, rest of values become NaN
    if (dim(qaDFFiltered)[1] > 0) {
      raster_file = subs(raster_file, data.frame(id=qaDFFiltered$Integer_Value, v=rep(1,length(qaDFFiltered$Integer_Value))))
    } else {
      raster_file[]=NaN
    }
    
  } else {
    raster_file[]=NaN
  }
  
  # write the QA to a file
  writeRaster(raster_file, filename = output_file_i, overwrite=T, datatype="INT2S")
  
  # return
  #return(raster_file)
  return(output_file_i)
}

# function to create a qa mask based on a qa raster brick "raster_brick"
CreateQAMask = function(raster_brick) {
  
  # message
  print(paste0(Sys.time(), ": Extracting QA..."))
  
  # measure time
  t1 = mytic()
  
  # create output filenames
  output_file=c()
  for (i in 1:length(raster_brick)) {
    output_file[i] = paste0(tempfile(),".tif")
  }
  
  # save files in parallel
  snowrun(fun = CreateSingleQAMask_wrapper,
          values = 1:length(raster_brick),
          no_cores = no_cores,
          var_export = c("raster_brick", "CreateSingleQAMask", "ComputeQuality", "FilterQuality", "output_file"),
          pack_export = "raster")
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Extracting QA finished in ", t2))
  
  # return
  return(output_file)
}

# function to apply the QA mask over a raster brick of the same size
ApplyMaskOnBrick = function(raster_brick, mask_brick) {
  
  # message
  print(paste0(Sys.time(), ": Applying mask..."))
  
  # measure time
  t1 = mytic()
  
  # # Initiate cluster
  # #cl = parallel::makeCluster(no_cores, outfile=log_fname)
  # cl = parallel::makeCluster(min(length(raster_brick), no_cores))
  # registerDoParallel(cl)
  # objects_to_export = c("raster_brick", "mask_brick")
  # 
  # # for each raster
  # masked_raster_brick = foreach(i = 1:length(raster_brick), .packages=c("raster"), .export=objects_to_export, .errorhandling="remove", .inorder = TRUE) %dopar% {
  #   # message
  #   print(paste0(Sys.time(), ": Applying mask in file ",i," from ",length(raster_brick)))
  #   
  #   # apply mask
  #   mask(raster_brick[[i]], mask_brick[[i]])
  # }
  # 
  # # finish cluster
  # stopCluster(cl)
  
  # create output filenames
  output_file=c()
  for (i in 1:length(raster_brick)) {
    output_file[i] = paste0(tempfile(),".tif")
  }
  
  # function to mask the data
  mask_brf = function(i) {
    #mask(raster_brick[[i]], mask_brick[[i]])
    
    gdal_calc_run = paste("gdal_calc.py",
                          "--calc \" (B == 1)*A \" ",
                          "-A", raster_brick[i],
                          "-B", mask_brick[i],
                          "--quiet",
                          "--allBands A",
                          "--format GTiff",
                          "--outfile", output_file[i])
    system(gdal_calc_run)
    
    
    if (FALSE) {
      plot(raster(raster_brick[i]), main="img")
      plot(raster(mask_brick[i]), main="mask")
      plot(raster(output_file[i]), main="img masked")
      click(raster(output_file[i]))
      gdalinfo(output_file[i])
    }
  }
  
  # save files in parallel
  snowrun(fun = mask_brf,
          values = 1:length(raster_brick),
          no_cores = no_cores,
          var_export = c("raster_brick", "mask_brick", "output_file"),
          pack_export = NULL)
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Applying mask finished in ", t2))
  
  # return
  #return(masked_raster_brick)
  return(output_file)
}

# function to create extreme azimutal angle >80 mask and apply to BRF
FilterEA = function(raster_brick, product_fname, output_dir, tmp_dir, isMCD) {
  #cosSZA = LoadMAIACFiles(product_fname, tmp_dir, "cosSZA")
  #cosSZA = FilterValOutRangeToNA(cosSZA, -10000, 10000)
  #acosSZA = lapply(cosSZA,FUN = function(raster_brick) calc(raster_brick, fun = acos))
  SAZ = LoadMAIACFiles(product_fname, output_dir, tmp_dir, "SAZ", isMCD)
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
  if ((object.size(raster_brick)/(1024*1024*1024)) >= as.numeric(benchmarkme::get_ram()*0.3)/(1024*1024*1024)) {
    # message
    print(paste0(Sys.time(), ": Saving normalized brf to disk - because object is too big to hold in memory..."))
    
    # measure time
    t1 = mytic()
    
    # create dir for normalized brf
    norm_dir = paste0(tmp_dir,"norm_brf\\")
    dir.create(file.path(norm_dir), showWarnings = FALSE, recursive=T)
    
    # vec size
    n_samples = length(raster_brick)
    n_layers = nlayers(raster_brick[[1]])
    
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
      raster_brick[[i]] = stack(paste0(norm_dir, "norm_brf_",i,"_",1:n_layers,".tif"))
      #print(i)
    }
    
    # measure time
    t2 = mytoc(t1)
    
    # message
    print(paste0(Sys.time(), ": Saving normalized brf to disk finished in ", t2))
    
  }
  
  # new list
  y = list()
  
  # measure time
  t1 = mytic()
  
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
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Re-organization finished in ", t2))
  
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
  for (i in 1:length(raster_brick_per_band))
    names(raster_brick_per_band[[i]])[1] = i
  
  # create the iterator object with the raster brick
  myit = iter(obj = raster_brick_per_band)
  
  # Initiate cluster
  #cl = parallel::makeCluster(min(8, no_cores), outfile=log_fname)
  cl = parallel::makeCluster(min(length(raster_brick_per_band), no_cores))
  registerDoParallel(cl)
  objects_to_export = c("CalcMedianAndN", "output_dir", "tmp_dir")
  
  # for each band, iterate through myit so each band is passed to one core
  foreach(i = myit, .packages=c("raster","median2rcpp"), .export=objects_to_export, .errorhandling="remove") %dopar% {
    # message
    #print(paste0(Sys.time(), ": Calculating median per band ",i," from ",length(raster_brick_per_band)))
    tmp = calc(i, fun=CalcMedianAndN)
    
    # calc median, if i == 1 save the number of pixels, otherwise just save the values
    if (names(i)[1]=="X1") {
      writeRaster(round(stack(tmp[[1]]*10000, tmp[[2]]),0), filename=paste0(tmp_dir, "Band_",names(i)[1],".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
    } else {
      writeRaster(round(tmp[[1]]*10000,0), filename=paste0(tmp_dir, "Band_",names(i)[1],".tif"), format="GTiff", overwrite=TRUE, datatype = "INT2S")
    }
    
    # return 0 to the foreach
    c(0)
  }
  
  # finish cluster
  stopCluster(cl)
  
  # open the rasters and make a brick
  b1 = brick(paste0(tmp_dir, "Band_X",1,".tif"))
  if (length(raster_brick_per_band) > 1) {
    median_raster_brick_per_band = stack(c(b1[[1]],paste0(tmp_dir, "Band_X",c(2:length(raster_brick_per_band)),".tif"),b1[[2]]))
  } else {
    median_raster_brick_per_band = stack(c(b1[[1]], b1[[2]]))
  }
  
  # put a name to the bands
  #names(median_raster_brick_per_band) = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")
  names(median_raster_brick_per_band)[1:(length(raster_brick_per_band))] = paste0("band", 1:(length(raster_brick_per_band)))
  names(median_raster_brick_per_band)[length(raster_brick_per_band)+1] = "no_samples"
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Calculating median finished in ", t2))
  
  # return
  return(median_raster_brick_per_band)
}

CalcMedianBRF = cmpfun(CalcMedianBRF)

# function to write the processed file to disk, don't need to apply factors anymore, because we're already applying it on median
SaveProcessedTileComposite = function(medianBRF, output_dir, composite_fname, tile, year, day, composite_no, product_res = 1000) {
  # factors for each band
  #factors = c(10000,10000,10000,10000,10000,10000,10000,10000,10000)
  
  # apply factors
  #b = brick(lapply(c(1:9),FUN=function(x) round(unstack(medianBRF)[[x]]*factors[x],0)))
  
  # verify if tile is a nantile; if it is, we copy it as many times as needed
  if (names(medianBRF)[1] == "nantile") {
    # 1000 m we have bands 1-8, and 500 m we have bands 1-7, therefore 9 bands for 1000 and 8 for 500 m
    if (product_res == 1000) {
      medianBRF = brick(medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF)
    } else {
      medianBRF = brick(medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF, medianBRF)  
    }
  }
  
  # name of the bands
  if (product_res == 1000) {
    band_names = c("band1","band2","band3","band4","band5","band6","band7","band8","no_samples")
  } else {
    band_names = c("band1","band2","band3","band4","band5","band6","band7","no_samples")
  }
  
  # define the composite number or name
  if (composite_no == "month") {
    composite_num = paste0("_",format(as.Date(paste0(day[length(day)],"-", year), "%j-%Y"), "%m"))
  } else {
    composite_num = day[length(day)]
  }
  
  # write to file
  for (i in 1:length(band_names)) {
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
  # projecao ajustada do maiac com longitude central da SouthAmerica -58W (sugest?o Yujie Wang): "+proj=sinu +lon_0=-58 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs"
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
CreateCompositeName = function(composite_no, product, is_qa_filter, is_ea_filter, view_geometry = "nadir", product_res = 1000) {
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
  
  # which product resolution
  if (product_res != 1000) {
    if (product_res == 500) {
      composite_fname = paste0(composite_fname, "_", product_res)
    } else {
      print(paste0(Sys.time(), ": ERROR Product resolution invalid."))
      stop(paste0(Sys.time(), ": ERROR Product resolution invalid."))
    }
  }
  
  #return
  return(composite_fname)
}

# function to create loop mat, filtering the start and end dates from loop_mat, depending on composite_no
CreateLoopMat = function(day_mat, composite_no, input_dir_vec, tile_vec, manual_run) {
  ## the else part of this function is deprecated i think, using the input_dir_vec and tile_vec
  
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

# function to check whether the product for the given composite and tile is from old MAIAC or MCD19A1
IsMCD = function(input_dir, day, year, tile) {
  tmp = list.files(input_dir, pattern = tile, full.names=T)
  tmp = grep(paste(paste0(year, day), collapse="|"), tmp, value=T)
  if (length(tmp) == 0) {
    print(paste0(Sys.time(), ": Couldn't find data for tile ", tile, ", year ", year, ", and day ", day[length(day)],". Going to next iteration..."))
    # log the bad file
    line = paste(year, day[length(day)], tile, sep=",")
    write(line, file=paste0(output_dir,"missing_files.txt"), append=TRUE)
    #
    response = NA # there is no data
  } else {
    if (length(grep("MCD19A",tmp, value=T)) > 0) {
      response = TRUE
    } else {
      response = FALSE
    }
  }
  return(response)
}

# function to create composite dates
createCompDates = function(composite_no = "month", end_year = 2019) {
  value = NA
  if (composite_no == "month") {
    x = expand.grid(sprintf("%02d",seq(1,12)), 2001:end_year)
    value = c(paste0(2000,"_", sprintf("%02d",seq(3,12))), paste0(x$Var2, "_", x$Var1))
  } else {
    x0 = seq(composite_no, 365, composite_no)
    x = expand.grid(sprintf("%03d", x0), 2001:end_year)
    value = c(paste0(2000, sprintf("%03d", x0[x0>=56])), paste0(x$Var2, x$Var1))
  }
  return(value)
}


# function to get dates out of the FV
get_dates_fv = function(brf_fv) {
  brf_fv_names = c()
  for (i in 1:length(brf_fv)) {
    if (substr(names(brf_fv[[i]]), 1,3) != "MCD") {
      names(brf_fv[[i]]) = basename(brf_fv[[i]]@file@name)
    }
    brf_fv_names[i] = names(brf_fv[[i]])
  }
  brf_fv_names = substr(brf_fv_names, 14, 16)
  return(brf_fv_names)
}

# function to get dates out of the kiso
get_dates_kiso = function(rtls_kiso) {
  rtls_kiso_names = c()
  for (i in 1:length(rtls_kiso)) {
    rtls_kiso_names[i] = basename(rtls_kiso[[i]]@file@name)
  }
  rtls_kiso_names = substr(rtls_kiso_names, 15, 17)
  return(rtls_kiso_names)
}

# function to save the geotiffs of files in temporary folder, and resample fv/fg for normalization
SaveMAIACFilesTemporary = function(r, tmp_dir, r_resample = NULL) {
  # r = brf_fv
  # r_resample = brf_reflectance[[1]]
  
  # check if we resampling
  if (!is.null(r_resample)) {
    # set parameters, interpolate the 5km to 1 km by nearest neighbor
    dis_fac_5 = res(r[[1]])[1] / res(r_resample)[1]
  } else {
    dis_fac_5 = NULL
  }
  
  # create temporary filenames
  tmp_fname_list = c()
  for (i in 1:length(r)) {
    tmp_fname = paste0(tmp_dir, basename(tempfile()), ".tif")
    tmp_fname_list[i] = tmp_fname
  }
  
  # function to save the files    
  save_geotiffs = function(i) {
    x = r[[i]]
    r[[i]] = disaggregate(r[[i]], fact=c(dis_fac_5,dis_fac_5)) # fac = 5 for 1km and 10 for 0.5 km
    writeRaster(r[[i]], filename = tmp_fname_list[i], overwrite=T)
  }
  
  # save files in parallel
  snowrun(fun = save_geotiffs,
          values = 1:length(r),
          no_cores = no_cores,
          var_export = c("r", "tmp_fname_list", "dis_fac_5"),
          pack_export = "raster")
  
  # return filenames
  return(tmp_fname_list)
}

# function to resample the FV and FG to match the brf
resample_f = function(f, brf) {
  # f = brf_fv
  # brf = brf_reflectance
  
  # create output filenames
  f2=c()
  for (i in 1:length(f)) {
    f2[i] = paste0(tempfile(),".tif")
  }
  
  
  #
  resample_wrapper = function(i) {
    # gdal_translate to change data format to Float32
    gdal_translate_run = paste("gdal_translate",
                               "-ot Float32",
                               "-b 1",
                               "-q",
                               brf[i],
                               f2[i]
    )
    system(gdal_translate_run)
    
    # resample
    gdalwarp_run = paste("gdalwarp",
                         "-wm", 20*1024*1024*1024,
                         #"-r bilinear",
                         "-q",
                         f[i],
                         f2[i]
    )
    system(gdalwarp_run)
  }
  
  # save files in parallel
  snowrun(fun = resample_wrapper,
          values = 1:length(f),
          no_cores = no_cores,
          var_export = c("f", "brf", "f2"),
          pack_export = NULL)
  
  # return
  return(f2)
  
}

# function to reorder bricks per band to calculate median
ReorderBrickPerBandGDAL = function(nadir_brf_reflectance, n_bands = 8) {
  
  # message
  print(paste0(Sys.time(), ": Reordering observations by band..."))
  
  # measure time
  t1 = mytic()
  
  # create output filenames
  #n_bands = nlayers(stack(nadir_brf_reflectance[1]))
  output_list=list()
  for (i in 1:n_bands) {
    output_list[[i]] = vector("character", length(nadir_brf_reflectance))
    for (j in 1:length(nadir_brf_reflectance)) {
      output_list[[i]][j] = paste0(tempfile(),".tif")
    }
  }
  
  # loop through n_bands
  i=1
  tif_organize = function(i) {
    
    # loop observations
    j=1
    for (j in 1:length(nadir_brf_reflectance)) {
      
      # get the observation j for band i
      gdal_translate_run = paste("gdal_translate",
                                 "-of GTiff",
                                 "-q",
                                 paste0("-b ", i),
                                 nadir_brf_reflectance[j],
                                 output_list[[i]][j])
      system(gdal_translate_run)
      
    } # end observations
    
  }
  
  # run in parallel
  snowrun(fun = tif_organize,
          values = 1:n_bands,
          no_cores = no_cores,
          var_export = c("nadir_brf_reflectance", "output_list"),
          pack_export = NULL)
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Reordering finished in ", t2))
  
  # return 
  return(output_list)
  
}

# function to calculate median using gdal functions
CalcMedianBRFGDAL = function(output_list) {
  
  # message
  print(paste0(Sys.time(), ": Calculating median..."))
  
  # measure time
  t1 = mytic()
  
  # create output filenames
  output_file1=c()
  output_file2=c()
  for (i in 1:length(output_list)) {
    output_file1[i] = paste0(tempfile(),".tif")
    output_file2[i] = paste0(tempfile(),".tif")
  }
  
  # wrapper for normalization in parallel
  i=1
  normalize_wrapper = function(i) {
    
    # create a vrt with all the observations for a given band
    output_vrt = vrt_imgs(output_list[[i]], "gdalbuildvrt", add_cmd = "-separate")
    
    # calculate median using gdal_summarize - https://github.com/mstrimas/gdal-summarize - https://strimas.com/post/raster-summarization-in-python/
    gdal_summarize_run = paste("python3 gdal-summarize.py",
                               "--function", func,
                               "--overwrite",
                               "--quiet",
                               output_vrt,
                               "--outfile", output_file1[i])
    system(gdal_summarize_run)
    
    # convert back to UInt16 and assign nodata to 65535
    gdal_translate_run = paste("gdal_translate",
                               "-of GTiff",
                               "-ot UInt16",
                               ifelse(!isMCD,"-a_srs \"+proj=sinu +lon_0=-70 +R=6371007.181 +units=m +no_defs\"",""), # override the projection of experimental product
                               "-q",
                               "-a_nodata 65535",
                               "-co COMPRESS=DEFLATE",
                               output_file1[i],
                               output_file2[i])
    system(gdal_translate_run)
    
    # remove temp files
    file.remove(output_vrt)
    file.remove(output_file1[i])
    
  }
  
  # run in parallel
  func = "median"
  snowrun(fun = normalize_wrapper,
          values = 1:length(output_list),
          no_cores = no_cores,
          var_export = c("output_list", "output_file1", "output_file2", "vrt_imgs", "func", "isMCD"),
          pack_export = NULL)
  
  # # wrapper to count how many valid observations we have for each pixel
  # i=1
  # count_observations_wrapper = function(i) {
  #   
  #   # create tmp rasters
  #   tmp_file=c()
  #   for (j in 1:length(output_list[[i]])) {
  #     tmp_file[j] = paste0(tempfile(),".tif")
  #   }
  #   
  #   # binarize rasters
  #   for (j in 1:length(output_list[[i]])) {
  #     # convert values to 0 or 1
  #     gdal_calc_run = paste("gdal_calc.py",
  #                           "--calc \" (A>=0)*1 \" ",
  #                           #"-of GTiff",
  #                           #"-ot Byte",
  #                           #"-a_nodata 255",
  #                           "-A", output_list[[i]][j],
  #                           "--outfile", tmp_file[j])
  #     system(gdal_calc_run)
  #   }
  #   
  #   # create a vrt with all the observations for a given band
  #   output_vrt = vrt_imgs(tmp_file, "gdalbuildvrt", add_cmd = "-separate")
  #   
  #   # calculate median using gdal_summarize - https://github.com/mstrimas/gdal-summarize - https://strimas.com/post/raster-summarization-in-python/
  #   gdal_summarize_run = paste("python3 gdal-summarize.py",
  #                              "--function", func,
  #                              "--overwrite",
  #                              output_vrt,
  #                              "--outfile", output_file1[i])
  #   system(gdal_summarize_run)
  #   
  #   # convert back to UInt16 and assign nodata to 65535
  #   gdal_translate_run = paste("gdal_translate",
  #                              "-of GTiff",
  #                              "-ot UInt16",
  #                              "-a_nodata 65535",
  #                              output_file1[i],
  #                              output_numobs[i])
  #   system(gdal_translate_run)
  #   
  #   # remove temp files
  #   file.remove(output_vrt)
  #   file.remove(output_file1[i])
  #   file.remove(tmp_file)
  #   
  # }
  #
  ## run in parallel for all bands
  # output_numobs=c()
  # for (i in 1:length(output_list)) {
  #   output_numobs[i] = paste0(tempfile(),".tif")
  # }
  #func = "sum"
  # snowrun(fun = count_observations_wrapper,
  #         values = 1:length(output_list),
  #         no_cores = no_cores,
  #         var_export = c("output_list", "output_file1", "output_file2", "vrt_imgs", "func"),
  #         pack_export = NULL)
  
  # wrapper to count how many valid observations we have for each pixel
  i=1
  count_observations_wrapper = function(i) {
    
    # create tmp rasters
    tmp_file=c()
    for (j in 1:length(output_list[[i]])) {
      tmp_file[j] = paste0(tempfile(),".tif")
    }
    
    # binarize rasters
    binarize_rasters = function(j) {
      #for (j in 1:length(output_list[[i]])) {
      # convert values to 0 or 1
      gdal_calc_run = paste("gdal_calc.py",
                            "--calc \" (A>=0)*1 \" ",
                            #"-of GTiff",
                            #"-ot Byte",
                            #"-a_nodata 255",
                            "-A", output_list[[i]][j],
                            "--quiet",
                            "--outfile", tmp_file[j])
      system(gdal_calc_run)
    }
    
    # binarize in parallel
    snowrun(fun = binarize_rasters,
            values = 1:length(output_list[[i]]),
            no_cores = no_cores,
            var_export = c("output_list", "tmp_file"),
            pack_export = NULL)
    
    # create a vrt with all the observations for a given band
    output_vrt = vrt_imgs(tmp_file, "gdalbuildvrt", add_cmd = "-separate")
    
    # calculate median using gdal_summarize - https://github.com/mstrimas/gdal-summarize - https://strimas.com/post/raster-summarization-in-python/
    gdal_summarize_run = paste("python3 gdal-summarize.py",
                               "--function", func,
                               "--overwrite",
                               "--quiet",
                               output_vrt,
                               "--outfile", output_file1[i])
    system(gdal_summarize_run)
    
    # convert back to UInt16 and assign nodata to 65535
    gdal_translate_run = paste("gdal_translate",
                               "-of GTiff",
                               "-ot UInt16",
                               ifelse(!isMCD,"-a_srs \"+proj=sinu +lon_0=-70 +R=6371007.181 +units=m +no_defs\"",""), # override the projection of experimental product
                               "-q",
                               "-a_nodata 65535",
                               output_file1[i],
                               output_numobs[i])
    system(gdal_translate_run)
    
    # remove temp files
    file.remove(output_vrt)
    file.remove(output_file1[i])
    file.remove(tmp_file)
    
  }
  
  # run count obs for band 1 only
  output_numobs = paste0(tempfile(),".tif")
  func = "sum"
  count_observations_wrapper(1)
  
  # # clean previous files
  # for (i in 1:length(output_list)) {
  #   file.remove(output_list[[i]])
  # }
  
  # debugging
  if (FALSE) {
    r = raster(output_file1[i])
    r
    r[r<0 | r>10000]=NA
    plot(r)
    click(r)
    gdalinfo(output_file1[i])
    
    # the original data
    plot(raster(output_list[[i]][1]))
    
    # NA = 65535
    r = raster(output_file2[i])
    r
    #r[r<0 | r>10000]=NA
    plot(r)
    click(r)
    gdalinfo(output_file2[i])
  }
  
  # testing
  if (FALSE) {
    r = stack(output_file2)
    table(r[[1]][])
    plot(r[[1]])
    plot(r[[6]])
    plot(r[[1]] - r[[6]])
  }
  
  # combine the bands with the num of observations
  output_file2 = c(output_file2, output_numobs)
  
  # measure time
  t2 = mytoc(t1)
  
  # message
  print(paste0(Sys.time(), ": Calculating median finished in ", t2))
  
  # return
  return(output_file2)
  
}

# function to make automatic time spent
timer = function(obj) {
  
  if (is.character(obj)) {
    
    # message
    print(paste0(Sys.time(), ": ", obj, " starting..."))
    
    # measure time
    t1 = mytic()
    
    obj_output = list()
    obj_output[[1]] = t1
    obj_output[[2]] = obj
    
    return(obj_output)
    
  } else {
    
    # measure time
    t2 = mytoc(obj[[1]])
    
    # message
    print(paste0(Sys.time(), ": ",obj[[2]], " finished in ", t2))
    
  }
  
  
  
}

# aws functions -----------------------------------------------------------

# function to get the filenames of files to download based on file_list_<year>.txt inside functions_dir
get_filenames_to_download = function(functions_dir, year) {
  txt_fname = paste0(functions_dir, "file_list_",year,".txt")
  if (file.exists(txt_fname)) {
    txt = read.table(txt_fname)$V1
    txt = txt[which(nchar(txt)>100)]
    idx_ref = grep("MCD19A1", txt)
    txt_ref = txt[idx_ref]
    txt_brdf = txt[-idx_ref]
    fname_brdf = substr(txt_brdf, 77, nchar(txt_brdf[1]))
    fname_ref = substr(txt_ref, 76, nchar(txt_ref[1]))
    fname = c(fname_brdf, fname_ref)
  } else {
    fname = NA
    stop(paste0("file does not exist: ", txt_fname))
  }
  return(fname)
}

# # function to sync S3 files 
# S3_sync = function(input_dir, output_dir) {
#   system(paste0("aws s3 sync ", input_dir, " ", output_dir))
# }
# function to sync S3 files 
S3_sync = function(input_dir, output_dir, S3_profile = "ctrees") {
  system(paste0("aws s3 sync ", input_dir, " ", output_dir, " --profile ", S3_profile))
}

# function to download files from earthdata
S3_download_single_file = function(i) {
  output_fname = paste0(output_dir, "/", basename(files_to_download[i]))
  if (!file.exists(output_fname)) {
    system(paste0("aws s3 cp ", files_to_download[i], " ", output_fname, " --profile earthdata"))
  }
}

# function to download/upload files from S3
S3_download_upload = function(i) {
  system(paste0("aws s3 cp ", input_files[i], " ", output_files[i], " --profile ", S3_profile))
}

# function to list files in a bucket
s3_list_bucket = function(s3_input, RETRIEVE_ONLY_KEY = TRUE) {
  
  # lib
  require(paws)
  
  # determine bucket and prefix to search that fit in the s3 function
  tmp = stringr::str_split(s3_input, "/")[[1]]
  bucket = tmp[3]
  prefix = paste(tmp[4:length(tmp)], collapse ="/")
  
  # create connection
  s3 <- paws::s3()
  
  # # to list files in AWS 
  # list_files_aws = s3$list_objects(
  #   Bucket = bucket,
  #   Prefix = prefix,
  #   MaxKeys = 99999
  # )
  
  # # create vector with the list
  # file_list = c()
  # for (i in 1:length(list_files_aws$Contents)) file_list[i] = list_files_aws$Contents[[i]]$Key
  
  # to list files in AWS 
  file_list = s3_list_objects_paginated(bucket, prefix, RETRIEVE_ONLY_KEY)
  #file_list = list_files_aws$Key
  
  # adjust file names
  if (!is.null(file_list)) {
    file_list[[1]] = paste0("s3://", bucket, "/", file_list[[1]])
  }
  
  if (RETRIEVE_ONLY_KEY) file_list = file_list[[1]]
  
  # return files
  return(file_list)
  
}

# function to list s3 objects with pagination
s3_list_objects_paginated <- function(bucket, prefix, RETRIEVE_ONLY_KEY = TRUE, last_modified = TRUE, max_retries = 5) {
  
  response <- paws.storage::s3()$list_objects_v2(
    Bucket = bucket,
    Prefix = prefix
  )
  
  responses <- list(response)
  
  if (response[["IsTruncated"]]) {
    
    truncated <- TRUE
    
    while (truncated) {
      
      retry <- TRUE
      retries <- 0
      
      # If an error is returned by AWS then try again with exponential backoff
      while (retry && retries < max_retries) {
        
        response <- tryCatch(
          paws.storage::s3()$list_objects_v2(
            Bucket = bucket,
            Prefix = prefix,
            ContinuationToken = response[["NextContinuationToken"]]
          ) 
          #,error = \(e) e
        )
        
        if (inherits(response, "error")) {
          if (retries == max_retries) stop(response)
          
          wait_time <- 2 ^ retries / 10
          Sys.sleep(wait_time)
          retries <- retries + 1
        } else {
          retry <- FALSE
        }
      }
      
      responses <- append(responses, list(response))
      
      truncated <- response[["IsTruncated"]]
      
    }
    
  } 
  
  # 
  print(paste(Sys.time(), "All files were queried. Now concatenating the results."))
  
  i=1
  file_list=c()
  for (i in 1:length(responses)) {
    j=1
    if (length(responses[[i]]$Contents) > 0) {
      for (j in 1:length(responses[[i]]$Contents)) file_list = c(file_list, responses[[i]]$Contents[[j]]$Key)
    }
  }
  
  # also retrieve other info
  if (!RETRIEVE_ONLY_KEY) {
    last_modified_info=c()
    for (i in 1:length(responses)) {
      j=1
      if (length(responses[[i]]$Contents) > 0) {
        for (j in 1:length(responses[[i]]$Contents)) last_modified_info = c(last_modified_info, responses[[i]]$Contents[[j]]$LastModified)
      }
    }
    
    file_list = list(file_list, last_modified_info)
  } else {
    file_list = list(file_list)
  }
  
  print(paste(Sys.time(), "File listing done."))
  
  # df_responses <- responses |> 
  #   purrr::map("Contents") |> 
  #   purrr::map(
  #     \(x) purrr::map(
  #       x, \(y) purrr::keep(y, names(y) %in% c("Key", "LastModified"))
  #     )
  #   ) |> 
  #   dplyr::bind_rows()
  # 
  # if (last_modified) {
  #   dplyr::arrange(df_responses, dplyr::desc(LastModified))
  # } else {
  #   df_responses
  # }
  
  return(file_list)
  
}

# function to download S3 files
S3_copy_single = function(input_fname, output_fname, s3_profile = NULL) {
  
  for (i in 1:length(input_fname)) {
    system(paste0("aws s3 cp ", input_fname[i], " ", output_fname[i], ifelse(!is.null(s3_profile), paste0(" --profile ", s3_profile), "")))
  }
  
}

# function to download S3 files in parallel
S3_copy_single_parallel = function(input_fname, output_fname, s3_profile = NULL, no_cores = parallel::detectCores()-1, add_cmd = NULL) {
  
  if (length(input_fname) < no_cores) {
    
    S3_copy_single(input_fname, output_fname, s3_profile)
    
  } else {
    
    download_wrapper = function(i){
      if (!file.exists(output_fname[i])) {
        system(paste0("aws s3 cp ",
                      input_fname[i],
                      " ",
                      output_fname[i],
                      ifelse(!is.null(s3_profile), paste0(" --profile ", s3_profile), ""),
                      ifelse(!is.null(add_cmd),paste0(" ", add_cmd),"")
        ))
      }
    }
    
    # run tiling
    snowrun(fun = download_wrapper,
            values = 1:length(input_fname),
            no_cores = no_cores,
            var_export = c("input_fname", "output_fname", "s3_profile"),
            pack_export = NULL)
    
  }
  
}

# function to refresh credentials from earth data
refresh_credentials_earthdata = function() {
  # content of earthdata.netrc file: machine urs.earthdata.nasa.gov login <YOURLOGIN> password <YOURPWD>
  get_credentials = system("curl -b cookies.txt -c cookies.txt -L --netrc-file earthdata.netrc https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials", intern=T)#[4]
  get_credentials = jsonlite::fromJSON(get_credentials)
  #dput(get_credentials)
  
  # set config
  system(paste0("aws configure set aws_access_key_id \"",get_credentials$accessKeyId,"\" --profile earthdata"))
  system(paste0("aws configure set aws_secret_access_key \"",get_credentials$secretAccessKey,"\" --profile earthdata"))
  system(paste0("aws configure set aws_session_token \"",get_credentials$sessionToken,"\" --profile earthdata"))
  system(paste0("aws configure set aws_default_region \"","us-west-2","\" --profile earthdata"))
  
  #
  print("Earth Data credentials refreshed.")
}

# function to sub s3 for vsis
s3_to_vsis = function(fname) {
  return(gsub("s3://", "/vsis3/", fname))
}


# utilities ---------------------------------------------------------------



# wrapper function around snowfall to run codes in parallel
snowrun = function(fun, values, no_cores, var_export = NULL, pack_export = NULL, ...) {
  # libraries need for snowfall parallel
  library(pacman)
  p_load(snowfall)
  snowfall::sfInit(parallel = TRUE, cpus = no_cores) # adjust number of cores here
  
  # import variables or packages inside cores
  if (!is.null(var_export)) {
    snowfall::sfExport(list = var_export)
  }
  if (!is.null(pack_export)) {
    for (i in 1:length(pack_export)) {
      snowfall::sfLibrary(pack_export[i], character.only=TRUE)
    }
  }
  
  # run in parallel
  system.time({a = snowfall::sfLapply(values, fun)})
  snowfall::sfStop()
  
  # return
  return(a)
}

# function to mosaic images
vrt_imgs = function(file_list, gdalbuildvrt, add_cmd = NULL) {
  
  # write txt with fnames to mosaic
  #txt_fname = paste0(output_dir,"/file_list_mosaic.txt")
  txt_fname = paste0(tempfile(), ".txt")
  sink(txt_fname)
  #cat(paste0(getwd(), "\\", list_files), sep="\n")
  cat(paste0(file_list), sep="\n")
  sink()
  
  #
  name_final_vrt = sub(".txt", ".vrt", txt_fname)
  
  # run
  system(paste(gdalbuildvrt, ifelse(!is.null(add_cmd),add_cmd,""), "-input_file_list", txt_fname, name_final_vrt ))
  
  # remove txt
  file.remove(txt_fname)
  return(name_final_vrt)
  
}

# function to sopt ec2 machine
ec2_stop = function() {
  instance_id = system('ec2metadata --instance-id', intern=T)
  system(paste0("aws ec2 stop-instances --instance-ids ", instance_id))
}

# function to check if one S3 file exists
S3_file_exists = function(fname) {
  # determine bucket and prefix to search
  tmp = stringr::str_split(fname, "/")[[1]]
  bucket = tmp[3]
  prefix = paste(tmp[4:length(tmp)], collapse ="/")
  
  # list objects
  s3 <- paws::s3()
  # to list files in AWS 
  list_files_aws = s3$list_objects(
    Bucket = bucket,
    Prefix = prefix,
    MaxKeys = 99999
  )
  
  # check
  if (length(list_files_aws$Contents) == 0) {
    val = FALSE
  } else {
    val = TRUE
  }
  
  # return
  return(val)
}
