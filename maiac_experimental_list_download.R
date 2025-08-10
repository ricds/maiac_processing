
# begin -------------------------------------------------------------------

# list maiac files from south america with c6.1 experimental

# load packages
library(xml2)
library(rvest) # install.packages("rvest")

# working dir
setwd("/home/rstudio/")
#setwd("E:/")

# directories in s3
s3_output_txt = "s3://ctrees-input-data/modis/MCD19A1_C6.1_experimental_SA_May25/txt/"
s3_output_raw = "s3://ctrees-input-data/modis/MCD19A1_C6.1_experimental_SA_May25/raw/"

# local output directory for the txt files
txt_local_dir = "maiac_txt_file_list/"
dir.create(txt_local_dir, showWarnings = F, recursive=T)

# url to search
URL_main = "https://portal.nccs.nasa.gov/datashare/maiac/DataRelease/SA_2000-2023/"
year_list = 2000:2024
tile_list = c("h29v07", #ok
              "h30v07", #ok
              "h30v08", #deleting files
              "h30v09", #deleting
              "h30v10", #ok
              "h31v07", #ok
              "h31v08", #ok
              "h31v09", #ok
              "h31v10", #ok
              "h31v11", #downloading
              "h32v07", 
              "h32v08",
              "h32v09", #ok
              "h32v10", #ok
              "h32v11", #downloading
              "h33v08",
              "h33v09",
              "h33v10",
              #"h33v11", #nodata
              "h34v09", #ok
              "h34v10" #ok
              #"h34v11" #nodata
)
length(tile_list)

# filtered tile list
#tile_list = c("h33v07", "h34v07", "h34v08", "h33v10")
#tile_list = "h33v10"

#
txt_name_error = "maiac_txt_error.txt"


# functions ---------------------------------------------------------------


# function to sync S3 files 
S3_sync = function(input_dir, output_dir, S3_profile = "ctrees") {
  system(paste0("aws s3 sync ", input_dir, " ", output_dir, " --profile ", S3_profile))
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


# function to delete S3 files
S3_remove = function(input_fname, s3_profile = NULL, no_cores = parallel::detectCores()) {
  
  wrapper = function(i){
    system(paste0("aws s3 rm ", input_fname[i], ifelse(!is.null(s3_profile), paste0(" --profile ", s3_profile), "")))  
  }
  
  # to parallel or not to parallel, that is the question
  if (length(input_fname) < no_cores) {
    
    # simple loop
    for (i in 1:length(input_fname)) {
      wrapper(i)
    }
    
  } else {
    
    # parallel
    snowrun(fun = wrapper,
            values = 1:length(input_fname),
            no_cores = no_cores,
            var_export = c("input_fname", "s3_profile"),
            pack_export = NULL)
    
  }
  
}

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

# run listing -------------------------------------------------------------

# loop tile
j=1
for (j in 1:length(tile_list)) {
  
  # 
  tile = tile_list[j]
  
  
  # loop year
  i=1
  for (i in 1:length(year_list)) {
    
    year = year_list[i]
    URL = paste0(URL_main, tile, "/", year, "/")
    
    
    # create file to store the URLs
    txt_name = paste0("maiac_txt_file_list/file_list_",tile,"_year_",year,"_experimental_maiac.txt")
    sink(file = txt_name)
    sink()
    
    # try to read the page a few times
    tries = 0
    pg = "try-error"
    class(pg) = "try-error"
    while (class(pg)[1] == "try-error" & tries < 5) {
      #pg <- read_html(URL)
      pg <- try(read_html(curl::curl(URL, handle = curl::new_handle("useragent" = "Mozilla/5.0"))))
      tries = tries + 1
      if (class(pg)[1] == "try-error") {
        print(paste0("Error in ", year_list[i], " - tile ", tile))
        Sys.sleep(sample(10, 1) * 0.1 + tries/2) # random sleep
      }
    }
    # if still error print the URL on a new file
    if (class(pg)[1] == "try-error") {
      if (!file.exists(txt_name_error)) {
        sink(file = txt_name_error); sink()
      } else {
        sink(file = txt_name_error, append=T)
        cat(URL, sep="\n")
        sink()
      }
    } else { #if not error
      
      # get info
      a = html_attr(html_nodes(pg, "a"), "href")
      a = grep(".hdf", a, value=T)
      #a = grep(paste(tile_list, collapse="|"), a, value=T)
      a = a[!duplicated(a)]
      
      sink(file = txt_name, append=T)
      cat(paste0(URL,a), sep="\n")
      sink()
      
    }
    
    print(paste(j,i))
    
  }
  
}

# sync txt with s3
S3_sync(txt_local_dir, s3_output_txt)


# download ----------------------------------------------------------------

# download txts
S3_sync(s3_output_txt, txt_local_dir)

# define download directory
download_dir = "maiac_download/"
dir.create(download_dir, showWarnings = F, recursive=T)

# clean download dir
file.remove(list.files(download_dir, full.names=T))

# define aria
aria_path = "aria2c"

# select tiles to run
if (FALSE) {
  mat=matrix(1:21, nrow=4, byrow=T)
  tile_list=tile_list[mat[1,]]
  tile_list=tile_list[3:6]
}

# loop tile
j=1
for (j in 1:length(tile_list)) {
  
  # 
  tile = tile_list[j]
  
  
  # loop year
  i=1
  for (i in 1:length(year_list)) {
    
    year = year_list[i]
    
    # create file to store the URLs
    txt_name = paste0(txt_local_dir, "file_list_",tile,"_year_",year,"_experimental_maiac.txt")
    
    # skip if file does not exist
    if (!file.exists(txt_name)) next
    
    # download
    system.time({
      aria2 = paste(aria_path
                    ,"-c"
                    ,"-j 5" # 5 seems to be the maximum downloads at the same time in earth data
                    ,"--retry-wait 1"
                    ,"-q" # quiet
                    ,"-i", txt_name
                    ,"-d", download_dir
                    #,paste0("--http-user=", login, " --http-passwd=", pwd)
      )
      system(aria2)
    })
    
    # check which files have been downloaded
    txt_name_read = try(read.table(txt_name)[[1]])
    
    # skip
    if (class(txt_name_read)[1] == "try-error") next
    
    # get file names
    #head(txt_name_read)
    #tail(txt_name_read)
    check_files = file.exists(paste0(download_dir, basename(txt_name_read)))
    idx_missing = which(!check_files)
    
    # missing
    if (length(idx_missing) > 0) {
      #
      files_missing = txt_name_read[idx_missing]
      
      # create file to store the URLs
      txt_name = paste0(txt_local_dir, "missing_files.txt")
      sink(file = txt_name)
      cat(files_missing, sep="\n")
      sink()
      
      # we put it to download again with j=1 because some files get error due to multiple connections, so this time with j=1 it gets the missing ones
      system.time({
        aria2 = paste(aria_path
                      ,"-c"
                      ,"-j 1" # 5 seems to be the maximum downloads at the same time in earth data
                      ,"--retry-wait 1"
                      ,"-q" # quiet
                      ,"-i", txt_name
                      ,"-d", download_dir
                      #,paste0("--http-user=", login, " --http-passwd=", pwd)
        )
        system(aria2)
      })
      
      file.remove(txt_name)
      
    } # missing if
    
    # upload files
    s3_output_dir = paste0(s3_output_raw, tile, "/", year, "/")
    
    # sync files
    S3_sync(download_dir, s3_output_dir)
    
    # clean download dir
    file.remove(list.files(download_dir, full.names=T))
    
    print(paste(j, i))
    
  }
  
}



# check files and delete non-matching -------------------------------------

# 
files_to_delete = c()

# loop tiles
i=3
for (i in 1:length(tile_list)) {
  
  # get tile i
  tile = tile_list[i]
  
  # define s3 path
  s3_path = paste0(s3_output_raw, tile, "/")
  
  # list files
  file_list = s3_list_bucket(s3_path)
  
  # loop years 
  j=20
  for (j in 1:length(year_list)) {
    
    # get year
    year = year_list[j]
    
    # str to search
    file_list_j = grep(paste0("/",year,"/"), file_list, value=T)
    
    # find those that belong to this year
    idx_belong = grep(paste0("\\.", year), file_list_j)

    # get those that do not belong
    file_list_j_donotbelong = file_list_j[-idx_belong]
    
    # if there are files add to list
    if (length(file_list_j_donotbelong) > 0) files_to_delete = c(files_to_delete, file_list_j_donotbelong)
    
  }
  
  # clean
  rm(file_list)
  print(i)
  
}

# check
length(files_to_delete)
head(files_to_delete)
tail(files_to_delete)

# remove the listed files
if (FALSE) {

  S3_remove(files_to_delete, "ctrees")
  
}
