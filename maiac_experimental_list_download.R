# list maiac files from south america with c6.1 experimental

# load packages
library(xml2)
library(rvest) # install.packages("rvest")

# function to sync S3 files 
S3_sync = function(input_dir, output_dir) {
  system(paste0("aws s3 sync ", input_dir, " ", output_dir))
}

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
year_list = 2000:2023
tile_list = c("h29v07",
              "h30v07",
              "h30v08",
              "h30v09",
              "h30v10",
              "h31v07",
              "h31v08",
              "h31v09",
              "h31v10",
              "h31v11",
              "h32v07",
              "h32v08",
              "h32v09",
              "h32v10",
              "h32v11",
              "h33v08",
              "h33v09",
              "h33v10",
              #"h33v11", no data here in the may2025 version
              "h34v09",
              "h34v10",
              "h34v11")
length(tile_list)

# filtered tile list
#tile_list = c("h33v07", "h34v07", "h34v08", "h33v10")
#tile_list = "h33v10"

#
txt_name_error = "maiac_txt_error.txt"


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
                    ,"-j 10" # 5 seems to be the maximum downloads at the same time in earth data
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
