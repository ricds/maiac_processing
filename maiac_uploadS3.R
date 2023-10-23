# define download directory
download_dir = "E:/MAIAC_Download/"
dir.create(download_dir, showWarnings = F, recursive=T)

# define working directory
setwd(download_dir)

# number of cores
no_cores = 10

# define bucket in S3 to upload the data
s3_bucket = "s3://ctrees-input-data/MCD19_MAIAC/v061/"

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

# function to download S3 files
S3_upload = function(j) {
  
  system(paste0("aws s3 cp ", file_list[j], " ", s3_file_list[j]))
}

# while loop that runs until we finish downloading all
#365*23*2*40
PROCESSING = TRUE
while (PROCESSING) {
  
  # list files in the download dir
  file_list = list.files(".", full.names=T, pattern=".hdf$", recursive=T)
  
  # wait for 30 secs, so we dont have any problem with files still being written to disk
  Sys.sleep(30)
  
  # define filenames in s3
  s3_file_list = paste0(s3_bucket, paste0(basename(dirname(file_list)), "/", basename(file_list)))
  
  # upload files to s3
  system.time({
  snowrun(fun = S3_upload,
          values = 1:length(file_list),
          no_cores = no_cores,
          var_export = c("file_list", "s3_file_list"),
          pack_export = NULL)
  })
  
  # delete files
  # file.remove(file_list)
  
}