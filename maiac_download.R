# Script to download MCD19A1 and MCD19A3D files from NASA LAADS

# load packages
library(xml2)
library(rvest) # install.packages("rvest")

# define download directory
download_dir = "E:/MAIAC_Download/"
dir.create(download_dir, showWarnings = F, recursive=T)

# define working directory
setwd(download_dir)

# set path to aria program
aria_path = "C:\\aria2\\aria2c.exe"

# define tiles to download
# MCD tiles for south america
tiles_list = c("h09v07", "h10v07", "h11v07", "h12v07", "h09v08", "h10v08", "h11v08", "h12v08", "h13v08", "h09v09", "h10v09", "h11v09", "h12v09", "h13v09", "h14v09", "h10v10", "h11v10", "h12v10", "h13v10", "h14v10", "h11v11", "h12v11", "h13v11", "h14v11", "h11v12", "h12v12", "h13v12", "h12v13", "h13v13", "h13v14", "h14v14")
#tiles_list = c("h12v09", "h12v10")

# define years to download
year_list = 2000:2023

# get input from user for login and password
login = readline("Type in the login for Earth Data website:")
pwd = readline("Type in the password for Earth Data website:")

# filename for the error file
txt_name_error = "file_list_error.txt"

# product version string
#product_reflectance = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/6/MCD19A1/" # v6 - not working anymore
#product_brdf = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/6/MCD19A3/" # v6 - not working anymore
product_reflectance = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MCD19A1/" # v6.1
product_brdf = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MCD19A3D/" # v6.1

## list MCD19A1 surface reflectance product
# loop year
year=1
for (year in 1:length(year_list)) {
  
  # create file to store the URLs
  txt_name = paste0("file_list_",year_list[year],".txt")
  sink(file = txt_name)
  sink()
  
  # define doy to download
  if (year_list[year] == 2000) {
    doy_list = 55:365
  } else {
    doy_list = 1:365 # the whole year  
  }
  #doy_list = 213:243 # only august
  #if (year_list[year] == 2014) doy_list = 182:212 # for testing !! only 7
  #if (year_list[year] == 2014) doy_list = 182:365 # 7-12
  #if (year_list[year] == 2015) doy_list = 1:120 # 1-4
  
  # loop DOY
  j=1
  for (j in 1:length(doy_list)) {
    doy_j = doy_list[j]
    URL <- paste0(product_reflectance, year_list[year], "/", sprintf("%03d", doy_j))
    
    # try to read the page a few times
    tries = 0
    pg = "try-error"
    class(pg) = "try-error"
    while (class(pg)[1] == "try-error" & tries < 5) {
      #pg <- read_html(URL)
      pg <- try(read_html(curl::curl(URL, handle = curl::new_handle("useragent" = "Mozilla/5.0"))))
      tries = tries + 1
      if (class(pg)[1] == "try-error") {
        print(paste0("Error in ", year_list[year], " - ", doy_j))
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
      a = grep(paste(tiles_list, collapse="|"), a, value=T)
      a = a[!duplicated(a)]
      
      sink(file = txt_name, append=T)
      cat(paste0("https://ladsweb.modaps.eosdis.nasa.gov",a), sep="\n")
      sink()
      
    }
    
    print(paste0("Ref - ", year_list[year], " - ", doy_j))
    Sys.sleep(sample(10, 1) * 0.1) # random sleep
  }
}


## list BRDF
year=1
for (year in 1:length(year_list)) {
  
  # get year fname
  txt_name = paste0("file_list_",year_list[year],".txt")
  
  # define doy to download
  if (year_list[year] == 2000) {
    doy_list = 55:365
  } else {
    doy_list = 1:365 # the whole year  
  }
  #doy_list = 213:243 # only august
  #if (year_list[year] == 2014) doy_list = 182:212 # for testing !! only 7
  #if (year_list[year] == 2014) doy_list = 182:365 # 7-12
  #if (year_list[year] == 2015) doy_list = 1:120 # 1-4
  
  # define the rtls list
  #brdf_list = doy_list[which(doy_list %in% seq(1,365,8))] # v6 collection
  brdf_list = doy_list # v6.1 collection
  
  # loop DOY
  j=1
  for (j in 1:length(brdf_list)) {
    doy_j = brdf_list[j]
    URL <- paste0(product_brdf, year_list[year], "/", sprintf("%03d", doy_j))
    
    # try to read the page a few times
    tries = 0
    pg = "try-error"
    class(pg) = "try-error"
    while (class(pg)[1] == "try-error" & tries < 5) {
      #pg <- read_html(URL)
      pg <- try(read_html(curl::curl(URL, handle = curl::new_handle("useragent" = "Mozilla/5.0"))))
      tries = tries + 1
      if (class(pg)[1] == "try-error") {
        print(paste0("Error in ", year_list[year], " - ", doy_j))
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
      a = grep(paste(tiles_list, collapse="|"), a, value=T)
      a = a[!duplicated(a)]
      
      sink(file = txt_name, append=T)
      cat(paste0("https://ladsweb.modaps.eosdis.nasa.gov",a), sep="\n")
      sink()
    }
    
    print(paste0("BRDF - ", year_list[year], " - ", doy_j))
    Sys.sleep(sample(10, 1) * 0.1) # random sleep
  }
}

## download ARIA
year=1
for (year in 1:length(year_list)) {
  
  # get year fname
  txt_name = paste0("file_list_",year_list[year],".txt")
  
  # create the output dir
  dir.create(paste0(download_dir, year_list[year]), showWarnings = F, recursive=T)
  
  system.time({
  # download
  aria2 = paste(aria_path
                ,"-c"
                ,"-j 5" # 5 seems to be the maximum downloads at the same time in earth data
                ,"--retry-wait 1"
                ,"-i", paste0(download_dir, txt_name)
                ,"-d", paste0(download_dir, year_list[year], "/")
                ,paste0("--http-user=", login, " --http-passwd=", pwd)
  )
  system(aria2)
  
  # we put it to download again with j=1 because some files get error due to multiple connections, so this time with j=1 it gets the missing ones
  aria2 = paste(aria_path
                ,"-c"
                ,"-j 1" # 5 seems to be the maximum downloads at the same time in earth data
                ,"--retry-wait 1"
                ,"-i", paste0(download_dir, txt_name)
                ,"-d", paste0(download_dir, year_list[year], "/")
                ,paste0("--http-user=", login, " --http-passwd=", pwd)
  )
  system(aria2)
  })
  
}


# helper ------------------------------------------------------------------


# generate tile list for south america
# # h09 - h14 and v07 - v14
# v1 = paste0("h", sprintf("%02d", seq(9,14,1)))
# v2 = paste0("v", sprintf("%02d", seq(7,14,1)))
# tiles_total = expand.grid(v1,v2)
# tiles_total = paste0(tiles_total$Var1, tiles_total$Var2)
# # exclude
# tiles_exclude = c("h09v10","h09v11","h10v11","h09v12","h10v12","h09v13","h10v13","h11v13","h09v14","h10v14","h11v14","h14v08","h14v12","h14v13")
# tiles_list = tiles_total[-which(tiles_total %in% tiles_exclude)]
