#curl -b ~/.urs_cookies -c ~/.urs_cookies -L -n 'https://data.nsidc.earthdatacloud.nasa.gov/s3credentials'
# https://nsidc.org/data/user-resources/help-center/nasa-earthdata-cloud-data-access-guide
library(jsonlite)
# https://nasa-openscapes.github.io/2021-Cloud-Workshop-AGU/how-tos/Earthdata_Cloud__Single_File__Direct_S3_Access_COG_Example.html this has all the credentials
# s3_cred_endpoint = {
#   'podaac':'https://archive.podaac.earthdata.nasa.gov/s3credentials',
#   'gesdisc': 'https://data.gesdisc.earthdata.nasa.gov/s3credentials',
#   'lpdaac':'https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials',
#   'ornldaac': 'https://data.ornldaac.earthdata.nasa.gov/s3credentials',
#   'ghrcdaac': 'https://data.ghrc.earthdata.nasa.gov/s3credentials'
# }
# content of earthdata.netrc file: machine urs.earthdata.nasa.gov login <YOURLOGIN> password <YOURPWD>
get_credentials = system("curl -b cookies.txt -c cookies.txt -L --netrc-file earthdata.netrc https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials", intern=T)#[4]
get_credentials = fromJSON(get_credentials)
dput(get_credentials)

# set config
system(paste0("aws configure set aws_access_key_id \"",get_credentials$accessKeyId,"\" --profile earthdata"))
system(paste0("aws configure set aws_secret_access_key \"",get_credentials$secretAccessKey,"\" --profile earthdata"))
system(paste0("aws configure set aws_session_token \"",get_credentials$sessionToken,"\" --profile earthdata"))
system(paste0("aws configure set aws_default_region \"","us-west-2","\" --profile earthdata"))

# ## aws configure --profile earthdata
# Sys.setenv(AWS_PROFILE = "earthdata")
# Sys.setenv(
#   "AWS_ACCESS_KEY_ID" = get_credentials$accessKeyId,
#   "AWS_SECRET_ACCESS_KEY" = get_credentials$secretAccessKey,
#   "AWS_SESSION_TOKEN" = get_credentials$sessionToken,
#   "AWS_DEFAULT_REGION"="us-west-2",
#   "AWS_ENDPOINT"="s3-us-west-2.amazonaws.com")

# test
if (FALSE) {
  fname = "s3://lp-prod-protected/MCD19A1.061/MCD19A1.A2023292.h30v12.061.2023294041001/MCD19A1.A2023292.h30v12.061.2023294041001.hdf"
  system(paste0("aws s3 cp ",fname," /home/rstudio/test.hdf --profile earthdata"))
  
  #s3_fname = "/vsis3/lp-prod-protected/MCD19A1.061/MCD19A1.A2023292.h30v12.061.2023294041001/MCD19A1.A2023292.h30v12.061.2023294041001.hdf"
  s3_fname = "/vsihdfs/lp-prod-protected/MCD19A1.061/MCD19A1.A2023292.h30v12.061.2023294041001/MCD19A1.A2023292.h30v12.061.2023294041001.hdf"
  system(paste("gdal_translate",
               s3_fname,
               "/home/rstudio/test.hdf"
  ))
}

files_to_download = read.table("file_list_2000.txt")$V1
head(files_to_download)
tail(files_to_download)

bucket_str = "s3://lp-prod-protected/MCD19A1.061/"
dir.create("/home/rstudio/maiac_raw/", showWarnings=F, recursive=T)

i=1
for (i in 1:100) {
  fname = basename(files_to_download[i])
  fname_s3 = paste0(bucket_str, sub(".hdf", "", fname), "/", fname)
  fname_local = paste0("/home/rstudio/maiac_raw/", fname)
  
  system(paste0("aws s3 cp ",fname_s3," ", fname_local, " --profile earthdata"))
}