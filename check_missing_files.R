# list files
url="s3://ctrees-input-data/modis/AnisoVeg_MCD19A1/v061_1km_monthly_expMay25/tiled/nadir/2023/"
file_list = s3_list_bucket(url)
length(file_list)

manual_tile = c("h29v07", #ok
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


# "SR_Nadir_month_MAIACTerraAqua_h29v07_2022_01_band1.tif"
band_list = c(paste0("band",1:8),"no_samples")
grid_List = expand.grid(manual_tile, "2023", sprintf("%02d", 1:12), band_list)
file_list_expected = paste0(grid_List$Var1,"_",grid_List$Var2,"_",grid_List$Var3,"_",grid_List$Var4)
length(file_list_expected)

# compare file_list with file_list_expected
i=1
for (i in 1:length(file_list_expected)) {
  if (length(grep(file_list_expected[i], file_list)) != 1)
    print(i)
}
file_list_expected[1747]
