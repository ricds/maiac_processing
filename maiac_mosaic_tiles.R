##################################################
## Project: maiac_processing (https://github.com/ricds/maiac_processing)
## Script purpose: to merge MAIAC composites in a mosaic
## Author: Ricardo Dal'Agnol da Silva (ricds@hotmail.com)
## Date: 2017-02-09
##################################################
## General processing workflow:
## mosaic the resulting median tiles (mosaic_rasters, also from gdal)
## reproject the composite to latlon (gdalwarp) and save as geotif
##################################################

# TO DO: update the code to mosaic and reproject
#
# # list and load tiles
# processed_tiles = list.files(paste0(output_dir,tmp_dir), pattern = "Processed")
# 
# # mosaic the tiles
# mosaic_brf = MosaicTilesAndSave(processed_tiles, output_dir, tmp_dir, year, day)
# 
# # plot brfnadir to file to verify it later
# png(filename=paste0(tile_preview_dir,"fig_",composite_fname,"_",year,day[8],".png"), type="cairo", units="cm", width=15, height=15, pointsize=10, res=300)
# par(oma=c(4,4,4,4))
# plot(mosaic_brf)
# dev.off()