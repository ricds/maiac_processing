#!/usr/bin/env python3
import argparse
import os
import os.path
import warnings
import math
import numpy as np
from osgeo import gdal
from osgeo import gdalnumeric

def main():
    # initiate the parser
    parser = argparse.ArgumentParser(description = 'Summarize a set of rasters layers.')
    parser.add_argument('files', nargs = '+', 
                        help = 'input raster file(s)')
    parser.add_argument('--outfile', '-o', required = True, help = 'output raster')
    band_help = 'bands to summarize. ' + \
        'single file: bands in this file to summarize (default all bands); ' + \
        'multiple files: bands in corresponding files to summarize (default = 1)'
    parser.add_argument('--bands', '-b', nargs = '+', 
                        type = int, 
                        help = band_help)
    function_help = 'function to apply to cell values across layers.' + \
        'meannz gives a mean of the non-zero values.' + \
        'count counts the number layers with non-negative values for each cell.' + \
        'richness counts the number of layers with positive values for each cell.'
    parser.add_argument('--function', '-f', 
                        dest = 'summary_function',
                        default = 'mean',
                        choices = ['mean', 'median', 'max', 'sum', 'meannz', 'count', 'richness'],
                        help = "summarization function (default = 'mean')")                    
    parser.add_argument('--block_size', '-s', nargs = 2,
                        type = int,
                        help = 'x and y dimensions of blocks to process (default based on input)')
    parser.add_argument('--nrows', '-n', type = int,
                        help = 'number of rows to process in a single block (block_size ignored if provided)')
    parser.add_argument('--overwrite', '-w', 
                        action = "store_true", 
                        help = 'overwrite existing file')
    parser.add_argument('--creation-option', '--co', 
                        dest = 'creation_options', 
                        default = [], action = 'append',
                        help='passes one or more creation options to the output format driver multiple')
    parser.add_argument('--quiet', '-q', 
                        action = "store_true", 
                        help = 'supress messages')

    # read arguments from the command line
    args = parser.parse_args()

    # summarize multiple bands of same file or bands across files
    if len(args.files) == 1:
        # use all bands if not specified
        if not args.bands:
            b = gdal.Open(args.files[0], gdal.GA_ReadOnly).RasterCount
            args.bands = [i for i in range(1, b + 1)]
        # elif len(args.bands) == 1:
        #     raise Exception('For a single input file, provide multiple bands to summarize over.')
        args.files = [args.files[0]] * len(args.bands)
    else:
        # use band 1 if not specified
        if not args.bands:
            args.bands = [1] * len(args.files)
        elif len(args.bands) == 1:
            args.bands = [args.bands[0]] * len(args.files)
        elif len(args.bands) != len(args.files):
            raise Exception('The number of bands must be 1 or equal to the number of input files.')
    
    # set up some default nodatavalues for each datatype
    ndv_lookup = {'Byte': 255, 
                  'UInt16': 65535, 'Int16': -32767, 
                  'UInt32': 4294967293, 'Int32': -2147483647, 
                  'Float32': 3.402823466E+38, 
                  'Float64': 1.7976931348623158E+308}

    stack = []
    in_type = []
    in_ndv = []
    raster_dim = None
    block_size = None
    out_transform = None
    out_projection = None
    # pass over files to get metadata and check arguments
    for i in range(len(args.files)):
        f = args.files[i]
        b = args.bands[i]
        s = gdal.Open(f, gdal.GA_ReadOnly)

        # check that file exists
        if not s:
            raise IOError('No such file or directory: {}'.format(f))
            
        # check that band is valid
        if b > s.RasterCount:
            raise IOError('Invalid band number ({}) for file: {}'.format(b, f))

        # get specified band
        r = s.GetRasterBand(b)

        # store raster file
        stack.append(s)

        # metadata
        in_type.append(r.DataType)
        dt_name = gdal.GetDataTypeName(r.DataType).lower()
        in_ndv.append(np.float64(r.GetNoDataValue()).astype(dt_name))

        # check that the dimensions of each layer are the same
        if raster_dim:
            if raster_dim != [s.RasterXSize, s.RasterYSize]:
                raise Exception('Input files have different dimensions.')
        else:
            raster_dim = [s.RasterXSize, s.RasterYSize]

        # block size to chop grids into bite-sized chunks
        if not block_size:
            # use the block size of the first layer to read efficiently
            # or use user provided block size
            if not args.nrows:
                if not args.block_size:
                    block_size = s.GetRasterBand(1).GetBlockSize()
                else:
                    # block size can't be larger than raster dimensions
                    block_size = np.minimum(args.block_size, raster_dim).tolist()
            else:
                block_size = [raster_dim[0], min(raster_dim[1], args.nrows)]
                
        # get geo info from first layer
        if not out_transform:
            out_transform = s.GetGeoTransform()
        if not out_projection:
            out_projection = s.GetProjection()

    # prepare output file
    if os.path.isfile(args.outfile) and not args.overwrite:
        raise Exception("Output exists, use the --overwrite to overwrite the existing file")
    else:
        # remove existing file and regenerate
        if os.path.isfile(args.outfile):
            os.remove(args.outfile)

        # for sum use the largest type of the input files
        # otherise use a value suitable for the function
        if args.summary_function == 'sum':
            out_type = gdal.GetDataTypeName(max(in_type))
            out_type_np = out_type.lower()
            np_nan = 0
        elif args.summary_function == 'mean':
            out_type = 'Float32'
            out_type_np = out_type.lower()
            np_nan = np.nan
        elif args.summary_function == 'median':
            out_type = 'Float32'
            out_type_np = out_type.lower()
            np_nan = np.nan
        elif args.summary_function == 'max':
            out_type = 'Float32'
            out_type_np = out_type.lower()
            np_nan = np.nan
        elif args.summary_function == 'meannz':
            out_type = 'Float32'
            out_type_np = out_type.lower()
            np_nan = np.nan
        elif args.summary_function == 'count':
            out_type = 'Int16'
            out_type_np = out_type.lower()
            np_nan = -1
        elif args.summary_function == 'richness':
            out_type = 'Int16'
            out_type_np = out_type.lower()
            np_nan = -1

        # create file
        out_driver = gdal.GetDriverByName('GTiff')
        r_out = out_driver.Create(args.outfile, raster_dim[0], raster_dim[1], 1,
                                  gdal.GetDataTypeByName(out_type),
                                  args.creation_options)

        # set output geo info based on first input layer
        r_out.SetGeoTransform(out_transform)
        r_out.SetProjection(out_projection)

        # set no data value
        out_ndv = ndv_lookup[out_type]
        out_ndv_np = np.float64(out_ndv).astype(out_type_np)
        r_out.GetRasterBand(1).SetNoDataValue(out_ndv)

    # find total x and y blocks to be read
    xblocks = math.ceil(raster_dim[0] / block_size[0])
    yblocks = math.ceil(raster_dim[1] / block_size[1])

    # loop through blocks of data
    # store these numbers in variables that may change later
    xvalid = block_size[0]
    yvalid = block_size[1]

    # variables for displaying progress
    progress_cnt = -1
    progress_stp = 0
    progress_end = xblocks * yblocks
    progress_stps = [round(progress_end * i / 100) for i in range(0, 101, 10)]
    
    # message
    if not args.quiet:
        m = "Processing {} X {} raster (cols X rows) in {} blocks of {} X {}"
        m = m.format(raster_dim[0], raster_dim[1],
                     xblocks * yblocks,
                     block_size[0], block_size[1])
        print(m)

    # loop through x dimension
    for x in range(0, xblocks):
        # in case the blocks don't fit perfectly
        # change the block size of the final piece
        if x == xblocks - 1:
            xvalid = raster_dim[0] - x * block_size[0]

        # find x offset
        x_off = x * block_size[0]

        # reset buffer size for start of Y loop
        yvalid = block_size[1]

        # loop through y dimension
        for y in range(0, yblocks):
            # progress bar
            progress_cnt += 1
            if progress_cnt == progress_stps[progress_stp] and not args.quiet:
                print('%d...' % (10 * progress_stp), end = "", flush = True)
                progress_stp += 1

            # change the block size of the final piece
            if y == yblocks - 1:
                yvalid = raster_dim[1] - y * block_size[1]

            # find y offset
            y_off = y * block_size[1]

            # create empty buffer to mark where nodata occurs
            ndv_buffer = None

            # make array to store block
            block = np.empty(shape = (len(stack), yvalid, xvalid), 
                             dtype = 'float32')

            # fetch data for each input layer
            for i in range(len(stack)):
                vals = gdalnumeric.BandReadAsArray(stack[i].GetRasterBand(args.bands[i]),
                                                   xoff = x_off, yoff = y_off,
                                                   win_xsize = xvalid, win_ysize = yvalid)
                if out_type != "Int16":
                    vals = vals.astype(out_type_np)
                
                # fill in nodata values
                if in_ndv[i] is not None:
                    vals[vals == in_ndv[i]] = np_nan

                # add block to array
                block[i] = vals
                vals = None
            
            # ignore empty slice warnings
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', category = RuntimeWarning)
                if args.summary_function == 'sum':
                    result = np.nansum(block, axis = 0)
                elif args.summary_function == 'mean':
                    result = np.nanmean(block, axis = 0)
                elif args.summary_function == 'median':
                    result = np.nanmedian(block, axis = 0)
                elif args.summary_function == 'max':
                    result = np.nanmax(block, axis = 0)
                elif args.summary_function == 'meannz':
                    block = np.ma.masked_equal(block, 0)
                    result = np.nanmean(block, axis = 0)
                elif args.summary_function == 'count':
                    result = np.nansum(block >= 0, axis = 0)
                elif args.summary_function == 'richness':
                    result = np.nansum(block > 0, axis = 0)
                
            # replace nan with no data value
            result[np.isnan(result)] = out_ndv_np

            # write data block to the output file
            gdalnumeric.BandWriteArray(r_out.GetRasterBand(1), result, 
                                       xoff = x_off, yoff = y_off)

    # end progress bar
    if not args.quiet:
        print('100 - Done')

if __name__ == '__main__':
    main()
