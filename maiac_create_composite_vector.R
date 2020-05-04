# script to create composite vectors

# working directory
setwd("D:\\maiac_processing\\")


# x days composite -------------------------------------------------------

# create day matrix
composite_no = 16
day_mat = matrix(sprintf("%03d",1:(360 - 360%%composite_no)), ncol=composite_no, byrow=T)

# find the lines in day_mat of first and last composite
# 64 2000
idx_2000 = which(sprintf("%03d", 64) == day_mat, arr.ind = TRUE)[1]

# create the loop mat excluding the start and end index
mat1 = expand.grid(c(idx_2000:dim(day_mat)[1]), 2000)
mat2 = expand.grid(c(1:dim(day_mat)[1]), c(2001:2019))

# merge mat columns
mat1 = cbind(mat1$Var1, mat1$Var2)
mat2 = cbind(mat2$Var1, mat2$Var2)

# merge mats
day_year = rbind(mat1, mat2)

# substitute day_mat in day_year
composite_vec = c()
for (i in 1:dim(day_year)[1]) {
  composite_vec[i] = paste0(day_year[i,2],day_mat[day_year[i,1],composite_no])
}

# export
write(composite_vec, file = paste0("maiac_composite_vec_",composite_no,".csv"))


# x days composite with incomplete series (missing composites in the last year) --------

# create day matrix
composite_no = 16
day_mat = matrix(sprintf("%03d",1:(360 - 360%%composite_no)), ncol=composite_no, byrow=T)

# find the lines in day_mat of first and last composite
# 64 2000 and 240 2016
idx_2000 = which(sprintf("%03d", 64) == day_mat, arr.ind = TRUE)[1]
idx_2016 = which(sprintf("%03d", 240) == day_mat, arr.ind = TRUE)[1]

# create the loop mat excluding the start and end index
mat1 = expand.grid(c(idx_2000:dim(day_mat)[1]), 2000)
mat2 = expand.grid(c(1:dim(day_mat)[1]), c(2001:2015))
mat3 = expand.grid(c(1:idx_2016), 2016)

# merge mat columns
mat1 = cbind(mat1$Var1, mat1$Var2)
mat2 = cbind(mat2$Var1, mat2$Var2)
mat3 = cbind(mat3$Var1, mat3$Var2)

# merge mats
day_year = rbind(mat1, mat2, mat3)

# substitute day_mat in day_year
composite_vec = c()
for (i in 1:dim(day_year)[1]) {
  composite_vec[i] = paste0(day_year[i,2],day_mat[day_year[i,1],composite_no])
}

# export
write(composite_vec, file = paste0("maiac_composite_vec_",composite_no,".csv"))



# create composite dates file ---------------------------------------------

# read composite
composite_vec = as.character(read.csv(paste0("maiac_composite_vec_",composite_no,".csv"), header = F)[[1]])

# convert doy to date
composite_vec_dates = as.Date(composite_vec, format="%Y%j")

# save
write.table(composite_vec_dates, file = paste0("maiac_composite_vec_",composite_no,"_dates.csv"), col.names = F, row.names = F, quote=F, sep=",")


# create monthly composite vector -----------------------------------------

mat1 = expand.grid(sprintf("%02d",1:12), 2000:2019)
mat1 = paste0(mat1$Var2, "_", mat1$Var1)

# export
write(mat1, file = "maiac_composite_vec_month.csv")
