#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
double median2(std::vector<double> x) {
    double median;
    size_t size = x.size();
    sort(x.begin(), x.end());
    if (size  % 2 == 0){
		median = (x[size / 2 - 1] + x[size / 2]) / 2.0;
    }
    else {
		median = x[size / 2];
    }
    return median;
}