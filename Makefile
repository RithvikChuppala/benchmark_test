CXX=g++
CXXFLAGS=-std=c++11
CXXLD=$(CXX)
BIN=.

all: $(BIN)/benchmark

# Configuration variables to compile and link against the Cloud Spanner C++
# client library.
SPANNER_DEPS := google_cloud_cpp_spanner
SPANNER_CXXFLAGS   := $(shell pkg-config $(SPANNER_DEPS) --cflags)
SPANNER_CXXLDFLAGS := $(shell pkg-config $(SPANNER_DEPS) --libs-only-L)
SPANNER_LIBS       := $(shell pkg-config $(SPANNER_DEPS) --libs-only-l)

# A target using the Cloud Spanner C++ client library.
$(BIN)/benchmark: benchmark.cc
	$(CXXLD) $(CXXFLAGS) $(SPANNER_CXXFLAGS) $(SPANNER_CXXLDFLAGS) -o $@ $^ $(SPANNER_LIBS)