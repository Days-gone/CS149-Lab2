ARCH := $(shell uname -m)

ifeq ($(ARCH), aarch64)
    CXX = g++
else
    CXX = g++ -m64
endif

CXXFLAGS=-I. -I../common -I../tests -Iobjs/ -O3 -std=c++11 -Wall

APP_NAME=runtasks
OBJDIR=objs
COMMONDIR=../common

PPM_CXX=$(COMMONDIR)/ppm.cpp
PPM_OBJ=$(addprefix $(OBJDIR)/, $(subst $(COMMONDIR)/,, $(PPM_CXX:.cpp=.o)))

default: $(APP_NAME)

.PHONY: dirs clean compile

dirs:
	/bin/mkdir -p $(OBJDIR)/

clean:
	/bin/rm -rf $(OBJDIR) *.ppm *~ $(APP_NAME) compile_commands.json

compile:
	bear -- make

test: default
	./runtasks -n 16 mandelbrot_chunked
	clear
	python3 ../tests/run_test_harness.py -t super_super_light super_light

OBJS=$(PPM_OBJ) $(OBJDIR)/tasksys.o

$(APP_NAME): clean dirs $(OBJS)
	$(CXX) ../tests/main.cpp $(CXXFLAGS) -o $@ $(OBJDIR)/tasksys.o -lm -lpthread

$(OBJDIR)/%.o: $(COMMONDIR)/%.cpp
	$(CXX) $< $(CXXFLAGS) -c -o $@

$(OBJDIR)/%.o: %.cpp
	$(CXX) $< $(CXXFLAGS) -c -o $@


