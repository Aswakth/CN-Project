CXX=g++
CXXFLAGS=-O2 -std=c++17

all: sender receiver garbler

sender: sender.cpp
	$(CXX) $(CXXFLAGS) sender.cpp -o sender

receiver: receiver.cpp
	$(CXX) $(CXXFLAGS) receiver.cpp -o receiver

garbler: garbler.cpp
	$(CXX) $(CXXFLAGS) garbler.cpp -o garbler

clean:
	rm -f sender receiver garbler
