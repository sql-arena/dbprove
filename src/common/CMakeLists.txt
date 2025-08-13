set(_targetName "dbprove_common")

add_library(${_targetName} INTERFACE)
add_library(dbprove::common ALIAS ${_targetName})

target_sources(${_targetName} 
	PUBLIC FILE_SET installed TYPE HEADERS BASE_DIRS include
)

add_subdirectory(include/dbprove/common)
